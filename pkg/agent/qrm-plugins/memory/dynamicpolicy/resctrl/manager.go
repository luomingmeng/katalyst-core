/*
Copyright 2022 The Katalyst Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package resctrl

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.uber.org/atomic"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"

	qrmresctrlmanager "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/resctrl"
	qrmresctrl "github.com/kubewharf/katalyst-core/pkg/config/agent/qrm/resctrl"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
)

const (
	PodDirPrefix = "pod"
	MonGroupsDir = "mon_groups"
	tasks        = "tasks"
	cpus         = "cpus"
	schemata     = "schemata"
)

var ErrRDTDisabled = errors.New("RDT is disabled")

type Manager interface {
	Run(stopCh <-chan struct{})
	Create(podUID, closID string, createMonGroup bool) error
	ReconcileClos(state ClosReconcileState) error
	GetMonGroupsCount() (int64, error)
}

// ClosInvalidator clears RDT schemata state after a CLOS lifecycle transition.
type ClosInvalidator interface {
	InvalidateClos(closID string)
}

type closLifecycleRunner interface {
	RunClosLifecycle(closID string, update func() (bool, error)) error
}

// ClosReconcileState describes the desired CLOS directory layout.
// DisableRDT overrides the desired layout and removes all managed directories.
type ClosReconcileState struct {
	DisableRDT      bool
	ExpectedClosIDs sets.String
	ActivePodUIDs   sets.String
}

type managerImpl struct {
	config                  *qrmresctrl.ResctrlConfig
	enabled                 atomic.Bool
	root                    string
	disableRDT              bool
	lifecycleManagedClosIDs sets.String
	ownershipCheckpointPath string
	ownershipStore          *qrmresctrlmanager.ClosOwnershipStore
	ownershipLoadErr        error
	removeAll               func(string) error
	readFile                func(string) ([]byte, error)
	invalidator             ClosInvalidator
	sync.RWMutex
}

func NewManager(config *qrmresctrl.ResctrlConfig, invalidators ...ClosInvalidator) Manager {
	manager := &managerImpl{
		config:                  config,
		lifecycleManagedClosIDs: sets.NewString(),
		removeAll:               os.RemoveAll,
		readFile:                os.ReadFile,
	}
	if config != nil {
		manager.ownershipCheckpointPath = config.OwnershipCheckpointPath
	}
	manager.ownershipStore = qrmresctrlmanager.NewClosOwnershipStore(manager.ownershipCheckpointPath)
	manager.ownershipLoadErr = manager.loadOwnershipCheckpoint()
	if len(invalidators) > 0 {
		manager.invalidator = invalidators[0]
	}
	return manager
}

func (m *managerImpl) Run(stopCh <-chan struct{}) {
	wait.Until(func() {
		root, err := findResctrlMountpointDir()
		enable := root != ""
		m.Lock()
		if m.enabled.Load() == enable && m.root == root {
			m.Unlock()
			return
		}
		m.root = root
		m.enabled.Store(enable)
		m.Unlock()
		general.Infof("resctrl enabled %v: root %s, error: %v", enable, root, err)
	}, time.Minute, stopCh)
}

func (m *managerImpl) Create(podUID, closID string, createMonGroup bool) error {
	m.Lock()
	defer m.Unlock()
	if m.ownershipLoadErr != nil {
		return m.ownershipLoadErr
	}
	if m.disableRDT {
		return ErrRDTDisabled
	}
	if !m.enabled.Load() || m.root == "" {
		return nil
	}

	closIDPath, err := m.createClosLocked(closID)
	if err != nil {
		return err
	}
	if createMonGroup {
		rmID := PodDirPrefix + podUID
		monGroupsPath := filepath.Join(closIDPath, MonGroupsDir, rmID)
		if err := os.MkdirAll(monGroupsPath, 0o755); err != nil {
			return fmt.Errorf("create mon_groups dir %s failed: %v", monGroupsPath, err)
		}
	}
	return nil
}

// ReconcileClos creates default and expected CLOS directories, removes stale
// directories, or force removes every non-skipped CLOS directory while RDT is disabled.
func (m *managerImpl) ReconcileClos(state ClosReconcileState) error {
	m.Lock()
	defer m.Unlock()
	if m.ownershipLoadErr != nil {
		return m.ownershipLoadErr
	}
	m.disableRDT = state.DisableRDT
	if !m.enabled.Load() || m.root == "" {
		return nil
	}
	if err := m.recoverPendingDeletesLocked(); err != nil {
		return err
	}
	if err := m.refreshOwnershipLocked(); err != nil {
		return err
	}

	entries, err := os.ReadDir(m.root)
	if err != nil {
		return fmt.Errorf("read resctrl root %s failed: %w", m.root, err)
	}
	skipClosIDs := sets.NewString()
	if m.config != nil {
		skipClosIDs.Insert(m.config.SkipCleanupClosIDs.UnsortedList()...)
	}
	m.cleanupInactiveMonGroupsLocked(skipClosIDs, state.ActivePodUIDs)

	if state.DisableRDT {
		for _, entry := range entries {
			if !isClosDir(entry) || skipClosIDs.Has(entry.Name()) {
				continue
			}
			if !m.isLifecycleManagedClosLocked(entry.Name()) {
				general.Infof("resctrl: preserve external CLOS %s while RDT is disabled", entry.Name())
				continue
			}
			if err := m.removeClosLocked(entry.Name(), filepath.Join(m.root, entry.Name())); err != nil {
				return fmt.Errorf("force remove clos_id dir %s failed: %w", entry.Name(), err)
			}
		}
		return nil
	}

	expectedClosIDs := sets.NewString()
	if m.config != nil {
		expectedClosIDs.Insert(m.config.DefaultClosIDs...)
	}
	expectedClosIDs.Insert(state.ExpectedClosIDs.UnsortedList()...)
	for closID := range expectedClosIDs {
		if _, err := m.createClosLocked(closID); err != nil {
			return err
		}
	}

	entries, err = os.ReadDir(m.root)
	if err != nil {
		return fmt.Errorf("read resctrl root %s failed: %w", m.root, err)
	}
	for _, entry := range entries {
		if !isClosDir(entry) || expectedClosIDs.Has(entry.Name()) ||
			skipClosIDs.Has(entry.Name()) {
			continue
		}
		path := filepath.Join(m.root, entry.Name())
		if !m.isLifecycleManagedClosLocked(entry.Name()) {
			general.Infof("resctrl: skip external CLOS %s during regular cleanup", entry.Name())
			continue
		}
		if !m.isClosEmptyLocked(path) {
			continue
		}
		if err := m.removeClosLocked(entry.Name(), path); err != nil {
			return fmt.Errorf("remove stale clos_id dir %s failed: %w", entry.Name(), err)
		}
	}
	return nil
}

func (m *managerImpl) isLifecycleManagedClosLocked(closID string) bool {
	return m.lifecycleManagedClosIDs.Has(closID)
}

func (m *managerImpl) cleanupInactiveMonGroupsLocked(skipClosIDs, activePodUIDs sets.String) {
	if activePodUIDs == nil {
		return
	}
	walkMonGroupsDirs(m.root, skipClosIDs, func(uid, closID, path string) {
		if !m.isLifecycleManagedClosLocked(closID) ||
			activePodUIDs.Has(uid) || !m.isFileEmpty(path, tasks) {
			return
		}
		general.Infof("resctrl: remove pod %s mon_groups dir %s", uid, path)
		if err := m.remove(path); err != nil {
			general.Errorf("resctrl: remove pod %s mon_groups dir %s error: %v", uid, path, err)
		}
	}, nil)
}

func (m *managerImpl) GetMonGroupsCount() (int64, error) {
	if !m.enabled.Load() {
		return 0, nil
	}

	m.RLock()
	root := m.root
	m.RUnlock()

	var count int64
	subdirs, err := os.ReadDir(root)
	if err != nil {
		return 0, fmt.Errorf("read root %s error: %v", root, err)
	}
	for _, subdir := range subdirs {
		if !subdir.IsDir() || subdir.Name() == "info" || subdir.Name() == "mon_data" || subdir.Name() == MonGroupsDir {
			continue
		}
		monGroupPath := filepath.Join(root, subdir.Name(), MonGroupsDir)
		monGroupsDirs, err := os.ReadDir(monGroupPath)
		if err != nil && !os.IsNotExist(err) {
			general.Errorf("resctrl: read mon_groups dir %s error: %v", monGroupPath, err)
			continue
		}
		count += int64(len(monGroupsDirs))
	}
	return count, nil
}

func (m *managerImpl) isFileEmpty(root, name string) bool {
	readFile := m.readFile
	if readFile == nil {
		readFile = os.ReadFile
	}
	content, err := readFile(filepath.Join(root, name))
	if err != nil {
		return false
	}
	return len(strings.TrimSpace(string(content))) == 0
}

func isClosDir(entry os.DirEntry) bool {
	return entry.IsDir() && !sets.NewString("info", "mon_data", MonGroupsDir).Has(entry.Name())
}

func (m *managerImpl) createClosLocked(closID string) (string, error) {
	closIDPath := filepath.Join(m.root, closID)
	err := m.runClosLifecycleLocked(closID, func() (bool, error) {
		if _, err := os.Stat(closIDPath); err == nil {
			return false, nil
		} else if !os.IsNotExist(err) {
			return false, fmt.Errorf("stat clos_id dir %s failed: %w", closIDPath, err)
		}
		if err := m.markLifecycleManagedClosLocked(closID); err != nil {
			return false, fmt.Errorf("checkpoint CLOS %q ownership: %w", closID, err)
		}
		if err := os.MkdirAll(closIDPath, 0o755); err != nil {
			_ = m.unmarkLifecycleManagedClosLocked(closID)
			return false, fmt.Errorf("create clos_id dir %s failed: %w", closIDPath, err)
		}
		return true, nil
	})
	return closIDPath, err
}

func (m *managerImpl) isClosEmptyLocked(path string) bool {
	if !m.isFileEmpty(path, tasks) || !m.isFileEmpty(path, cpus) {
		return false
	}
	entries, err := os.ReadDir(filepath.Join(path, MonGroupsDir))
	return os.IsNotExist(err) || err == nil && len(entries) == 0
}

func (m *managerImpl) removeClosLocked(closID, path string) error {
	return m.runClosLifecycleLocked(closID, func() (bool, error) {
		wasManaged := m.lifecycleManagedClosIDs.Has(closID)
		if wasManaged && m.ownershipCheckpointPath != "" {
			if err := m.ownershipStore.BeginDelete(closID); err != nil {
				return false, err
			}
		}
		if err := m.remove(path); err != nil {
			return false, err
		}
		if wasManaged {
			if m.ownershipCheckpointPath != "" {
				if err := m.ownershipStore.FinishDelete(closID); err != nil {
					return false, err
				}
				m.lifecycleManagedClosIDs.Delete(closID)
			} else if err := m.unmarkLifecycleManagedClosLocked(closID); err != nil {
				return false, err
			}
		}
		return true, nil
	})
}

func (m *managerImpl) recoverPendingDeletesLocked() error {
	if m.ownershipCheckpointPath == "" {
		return nil
	}
	pending, err := m.ownershipStore.PendingDeletes()
	if err != nil {
		return err
	}
	for closID := range pending {
		path := filepath.Join(m.root, closID)
		if err := m.runClosLifecycleLocked(closID, func() (bool, error) {
			if err := m.remove(path); err != nil {
				return false, err
			}
			if err := m.ownershipStore.FinishDelete(closID); err != nil {
				return false, err
			}
			m.lifecycleManagedClosIDs.Delete(closID)
			return true, nil
		}); err != nil {
			return fmt.Errorf("recover pending deletion of CLOS %q: %w", closID, err)
		}
	}
	return nil
}

func (m *managerImpl) remove(path string) error {
	if m.removeAll != nil {
		return m.removeAll(path)
	}
	return os.RemoveAll(path)
}

func (m *managerImpl) markLifecycleManagedClosLocked(closID string) error {
	if m.lifecycleManagedClosIDs == nil {
		m.lifecycleManagedClosIDs = sets.NewString()
	}
	if m.lifecycleManagedClosIDs.Has(closID) {
		return nil
	}
	if m.ownershipCheckpointPath != "" {
		if err := m.ownershipStore.Register(closID); err != nil {
			return err
		}
	}
	m.lifecycleManagedClosIDs.Insert(closID)
	return nil
}

func (m *managerImpl) unmarkLifecycleManagedClosLocked(closID string) error {
	if m.lifecycleManagedClosIDs == nil || !m.lifecycleManagedClosIDs.Has(closID) {
		return nil
	}
	if m.ownershipCheckpointPath != "" {
		if err := m.ownershipStore.Unregister(closID); err != nil {
			return err
		}
	}
	m.lifecycleManagedClosIDs.Delete(closID)
	return nil
}

func (m *managerImpl) loadOwnershipCheckpoint() error {
	return m.refreshOwnershipLocked()
}

func (m *managerImpl) refreshOwnershipLocked() error {
	if m.ownershipCheckpointPath == "" {
		return nil
	}
	owned, err := m.ownershipStore.Load()
	if err != nil {
		return err
	}
	m.lifecycleManagedClosIDs = owned
	return nil
}

func (m *managerImpl) invalidateClosLocked(closID string) {
	if m.invalidator != nil {
		m.invalidator.InvalidateClos(closID)
	}
}

func (m *managerImpl) runClosLifecycleLocked(closID string, update func() (bool, error)) error {
	if runner, ok := m.invalidator.(closLifecycleRunner); ok {
		return runner.RunClosLifecycle(closID, update)
	}
	changed, err := update()
	if err != nil {
		return err
	}
	if changed {
		m.invalidateClosLocked(closID)
	}
	return nil
}

func walkMonGroupsDirs(root string, skipClosIDs sets.String, walkMonGroupsFunc func(uid, closID, path string), walkClosIDFunc func(closID, path string)) {
	subdirs, err := os.ReadDir(root)
	if err != nil {
		general.Errorf("resctrl: read root %s error: %v", root, err)
		return
	}
	for _, subdir := range subdirs {
		if !isClosDir(subdir) {
			continue
		}
		closID := subdir.Name()
		if skipClosIDs != nil && skipClosIDs.Has(closID) {
			continue
		}
		monGroupPath := filepath.Join(root, closID, MonGroupsDir)

		monGroupsSubdirs, err := os.ReadDir(monGroupPath)
		if err != nil {
			if !os.IsNotExist(err) {
				general.Errorf("resctrl: read mon_groups dir %s error: %v", monGroupPath, err)
			}
		} else {
			for _, monGroupsSubdir := range monGroupsSubdirs {
				rmID := monGroupsSubdir.Name()
				if !monGroupsSubdir.IsDir() || !strings.HasPrefix(rmID, PodDirPrefix) {
					continue
				}
				podMonGroupPath := filepath.Join(monGroupPath, rmID)
				uid := strings.TrimPrefix(rmID, PodDirPrefix)

				if walkMonGroupsFunc != nil {
					walkMonGroupsFunc(uid, closID, podMonGroupPath)
				}
			}
		}

		if walkClosIDFunc != nil {
			walkClosIDFunc(closID, filepath.Join(root, closID))
		}
	}
}
