/*
Copyright 2026 The Katalyst Authors.

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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/kubewharf/katalyst-core/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/util/external/rdt"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

const (
	cpus         = "cpus"
	monGroupsDir = "mon_groups"
)

// CPUListClos describes an existing resctrl CLOS directory. Epoch changes
// whenever the directory's filesystem identity changes.
type CPUListClos struct {
	ID    string
	Epoch uint64
}

// CPUListManager is the narrow resctrl query/write boundary used by CPU-side
// RDT resource plugins. ApplyCPUList may create a missing CLOS only when it
// needs to write a non-empty target; CLOS deletion stays owned by memory
// resctrl.Manager.
type CPUListManager interface {
	ListManagedClos(context.Context) ([]CPUListClos, error)
	ApplyCPUList(context.Context, string, string) error
}

type closResourceUpdater interface {
	RunClosResourceUpdate(closID string, update func() (bool, error)) error
}

type cpuListManager struct {
	root string

	mu       sync.Mutex
	epochs   map[string]uint64
	previous map[string]os.FileInfo
	updater  closResourceUpdater
}

func NewCPUListManager() CPUListManager {
	return newCPUListManagerWithResourceUpdater(consts.DefaultResctrlRootDir, rdt.NewDefaultManager())
}

func newCPUListManager(root string) *cpuListManager {
	return newCPUListManagerWithResourceUpdater(root, nil)
}

func newCPUListManagerWithResourceUpdater(root string, updater closResourceUpdater) *cpuListManager {
	return &cpuListManager{
		root:     root,
		epochs:   make(map[string]uint64),
		previous: make(map[string]os.FileInfo),
		updater:  updater,
	}
}

func (m *cpuListManager) ListManagedClos(_ context.Context) ([]CPUListClos, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entries, err := os.ReadDir(m.root)
	if os.IsNotExist(err) {
		m.previous = make(map[string]os.FileInfo)
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read resctrl root %q: %w", m.root, err)
	}

	current := make(map[string]os.FileInfo)
	clos := make([]CPUListClos, 0, len(entries))
	for _, entry := range entries {
		if !isClosDir(entry) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, fmt.Errorf("stat CLOS %q: %w", entry.Name(), err)
		}
		previous, existed := m.previous[entry.Name()]
		if !existed || !os.SameFile(previous, info) {
			m.epochs[entry.Name()]++
		}
		current[entry.Name()] = info
		clos = append(clos, CPUListClos{ID: entry.Name(), Epoch: m.epochs[entry.Name()]})
	}
	m.previous = current
	return clos, nil
}

func (m *cpuListManager) ApplyCPUList(_ context.Context, closID, target string) error {
	createMissingClos := strings.TrimSpace(target) != ""
	mask, err := m.formatCPUListMask(target)
	if err != nil {
		return fmt.Errorf("format CLOS %q cpu_list %q: %w", closID, target, err)
	}
	update := func() (bool, error) {
		created, err := m.ensureClos(closID, createMissingClos)
		if err != nil {
			return false, err
		}
		if err := os.WriteFile(filepath.Join(m.root, closID, cpus), []byte(mask), 0o644); err != nil {
			if os.IsNotExist(err) && !createMissingClos {
				return created, nil
			}
			return created, err
		}
		return created, nil
	}
	var writeErr error
	if m.updater != nil {
		writeErr = m.updater.RunClosResourceUpdate(closID, update)
	} else {
		_, writeErr = update()
	}
	if writeErr != nil {
		return fmt.Errorf("write CLOS %q cpu_list: %w", closID, writeErr)
	}
	return nil
}

func (m *cpuListManager) ensureClos(closID string, create bool) (bool, error) {
	if !create {
		return false, nil
	}
	if err := os.Mkdir(filepath.Join(m.root, closID), 0o755); err != nil {
		if os.IsExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("create CLOS %q: %w", closID, err)
	}
	return true, nil
}

func (m *cpuListManager) formatCPUListMask(target string) (string, error) {
	wordCount, err := m.resctrlCPUMaskWordCount()
	if err != nil {
		return "", err
	}
	words := make([]uint32, wordCount)
	if strings.TrimSpace(target) == "" {
		return formatCPUListMaskWords(words), nil
	}
	cpus, err := machine.Parse(target)
	if err != nil {
		return "", fmt.Errorf("parse cpu list: %w", err)
	}
	for _, cpu := range cpus.ToSliceInt() {
		if cpu < 0 {
			return "", fmt.Errorf("negative cpu id %d", cpu)
		}
		word := cpu / 32
		if word >= len(words) {
			return "", fmt.Errorf("cpu id %d exceeds resctrl cpus mask width %d", cpu, len(words)*32)
		}
		words[word] |= uint32(1) << uint(cpu%32)
	}
	return formatCPUListMaskWords(words), nil
}

func (m *cpuListManager) resctrlCPUMaskWordCount() (int, error) {
	content, err := os.ReadFile(filepath.Join(m.root, cpus))
	if err != nil {
		return 0, fmt.Errorf("read resctrl root cpus: %w", err)
	}
	parts := strings.Split(strings.TrimSpace(string(content)), ",")
	count := 0
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if _, err := strconv.ParseUint(part, 16, 32); err != nil {
			return 0, fmt.Errorf("parse resctrl root cpus word %q: %w", part, err)
		}
		count++
	}
	if count == 0 {
		return 0, fmt.Errorf("resctrl root cpus is empty")
	}
	return count, nil
}

func isClosDir(entry os.DirEntry) bool {
	return entry.IsDir() && !sets.NewString("info", "mon_data", monGroupsDir).Has(entry.Name())
}

func formatCPUListMaskWords(words []uint32) string {
	parts := make([]string, 0, len(words))
	for i := len(words) - 1; i >= 0; i-- {
		parts = append(parts, fmt.Sprintf("%08x", words[i]))
	}
	return strings.Join(parts, ",")
}
