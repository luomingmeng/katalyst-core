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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/kubewharf/katalyst-core/pkg/config"
)

type closOwnershipCheckpoint struct {
	Version        int      `json:"version"`
	ClosIDs        []string `json:"clos_ids"`
	PendingCreates []string `json:"pending_creates,omitempty"`
	PendingDeletes []string `json:"pending_deletes,omitempty"`
}

type closOwnershipState struct {
	owned          sets.String
	pendingCreates sets.String
	pendingDeletes sets.String
}

var ownershipCheckpointLocks sync.Map

func OwnershipCheckpointPath(conf *config.Configuration) string {
	if conf == nil || conf.AgentConfiguration == nil ||
		conf.AgentConfiguration.GenericAgentConfiguration == nil ||
		conf.AgentConfiguration.GenericAgentConfiguration.GenericQRMPluginConfiguration == nil ||
		conf.AgentConfiguration.GenericAgentConfiguration.GenericQRMPluginConfiguration.
			StateDirectoryConfiguration == nil {
		return ""
	}
	stateDir := conf.AgentConfiguration.GenericAgentConfiguration.GenericQRMPluginConfiguration.
		StateDirectoryConfiguration.StateFileDirectory
	if stateDir == "" {
		return ""
	}
	return filepath.Join(stateDir, "resctrl-clos-ownership.json")
}

// ClosOwnershipStore atomically updates the shared CLOS ownership checkpoint.
type ClosOwnershipStore struct {
	path string
	mu   *sync.Mutex
}

func NewClosOwnershipStore(path string) *ClosOwnershipStore {
	lockKey := path
	if lockKey == "" {
		lockKey = fmt.Sprintf("memory:%p", &path)
	}
	lock, _ := ownershipCheckpointLocks.LoadOrStore(lockKey, &sync.Mutex{})
	return &ClosOwnershipStore{path: path, mu: lock.(*sync.Mutex)}
}

func (s *ClosOwnershipStore) Register(closID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.loadStateLocked()
	if err != nil {
		return err
	}
	state.owned.Insert(closID)
	state.pendingCreates.Delete(closID)
	state.pendingDeletes.Delete(closID)
	return s.writeLocked(state)
}

func (s *ClosOwnershipStore) BeginCreate(closID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.loadStateLocked()
	if err != nil {
		return err
	}
	if !state.owned.Has(closID) {
		state.pendingCreates.Insert(closID)
	}
	state.pendingDeletes.Delete(closID)
	return s.writeLocked(state)
}

func (s *ClosOwnershipStore) FinishCreate(closID string) error {
	return s.Register(closID)
}

func (s *ClosOwnershipStore) AbortCreate(closID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.loadStateLocked()
	if err != nil {
		return err
	}
	state.pendingCreates.Delete(closID)
	return s.writeLocked(state)
}

func (s *ClosOwnershipStore) Unregister(closID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.loadStateLocked()
	if err != nil {
		return err
	}
	state.owned.Delete(closID)
	state.pendingCreates.Delete(closID)
	state.pendingDeletes.Delete(closID)
	return s.writeLocked(state)
}

func (s *ClosOwnershipStore) BeginDelete(closID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.loadStateLocked()
	if err != nil {
		return err
	}
	if state.owned.Has(closID) {
		state.pendingDeletes.Insert(closID)
	}
	return s.writeLocked(state)
}

func (s *ClosOwnershipStore) FinishDelete(closID string) error {
	return s.Unregister(closID)
}

func (s *ClosOwnershipStore) PendingDeletes() (sets.String, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.loadStateLocked()
	if err != nil {
		return nil, err
	}
	return sets.NewString(state.pendingDeletes.UnsortedList()...), nil
}

func (s *ClosOwnershipStore) PendingCreates() (sets.String, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.loadStateLocked()
	if err != nil {
		return nil, err
	}
	return sets.NewString(state.pendingCreates.UnsortedList()...), nil
}

func (s *ClosOwnershipStore) Load() (sets.String, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.loadStateLocked()
	if err != nil {
		return nil, err
	}
	active := sets.NewString(state.owned.UnsortedList()...)
	active.Delete(state.pendingCreates.UnsortedList()...)
	active.Delete(state.pendingDeletes.UnsortedList()...)
	return active, nil
}

func (s *ClosOwnershipStore) loadStateLocked() (closOwnershipState, error) {
	state := closOwnershipState{
		owned:          sets.NewString(),
		pendingCreates: sets.NewString(),
		pendingDeletes: sets.NewString(),
	}
	if s.path == "" {
		return state, nil
	}
	content, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return state, nil
	}
	if err != nil {
		return state, fmt.Errorf("read resctrl CLOS ownership checkpoint: %w", err)
	}
	var checkpoint closOwnershipCheckpoint
	if err := json.Unmarshal(content, &checkpoint); err != nil {
		return state, fmt.Errorf("decode resctrl CLOS ownership checkpoint: %w", err)
	}
	if checkpoint.Version < 1 || checkpoint.Version > 3 {
		return state, fmt.Errorf("unsupported resctrl CLOS ownership checkpoint version %d", checkpoint.Version)
	}
	state.owned.Insert(checkpoint.ClosIDs...)
	if checkpoint.Version >= 2 {
		state.pendingDeletes.Insert(checkpoint.PendingDeletes...)
	}
	if checkpoint.Version >= 3 {
		state.pendingCreates.Insert(checkpoint.PendingCreates...)
	}
	return state, nil
}

func (s *ClosOwnershipStore) writeLocked(state closOwnershipState) error {
	if s.path == "" {
		return nil
	}
	closIDs := state.owned.UnsortedList()
	sort.Strings(closIDs)
	pendingCreates := state.pendingCreates.UnsortedList()
	sort.Strings(pendingCreates)
	pendingDeletes := state.pendingDeletes.UnsortedList()
	sort.Strings(pendingDeletes)
	content, err := json.Marshal(closOwnershipCheckpoint{
		Version:        3,
		ClosIDs:        closIDs,
		PendingCreates: pendingCreates,
		PendingDeletes: pendingDeletes,
	})
	if err != nil {
		return fmt.Errorf("encode resctrl CLOS ownership checkpoint: %w", err)
	}
	parent := filepath.Dir(s.path)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return fmt.Errorf("create resctrl CLOS ownership checkpoint directory: %w", err)
	}
	tmp, err := os.CreateTemp(parent, ".resctrl-clos-ownership-*")
	if err != nil {
		return fmt.Errorf("create resctrl CLOS ownership checkpoint temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.Write(content); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write resctrl CLOS ownership checkpoint: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync resctrl CLOS ownership checkpoint: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close resctrl CLOS ownership checkpoint: %w", err)
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		return fmt.Errorf("replace resctrl CLOS ownership checkpoint: %w", err)
	}
	parentDir, err := os.Open(parent)
	if err != nil {
		return fmt.Errorf("open resctrl CLOS ownership checkpoint directory: %w", err)
	}
	defer parentDir.Close()
	if err := parentDir.Sync(); err != nil {
		return fmt.Errorf("sync resctrl CLOS ownership checkpoint directory: %w", err)
	}
	return nil
}
