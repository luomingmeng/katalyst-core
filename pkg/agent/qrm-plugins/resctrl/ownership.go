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
	Version int      `json:"version"`
	ClosIDs []string `json:"clos_ids"`
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
	owned, err := s.loadLocked()
	if err != nil {
		return err
	}
	owned.Insert(closID)
	return s.writeLocked(owned)
}

func (s *ClosOwnershipStore) Unregister(closID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	owned, err := s.loadLocked()
	if err != nil {
		return err
	}
	owned.Delete(closID)
	return s.writeLocked(owned)
}

func (s *ClosOwnershipStore) Load() (sets.String, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadLocked()
}

func (s *ClosOwnershipStore) loadLocked() (sets.String, error) {
	owned := sets.NewString()
	if s.path == "" {
		return owned, nil
	}
	content, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return owned, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read resctrl CLOS ownership checkpoint: %w", err)
	}
	var checkpoint closOwnershipCheckpoint
	if err := json.Unmarshal(content, &checkpoint); err != nil {
		return nil, fmt.Errorf("decode resctrl CLOS ownership checkpoint: %w", err)
	}
	if checkpoint.Version != 1 {
		return nil, fmt.Errorf("unsupported resctrl CLOS ownership checkpoint version %d", checkpoint.Version)
	}
	owned.Insert(checkpoint.ClosIDs...)
	return owned, nil
}

func (s *ClosOwnershipStore) writeLocked(owned sets.String) error {
	if s.path == "" {
		return nil
	}
	closIDs := owned.UnsortedList()
	sort.Strings(closIDs)
	content, err := json.Marshal(closOwnershipCheckpoint{Version: 1, ClosIDs: closIDs})
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
	return nil
}
