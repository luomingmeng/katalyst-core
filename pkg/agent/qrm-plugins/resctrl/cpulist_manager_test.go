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
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCPUListManagerApplyCPUListWritesResctrlMask(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, cpus), []byte("ffffffff,ffffffff,ffffffff\n"), 0o644))
	require.NoError(t, os.Mkdir(filepath.Join(root, "dedicated"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "dedicated", cpus), []byte("ffffffff,ffffffff,ffffffff\n"), 0o644))

	manager := newCPUListManager(root)
	require.NoError(t, manager.ApplyCPUList(context.Background(), "dedicated", "1-7,25,49"))

	content, err := os.ReadFile(filepath.Join(root, "dedicated", cpus))
	require.NoError(t, err)
	require.Equal(t, "00000000,00020000,020000fe", string(content))
}

func TestCPUListManagerApplyCPUListWritesZeroMaskForEmptyTarget(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, cpus), []byte("ffffffff,ffffffff,ffffffff\n"), 0o644))
	require.NoError(t, os.Mkdir(filepath.Join(root, "reclaim"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "reclaim", cpus), []byte("ffffffff,ffffffff,ffffffff\n"), 0o644))

	manager := newCPUListManager(root)
	require.NoError(t, manager.ApplyCPUList(context.Background(), "reclaim", ""))

	content, err := os.ReadFile(filepath.Join(root, "reclaim", cpus))
	require.NoError(t, err)
	require.Equal(t, "00000000,00000000,00000000", string(content))
}

func TestCPUListManagerApplyCPUListCreatesMissingClosBeforeWrite(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, cpus), []byte("ffffffff\n"), 0o644))

	manager := newCPUListManager(root)
	require.NoError(t, manager.ApplyCPUList(context.Background(), "dedicated", "1"))

	content, err := os.ReadFile(filepath.Join(root, "dedicated", cpus))
	require.NoError(t, err)
	require.Equal(t, "00000002", string(content))
}

func TestCPUListManagerApplyCPUListSkipsMissingClosForEmptyTarget(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, cpus), []byte("ffffffff\n"), 0o644))

	manager := newCPUListManager(root)
	require.NoError(t, manager.ApplyCPUList(context.Background(), "dedicated", ""))
	require.NoDirExists(t, filepath.Join(root, "dedicated"))
}

func TestCPUListManagerApplyCPUListRejectsCPUOutsideMaskWidth(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, cpus), []byte("ffffffff\n"), 0o644))
	require.NoError(t, os.Mkdir(filepath.Join(root, "dedicated"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "dedicated", cpus), []byte("ffffffff\n"), 0o644))

	manager := newCPUListManager(root)
	require.ErrorContains(t, manager.ApplyCPUList(context.Background(), "dedicated", "32"), "exceeds resctrl cpus mask width")
}

func TestCPUListManagerCPUListMatchesObservesLiveMask(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, cpus), []byte("ffffffff\n"), 0o644))
	require.NoError(t, os.Mkdir(filepath.Join(root, "dedicated"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "dedicated", cpus), []byte("00000002\n"), 0o644))
	manager := newCPUListManager(root)

	matches, err := manager.CPUListMatches(context.Background(), "dedicated", "1")
	require.NoError(t, err)
	require.True(t, matches)

	require.NoError(t, os.WriteFile(filepath.Join(root, "dedicated", cpus), []byte("00000004\n"), 0o644))
	matches, err = manager.CPUListMatches(context.Background(), "dedicated", "1")
	require.NoError(t, err)
	require.False(t, matches)
}

func TestCPUListManagerWritesExistingClosAndBumpsEpochAfterRecreate(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, cpus), []byte("ffffffff\n"), 0o644))
	closPath := filepath.Join(root, "dedicated")
	require.NoError(t, os.Mkdir(closPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(closPath, cpus), nil, 0o644))

	manager := newCPUListManager(root)
	clos, err := manager.ListManagedClos(context.Background())
	require.NoError(t, err)
	require.Len(t, clos, 1)
	require.NoError(t, manager.ApplyCPUList(context.Background(), "dedicated", "1-2"))
	got, err := os.ReadFile(filepath.Join(closPath, cpus))
	require.NoError(t, err)
	require.Equal(t, "00000006", string(got))

	require.NoError(t, os.RemoveAll(closPath))
	require.NoError(t, os.Mkdir(closPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(closPath, cpus), nil, 0o644))
	recreated, err := manager.ListManagedClos(context.Background())
	require.NoError(t, err)
	require.Len(t, recreated, 1)
	require.Greater(t, recreated[0].Epoch, clos[0].Epoch)
}

func TestCPUListManagerApplyCPUListWaitsForClosLifecycleUpdate(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, cpus), []byte("ffffffff\n"), 0o644))
	require.NoError(t, os.Mkdir(filepath.Join(root, "dedicated"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "dedicated", cpus), []byte("ffffffff\n"), 0o644))

	coordinator := newTestClosResourceCoordinator()
	manager := newCPUListManagerWithResourceUpdater(root, coordinator)
	lifecycleStarted := make(chan struct{})
	releaseLifecycle := make(chan struct{})
	lifecycleDone := make(chan struct{})
	go func() {
		defer close(lifecycleDone)
		require.NoError(t, coordinator.RunClosLifecycle("dedicated", func() (bool, error) {
			close(lifecycleStarted)
			<-releaseLifecycle
			return false, nil
		}))
	}()
	<-lifecycleStarted

	applyDone := make(chan error, 1)
	go func() {
		applyDone <- manager.ApplyCPUList(context.Background(), "dedicated", "1")
	}()

	select {
	case err := <-applyDone:
		require.NoError(t, err)
		t.Fatal("ApplyCPUList completed while CLOS lifecycle update was still running")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseLifecycle)
	<-lifecycleDone
	require.NoError(t, <-applyDone)
	content, err := os.ReadFile(filepath.Join(root, "dedicated", cpus))
	require.NoError(t, err)
	require.Equal(t, "00000002", string(content))
}

type testClosResourceCoordinator struct {
	mu     sync.Mutex
	closMu map[string]*sync.Mutex
}

func newTestClosResourceCoordinator() *testClosResourceCoordinator {
	return &testClosResourceCoordinator{closMu: make(map[string]*sync.Mutex)}
}

func (c *testClosResourceCoordinator) RunClosResourceUpdate(closID string, update func() (bool, error)) error {
	closMu := c.getClosMu(closID)
	closMu.Lock()
	defer closMu.Unlock()
	_, err := update()
	return err
}

func (c *testClosResourceCoordinator) RunClosLifecycle(closID string, update func() (bool, error)) error {
	closMu := c.getClosMu(closID)
	closMu.Lock()
	defer closMu.Unlock()
	_, err := update()
	return err
}

func (c *testClosResourceCoordinator) getClosMu(closID string) *sync.Mutex {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closMu[closID] == nil {
		c.closMu[closID] = &sync.Mutex{}
	}
	return c.closMu[closID]
}
