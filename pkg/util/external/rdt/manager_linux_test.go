//go:build linux
// +build linux

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

package rdt

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/consts"
)

var (
	clos            = "fake-clos"
	tasks           = []string{"0", "1"}
	defaultCATValue = "7ff"
	defaultMBAValue = 100
)

func TestNewDefaultManager(t *testing.T) {
	t.Parallel()

	defaultManager := NewDefaultManager()
	assert.NotNil(t, defaultManager)
}

func TestDefaultManagersShareSchemataCoordinator(t *testing.T) {
	first := NewDefaultManager().(*defaultRDTManager)
	second := NewDefaultManager().(*defaultRDTManager)
	require.Same(t, first.schemataCoordinator, second.schemataCoordinator)

	root := t.TempDir()
	writeSchemata(t, root, "clos", "L3:0=ff;\nMB:0=100;\n")
	first.schemataCoordinator.root = root
	first.schemataCoordinator.invalidateClos("clos")
	t.Cleanup(func() {
		first.schemataCoordinator.root = consts.DefaultResctrlRootDir
		first.schemataCoordinator.readFile = os.ReadFile
		first.schemataCoordinator.writeFile = os.WriteFile
		first.schemataCoordinator.invalidateClos("clos")
	})

	firstWriteStarted := make(chan struct{})
	unblockFirstWrite := make(chan struct{})
	var writeCalls int
	var mu sync.Mutex
	first.schemataCoordinator.writeFile = func(name string, data []byte, perm os.FileMode) error {
		mu.Lock()
		writeCalls++
		call := writeCalls
		mu.Unlock()
		if call == 1 {
			close(firstWriteStarted)
			<-unblockFirstWrite
		}
		return os.WriteFile(name, data, perm)
	}

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- first.ApplyCAT("clos", map[int]uint64{0: 0x3f})
	}()
	<-firstWriteStarted

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- second.ApplyMBA("clos", map[int]int{0: 60})
	}()
	select {
	case err := <-secondDone:
		t.Fatalf("second default manager update was not serialized, returned %v before first write completed", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(unblockFirstWrite)
	require.NoError(t, <-firstDone)
	require.NoError(t, <-secondDone)
	require.Equal(t, "L3:0=3f;\nMB:0=60;\n", readSchemata(t, root, "clos"))
}

func TestCheckSupportRDT(t *testing.T) {
	t.Parallel()

	defaultManager := NewDefaultManager()
	assert.NotNil(t, defaultManager)

	isSupport, err := defaultManager.CheckSupportRDT()
	assert.Error(t, err)
	assert.False(t, isSupport)
}

func TestInitRDT(t *testing.T) {
	t.Parallel()

	defaultManager := NewDefaultManager()
	assert.NotNil(t, defaultManager)

	err := defaultManager.InitRDT()
	assert.Error(t, err)
}

func TestApplyTasks(t *testing.T) {
	t.Parallel()

	defaultManager := NewDefaultManager()
	assert.NotNil(t, defaultManager)

	err := defaultManager.ApplyTasks(clos, tasks)
	assert.Error(t, err)
}

func TestApplyCAT(t *testing.T) {
	t.Parallel()

	defaultManager := NewDefaultManager()
	assert.NotNil(t, defaultManager)

	catInt64, err := strconv.ParseInt(defaultCATValue, 16, 32)
	assert.NoError(t, err)

	cat := map[int]uint64{
		0: uint64(catInt64),
		1: uint64(catInt64),
	}
	err = defaultManager.ApplyCAT(clos, cat)
	assert.Error(t, err)

	return
}

func TestApplyMBA(t *testing.T) {
	t.Parallel()

	defaultManager := NewDefaultManager()
	assert.NotNil(t, defaultManager)

	mba := map[int]int{
		0: defaultMBAValue,
		1: defaultMBAValue,
	}
	err := defaultManager.ApplyMBA(clos, mba)
	assert.Error(t, err)

	return
}

func TestDefaultRDTManagerCoordinatesCATAndMBA(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	closPath := filepath.Join(root, clos)
	assert.NoError(t, os.MkdirAll(closPath, 0o755))
	assert.NoError(t, os.WriteFile(filepath.Join(closPath, schemataFile),
		[]byte("L3:0=ff;\nMB:0=100;\n"), 0o644))
	manager := &defaultRDTManager{schemataCoordinator: newSchemataCoordinator(root)}

	assert.NoError(t, manager.ApplyCAT(clos, map[int]uint64{0: 0x3f}))
	assert.NoError(t, manager.ApplyMBA(clos, map[int]int{0: 60}))

	content, err := os.ReadFile(filepath.Join(closPath, schemataFile))
	assert.NoError(t, err)
	assert.Equal(t, "L3:0=3f;\nMB:0=60;\n", string(content))
}
