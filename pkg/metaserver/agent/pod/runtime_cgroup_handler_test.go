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

package pod

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func resetRuntimeCgroupPathHandlerInitializers(t *testing.T) {
	t.Helper()

	runtimeCgroupPathHandlerInitializers.Lock()
	old := append([]RuntimeCgroupPathHandlerInitializer(nil),
		runtimeCgroupPathHandlerInitializers.initializers...)
	runtimeCgroupPathHandlerInitializers.initializers = nil
	runtimeCgroupPathHandlerInitializers.Unlock()

	t.Cleanup(func() {
		runtimeCgroupPathHandlerInitializers.Lock()
		defer runtimeCgroupPathHandlerInitializers.Unlock()
		runtimeCgroupPathHandlerInitializers.initializers = old
	})
}

func TestInitializeRuntimeCgroupPathHandlersPreservesOrderAndFetcher(t *testing.T) {
	resetRuntimeCgroupPathHandlerInitializers(t)

	fetcher := &runtimePodFetcherStub{}
	gotOrder := make([]int, 0, 2)
	gotFetchers := make([]RuntimePodFetcher, 0, 2)
	RegisterRuntimeCgroupPathHandlerInitializer(func(runtimePodFetcher RuntimePodFetcher) {
		gotOrder = append(gotOrder, 1)
		gotFetchers = append(gotFetchers, runtimePodFetcher)
	})
	RegisterRuntimeCgroupPathHandlerInitializer(func(runtimePodFetcher RuntimePodFetcher) {
		gotOrder = append(gotOrder, 2)
		gotFetchers = append(gotFetchers, runtimePodFetcher)
	})

	initializeRuntimeCgroupPathHandlers(fetcher)

	require.Equal(t, []int{1, 2}, gotOrder)
	require.Equal(t, []RuntimePodFetcher{fetcher, fetcher}, gotFetchers)
}

func TestRegisterRuntimeCgroupPathHandlerInitializerIgnoresNil(t *testing.T) {
	resetRuntimeCgroupPathHandlerInitializers(t)

	RegisterRuntimeCgroupPathHandlerInitializer(nil)

	require.Empty(t, runtimeCgroupPathHandlerInitializers.snapshot())
}

func TestRuntimeCgroupPathHandlerInitializerRegistrySnapshotIsStable(t *testing.T) {
	resetRuntimeCgroupPathHandlerInitializers(t)

	RegisterRuntimeCgroupPathHandlerInitializer(func(RuntimePodFetcher) {})
	snapshot := runtimeCgroupPathHandlerInitializers.snapshot()
	RegisterRuntimeCgroupPathHandlerInitializer(func(RuntimePodFetcher) {})

	require.Len(t, snapshot, 1)
	require.Len(t, runtimeCgroupPathHandlerInitializers.snapshot(), 2)
}

func TestRuntimeCgroupPathHandlerInitializerRegistryConcurrentAccess(t *testing.T) {
	resetRuntimeCgroupPathHandlerInitializers(t)

	const registrations = 32
	var wg sync.WaitGroup
	wg.Add(registrations + 1)
	for i := 0; i < registrations; i++ {
		go func() {
			defer wg.Done()
			RegisterRuntimeCgroupPathHandlerInitializer(func(RuntimePodFetcher) {})
		}()
	}
	go func() {
		defer wg.Done()
		for i := 0; i < registrations; i++ {
			_ = runtimeCgroupPathHandlerInitializers.snapshot()
		}
	}()
	wg.Wait()

	require.Len(t, runtimeCgroupPathHandlerInitializers.snapshot(), registrations)
}
