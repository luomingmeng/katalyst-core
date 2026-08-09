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

import "sync"

type RuntimeCgroupPathHandlerInitializer func(RuntimePodFetcher)

type runtimeCgroupPathHandlerInitializerRegistry struct {
	sync.RWMutex
	initializers []RuntimeCgroupPathHandlerInitializer
}

var runtimeCgroupPathHandlerInitializers runtimeCgroupPathHandlerInitializerRegistry

func RegisterRuntimeCgroupPathHandlerInitializer(initializer RuntimeCgroupPathHandlerInitializer) {
	if initializer == nil {
		return
	}

	runtimeCgroupPathHandlerInitializers.Lock()
	defer runtimeCgroupPathHandlerInitializers.Unlock()
	runtimeCgroupPathHandlerInitializers.initializers = append(
		runtimeCgroupPathHandlerInitializers.initializers,
		initializer,
	)
}

func (r *runtimeCgroupPathHandlerInitializerRegistry) snapshot() []RuntimeCgroupPathHandlerInitializer {
	r.RLock()
	defer r.RUnlock()
	return append([]RuntimeCgroupPathHandlerInitializer(nil), r.initializers...)
}

func initializeRuntimeCgroupPathHandlers(runtimePodFetcher RuntimePodFetcher) {
	for _, initializer := range runtimeCgroupPathHandlerInitializers.snapshot() {
		initializer(runtimePodFetcher)
	}
}
