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

package resourcepoolvalidator

import (
	"context"

	v1 "k8s.io/api/core/v1"

	"github.com/kubewharf/katalyst-core/pkg/util/general"
)

// MaskExceeds reports whether admitting `incomingPerNUMA` to every NUMA node
// referenced by `maskBits` would exceed the pool's MaxAllocatable for any
// resource in `resources`.
//
// When allocCache is non-nil it is used as a pre-fetched allocation map (key:
// numaID) to avoid redundant Validator.GetAllocated calls across multiple
// masks. Callers that iterate many masks over the same set of NUMA nodes
// (e.g. inside IterateBitMasks) should pre-populate the cache via
// Validator.PrefetchNumaAllocations once before the loop. When allocCache is
// nil or does not contain a given numaID, that NUMA's allocation is fetched
// on demand — making it safe to use even without a complete cache.
//
// excludePodUIDs is forwarded to Validator.Validate (and ultimately
// GetAllocated) so that the specified pods are excluded from the allocated
// total. This avoids double-counting during inplace-update-resize.
func MaskExceeds(
	ctx context.Context,
	validator Validator,
	poolName string,
	incomingPerNUMA v1.ResourceList,
	resources []v1.ResourceName,
	maskBits []int,
	allocCache map[int]v1.ResourceList,
	excludePodUIDs ...string,
) bool {
	if validator == nil || poolName == "" {
		return false
	}
	if len(maskBits) == 0 || len(resources) == 0 || len(incomingPerNUMA) == 0 {
		return false
	}
	for _, numaID := range maskBits {
		var alloc v1.ResourceList
		if allocCache != nil {
			alloc = allocCache[numaID]
		}
		err := validator.Validate(ctx, poolName, NumaScope(numaID),
			incomingPerNUMA, resources, alloc, excludePodUIDs...)
		if err == nil {
			continue
		}
		if IsCapacityExceeded(err) {
			general.Infof("ResourcePool %q exceeds NumaScope=%d (mask=%v): %v",
				poolName, numaID, maskBits, err)
			return true
		}
		general.Warningf("ResourcePool MaskExceeds pool=%q numa=%d ignored err: %v",
			poolName, numaID, err)
		return false
	}
	return false
}
