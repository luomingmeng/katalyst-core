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

package dynamicpolicy

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	rpvalidator "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/util/resourcepoolvalidator"
	resourcepool "github.com/kubewharf/katalyst-core/pkg/util/resource-pool"
)

// cpuResourcePoolAllocatedProvider implements rpvalidator.AllocatedProvider
// for the CPU dynamic policy by walking the in-memory PodEntries.
type cpuResourcePoolAllocatedProvider struct {
	state state.ReadonlyState
}

// newCPUResourcePoolAllocatedProvider returns an AllocatedProvider that
// reports CPU usage aggregated by resource pool from the given state.
func newCPUResourcePoolAllocatedProvider(s state.ReadonlyState) rpvalidator.AllocatedProvider {
	return &cpuResourcePoolAllocatedProvider{state: s}
}

// GetAllocated returns the already-allocated CPU quantity for the given pool
// within the requested scope.
//
//   - When scope.NumaID == resourcepool.NumaIDAll, the full RequestQuantity of
//     each matching container is summed (NodeScope).
//   - When scope.NumaID >= 0 (NumaScope), accounting is split by binding type:
//     SNB (shared_cores numa-binding) containers contribute their whole
//     RequestQuantity against the persisted target NUMA hint; DNB
//     (dedicated_cores numa-binding) containers are pro-rated by the cpuset
//     share landing on this NUMA via TopologyAwareAssignments. Containers
//     that are neither SNB nor DNB are skipped under NumaScope because they
//     are not bound to a specific NUMA.
func (c *cpuResourcePoolAllocatedProvider) GetAllocated(poolName string, scope rpvalidator.Scope) (v1.ResourceList, error) {
	if c == nil || c.state == nil || poolName == "" {
		return nil, nil
	}

	var totalMilli int64
	for _, containerEntries := range c.state.GetPodEntries() {
		if containerEntries.IsPoolEntry() {
			continue
		}

		for _, ai := range containerEntries {
			if ai == nil {
				continue
			}
			if resourcepool.GetResourcePoolName(ai.Annotations) != poolName {
				continue
			}

			if scope.NumaID == resourcepool.NumaIDAll {
				totalMilli += int64(ai.RequestQuantity * 1000)
				continue
			}

			switch {
			case ai.CheckSharedNUMABinding():
				// SNB binds to exactly one NUMA. Account the whole
				// RequestQuantity against that NUMA. We rely on the persisted
				// NUMA hint annotation (set in allocateSharedNumaBindingCPUs)
				// rather than walking TopologyAwareAssignments, because the
				// latter is allowed to temporarily span multiple NUMAs during
				// ramp-up.
				targetNUMA, err := ai.GetSpecifiedNUMABindingNUMAID()
				if err != nil || targetNUMA != scope.NumaID {
					continue
				}
				totalMilli += int64(ai.RequestQuantity * 1000)

			case ai.CheckDedicatedNUMABinding():
				// DNB has its cpuset strictly partitioned per NUMA in
				// TopologyAwareAssignments by machine.GetNumaAwareAssignments.
				// Pro-rate the RequestQuantity by the share landing on this
				// NUMA.
				share, total := 0, 0
				for numaID, cset := range ai.TopologyAwareAssignments {
					size := cset.Size()
					total += size
					if numaID == scope.NumaID {
						share = size
					}
				}
				if total == 0 || share == 0 {
					continue
				}
				totalMilli += int64(ai.RequestQuantity * 1000 * float64(share) / float64(total))

			default:
				// Other QoS classes are not bound to a specific NUMA and
				// therefore do not contribute to per-NUMA ResourcePool
				// capacity accounting.
				continue
			}
		}
	}

	return v1.ResourceList{
		v1.ResourceCPU: *resource.NewMilliQuantity(totalMilli, resource.DecimalSI),
	}, nil
}

// validateResourcePool enforces ResourcePool MaxAllocatable for an incoming
// CPU request at NodeScope. Pods without a resource pool annotation are
// skipped.
//
// NodeScope-only: NumaScope is now enforced earlier, in the GetTopologyHints
// path via cpuResourcePoolMaskExceeds, so candidate NUMA combinations that
// would breach the per-NUMA capacity never reach Allocate.
func (p *DynamicPolicy) validateResourcePool(ctx context.Context, req *pluginapi.ResourceRequest, reqFloat64 float64) error {
	if p == nil || p.resourcePoolValidator == nil || req == nil {
		return nil
	}
	poolName := resourcepool.GetResourcePoolName(req.Annotations)
	if poolName == "" {
		return nil
	}

	incoming := v1.ResourceList{
		v1.ResourceCPU: *resource.NewMilliQuantity(int64(reqFloat64*1000), resource.DecimalSI),
	}
	return p.resourcePoolValidator.Validate(ctx, poolName, rpvalidator.NodeScope(), incoming, []v1.ResourceName{v1.ResourceCPU}, nil)
}

// cpuResourcePoolMaskExceeds is the CPU-flavoured wrapper of
// resourcepoolvalidator.MaskExceeds. It hides the (ResourceCPU only) resource
// list / MilliQuantity construction noise from hint handlers.
//
// `perNUMACPU` is the per-NUMA share (in cores, possibly fractional) that
// every NUMA node in `maskBits` would receive. Callers compute it as:
//   - SNB single NUMA:   request          (mask size == 1)
//   - DNB / distribute:  request / count  (mask size == count)
func (p *DynamicPolicy) cpuResourcePoolMaskExceeds(
	ctx context.Context,
	req *pluginapi.ResourceRequest,
	perNUMACPU float64,
	maskBits []int,
	allocCache map[int]v1.ResourceList,
) bool {
	if p == nil || req == nil || perNUMACPU <= 0 || len(maskBits) == 0 {
		return false
	}
	poolName := resourcepool.GetResourcePoolName(req.Annotations)
	if poolName == "" {
		return false
	}
	return rpvalidator.MaskExceeds(ctx, p.resourcePoolValidator, poolName,
		v1.ResourceList{
			v1.ResourceCPU: *resource.NewMilliQuantity(int64(perNUMACPU*1000), resource.DecimalSI),
		},
		[]v1.ResourceName{v1.ResourceCPU},
		maskBits,
		allocCache,
	)
}
