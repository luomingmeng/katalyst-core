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
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"

	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	cpuconsts "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/calculator"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	dynamicpolicyutil "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/util"
	cpuutil "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/util"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/util"
	resourcehelper "github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/plugin/qosaware/resource/helper"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	"github.com/kubewharf/katalyst-core/pkg/util/native"
	qosutil "github.com/kubewharf/katalyst-core/pkg/util/qos"
	rputil "github.com/kubewharf/katalyst-core/pkg/util/resource-package"
)

type requestStateCompensatedError struct {
	err error
}

func (e *requestStateCompensatedError) Error() string {
	return e.err.Error()
}

func (e *requestStateCompensatedError) Unwrap() error {
	return e.err
}

type requestStateOwnershipLostError struct {
	err error
}

func (e *requestStateOwnershipLostError) Error() string {
	return e.err.Error()
}

func (e *requestStateOwnershipLostError) Unwrap() error {
	return e.err
}

func (e *requestStateOwnershipLostError) OwnershipLost() bool {
	return true
}

func (p *DynamicPolicy) sharedCoresAllocationHandler(ctx context.Context,
	req *pluginapi.ResourceRequest,
	persistCheckpoint bool,
) (*pluginapi.ResourceAllocationResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("sharedCoresAllocationHandler got nil req")
	}

	switch req.Annotations[apiconsts.PodAnnotationMemoryEnhancementNumaBinding] {
	case apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable:
		return p.sharedCoresWithNUMABindingAllocationHandler(ctx, req, persistCheckpoint)
	default:
		return p.sharedCoresWithoutNUMABindingAllocationHandler(ctx, req, persistCheckpoint)
	}
}

func (p *DynamicPolicy) sharedCoresWithoutNUMABindingAllocationHandler(ctx context.Context,
	req *pluginapi.ResourceRequest, persistCheckpoint bool,
) (*pluginapi.ResourceAllocationResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("sharedCoresAllocationHandler got nil request")
	}

	_, reqFloat64, err := util.GetQuantityFromResourceReq(req)
	if err != nil {
		return nil, fmt.Errorf("getReqQuantityFromResourceReq failed with error: %v", err)
	}

	machineState := p.state.GetMachineState()
	pooledCPUs := machineState.GetFilteredAvailableCPUSet(p.reservedCPUs,
		state.WrapAllocationMetaFilter((*commonstate.AllocationMeta).CheckDedicated),
		state.WrapAllocationMetaFilter((*commonstate.AllocationMeta).CheckSharedOrDedicatedNUMABinding))
	// cores that are not allocatable from user binding need to be deducted from the pool.
	notAllocatablePoolsCPUs := state.GetUnitedPoolsCPUs(p.state.GetPodEntries(), state.IsForbiddenPool, commonstate.IsSystemPool)
	pooledCPUs = pooledCPUs.Difference(notAllocatablePoolsCPUs)

	if pooledCPUs.IsEmpty() {
		general.Errorf("pod: %s/%s, container: %s get empty pooledCPUs", req.PodNamespace, req.PodName, req.ContainerName)
		return nil, fmt.Errorf("get empty pooledCPUs")
	}

	pooledCPUsTopologyAwareAssignments, err := machine.GetNumaAwareAssignments(p.machineInfo.CPUTopology, pooledCPUs)
	if err != nil {
		general.Errorf("pod: %s/%s, container: %s GetTopologyAwareAssignmentsByCPUSet failed with error: %v",
			req.PodNamespace, req.PodName, req.ContainerName, err)
		return nil, fmt.Errorf("GetTopologyAwareAssignmentsByCPUSet failed with error: %v", err)
	}
	excludeRampUpReclaimFloor := func() error {
		rampUpReclaimFloor, err := p.deriveRampUpReclaimFloor(machineState, true)
		if err != nil {
			return fmt.Errorf("derive reclaim floor for shared_cores ramp-up failed: %w", err)
		}
		pooledCPUs = pooledCPUs.Difference(rampUpReclaimFloor)
		if pooledCPUs.IsEmpty() {
			return fmt.Errorf("shared_cores ramp-up cpuset is empty after excluding reclaim floor %s",
				rampUpReclaimFloor.String())
		}
		pooledCPUsTopologyAwareAssignments, err = machine.GetNumaAwareAssignments(p.machineInfo.CPUTopology, pooledCPUs)
		if err != nil {
			return fmt.Errorf("calculate shared_cores ramp-up assignments after excluding reclaim floor failed: %w", err)
		}
		return nil
	}

	needSet := true
	allocationInfo := p.state.GetAllocationInfo(req.PodUid, req.ContainerName)
	originAllocationInfo := allocationInfo.Clone()
	err = updateAllocationInfoByReq(req, allocationInfo)
	if err != nil {
		general.Errorf("pod: %s/%s, container: %s updateAllocationInfoByReq failed with error: %v",
			req.PodNamespace, req.PodName, req.ContainerName, err)
		return nil, fmt.Errorf("updateAllocationInfoByReq failed with error: %v", err)
	}

	if allocationInfo == nil {
		general.Infof("pod: %s/%s, container: %s is met firstly, do ramp up with pooled cpus: %s",
			req.PodNamespace, req.PodName, req.ContainerName, pooledCPUs.String())

		shouldRampUp := p.shouldSharedCoresRampUp(ctx, req.PodUid)
		if shouldRampUp {
			if err := excludeRampUpReclaimFloor(); err != nil {
				return nil, err
			}
		}

		allocationInfo = &state.AllocationInfo{
			AllocationMeta: commonstate.GenerateGenericContainerAllocationMeta(req,
				commonstate.EmptyOwnerPoolName, apiconsts.PodAnnotationQoSLevelSharedCores),
			RampUp:                           shouldRampUp,
			AllocationResult:                 pooledCPUs,
			OriginalAllocationResult:         pooledCPUs.Clone(),
			TopologyAwareAssignments:         pooledCPUsTopologyAwareAssignments,
			OriginalTopologyAwareAssignments: machine.DeepcopyCPUAssignment(pooledCPUsTopologyAwareAssignments),
			InitTimestamp:                    time.Now().Format(util.QRMTimeFormat),
			RequestQuantity:                  reqFloat64,
		}

		if !shouldRampUp {
			targetPoolName := allocationInfo.GetSpecifiedPoolName()
			poolAllocationInfo := p.state.GetAllocationInfo(targetPoolName, commonstate.FakedContainerName)

			if poolAllocationInfo == nil {
				if p.isSharedCoresRampUpDisabled() {
					// cold-start bootstrap: the target pool entry is not ready yet, but
					// DisableSharedCoresRampUp=true forbids binding this pod to the broad
					// pooledCPUs. Delegate to the assembler pipeline (adjustPoolsAndIsolatedEntries
					// -> generatePoolsAndIsolation -> reviseReclaimPool) to seed the pool entry
					// together with this pod's allocation atomically, honoring overlap/pkg/numa
					// semantics without duplicating the logic here.
					general.Infof("pod: %s/%s, container: %s cold-start seeding target pool %s under DisableSharedCoresRampUp",
						req.PodNamespace, req.PodName, req.ContainerName, targetPoolName)

					if err := p.updateAllocationInfo(allocationInfo.PodUid, allocationInfo.ContainerName, originAllocationInfo, allocationInfo, persistCheckpoint); err != nil {
						return nil, err
					}
					checkedAllocationInfo, err := p.doAndCheckPutAllocationInfo(allocationInfo, true, persistCheckpoint)
					if err != nil {
						// roll back the freshly-inserted allocation to keep pod entries clean
						p.state.Delete(allocationInfo.PodUid, allocationInfo.ContainerName, persistCheckpoint)
						general.Errorf("pod: %s/%s, container: %s cold-start seed pool %s failed: %v",
							req.PodNamespace, req.PodName, req.ContainerName, targetPoolName, err)
						return nil, fmt.Errorf("cold-start seed pool %s failed: %v", targetPoolName, err)
					}

					_ = p.emitter.StoreInt64(util.MetricNameSharedCoresRampUpDisabledSeeded, 1,
						metrics.MetricTypeNameCount,
						metrics.MetricTag{Key: "poolName", Val: targetPoolName},
						metrics.MetricTag{Key: "overlap", Val: strconv.FormatBool(p.state.GetAllowSharedCoresOverlapReclaimedCores())},
					)

					allocationInfo = checkedAllocationInfo
					needSet = false
				} else {
					general.Infof("pod: %s/%s, container: %s is active, but its specified pool entry doesn't exist, try to ramp up it",
						req.PodNamespace, req.PodName, req.ContainerName)
					if err := excludeRampUpReclaimFloor(); err != nil {
						return nil, err
					}
					allocationInfo.RampUp = true
					allocationInfo.AllocationResult = pooledCPUs
					allocationInfo.OriginalAllocationResult = pooledCPUs.Clone()
					allocationInfo.TopologyAwareAssignments = pooledCPUsTopologyAwareAssignments
					allocationInfo.OriginalTopologyAwareAssignments = machine.DeepcopyCPUAssignment(pooledCPUsTopologyAwareAssignments)
				}
			} else if p.isSharedCoresRampUpDisabled() {
				allocationInfo.AllocationResult = poolAllocationInfo.AllocationResult.Clone()
				allocationInfo.OriginalAllocationResult = poolAllocationInfo.OriginalAllocationResult.Clone()
				allocationInfo.TopologyAwareAssignments = machine.DeepcopyCPUAssignment(poolAllocationInfo.TopologyAwareAssignments)
				allocationInfo.OriginalTopologyAwareAssignments = machine.DeepcopyCPUAssignment(poolAllocationInfo.OriginalTopologyAwareAssignments)
			} else {
				if err := p.updateAllocationInfo(allocationInfo.PodUid, allocationInfo.ContainerName, originAllocationInfo, allocationInfo, persistCheckpoint); err != nil {
					return nil, err
				}
				_, err = p.doAndCheckPutAllocationInfo(allocationInfo, false, persistCheckpoint)
				if err != nil {
					return nil, err
				}

				needSet = false
			}
		}
	} else if allocationInfo.RampUp {
		if util.PodInplaceUpdateResizing(req) {
			general.Errorf("pod: %s/%s, container: %s is still in ramp up, not allow to inplace update resize",
				req.PodNamespace, req.PodName, req.ContainerName)
			return nil, fmt.Errorf("pod is still ramp up, not allow to inplace update resize")
		}

		if err := excludeRampUpReclaimFloor(); err != nil {
			return nil, err
		}
		general.Infof("pod: %s/%s, container: %s is still in ramp up, allocate pooled cpus: %s",
			req.PodNamespace, req.PodName, req.ContainerName, pooledCPUs.String())

		allocationInfo.AllocationResult = pooledCPUs
		allocationInfo.OriginalAllocationResult = pooledCPUs.Clone()
		allocationInfo.TopologyAwareAssignments = pooledCPUsTopologyAwareAssignments
		allocationInfo.OriginalTopologyAwareAssignments = machine.DeepcopyCPUAssignment(pooledCPUsTopologyAwareAssignments)
	} else {
		if util.PodInplaceUpdateResizing(req) {
			general.Infof("pod: %s/%s, container: %s request to inplace update resize (%.02f->%.02f)",
				req.PodNamespace, req.PodName, req.ContainerName, allocationInfo.RequestQuantity, reqFloat64)
			allocationInfo.RequestQuantity = reqFloat64

			if err := p.updateAllocationInfo(allocationInfo.PodUid, allocationInfo.ContainerName, originAllocationInfo, allocationInfo, persistCheckpoint); err != nil {
				return nil, err
			}
			_, err := p.doAndCheckPutAllocationInfoPodResizingAware(originAllocationInfo, allocationInfo, false, true, persistCheckpoint)
			if err != nil {
				general.Errorf("pod: %s/%s, container: %s doAndCheckPutAllocationInfoPodResizingAware failed: %q",
					req.PodNamespace, req.PodName, req.ContainerName, err)
				p.state.SetAllocationInfo(originAllocationInfo.PodUid, originAllocationInfo.ContainerName, originAllocationInfo, persistCheckpoint)
				return nil, err
			}
		} else {
			_, err := p.doAndCheckPutAllocationInfo(allocationInfo, true, persistCheckpoint)
			if err != nil {
				general.Errorf("pod: %s/%s, container: %s doAndCheckPutAllocationInfo failed: %q",
					req.PodNamespace, req.PodName, req.ContainerName, err)
				return nil, err
			}
		}
		needSet = false
	}

	if needSet {
		// update pod entries directly.
		// if one of subsequent steps is failed,
		// we will delete current allocationInfo from podEntries in defer function of allocation function.
		if err := p.updateAllocationInfo(allocationInfo.PodUid, allocationInfo.ContainerName, originAllocationInfo, allocationInfo, persistCheckpoint); err != nil {
			return nil, err
		}
		podEntries := p.state.GetPodEntries()

		updatedMachineState, err := generateMachineStateFromPodEntries(p.machineInfo.CPUTopology, podEntries, p.state.GetMachineState())
		if err != nil {
			general.Errorf("pod: %s/%s, container: %s GenerateMachineStateFromPodEntries failed with error: %v",
				req.PodNamespace, req.PodName, req.ContainerName, err)
			return nil, fmt.Errorf("GenerateMachineStateFromPodEntries failed with error: %v", err)
		}
		p.state.SetMachineState(updatedMachineState, persistCheckpoint)
	}

	resp, err := cpuutil.PackAllocationResponse(allocationInfo, string(v1.ResourceCPU), util.OCIPropertyNameCPUSetCPUs, false, true, req, allocationInfo.Annotations)
	if err != nil {
		general.Errorf("pod: %s/%s, container: %s packAllocationResponse failed with error: %v",
			req.PodNamespace, req.PodName, req.ContainerName, err)
		return nil, fmt.Errorf("PackResourceAllocationResponseByAllocationInfo failed with error: %v", err)
	}
	p.clearCPUSetInAllocationResponseIfNeeded(resp, allocationInfo)
	return resp, nil
}

func (p *DynamicPolicy) reclaimedCoresAllocationHandler(ctx context.Context,
	req *pluginapi.ResourceRequest, persistCheckpoint bool,
) (*pluginapi.ResourceAllocationResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("reclaimedCoresAllocationHandler got nil request")
	}

	if req.ContainerType == pluginapi.ContainerType_SIDECAR {
		return p.allocationSidecarHandler(ctx, req, apiconsts.PodAnnotationQoSLevelReclaimedCores, persistCheckpoint)
	}

	_, reqFloat64, err := util.GetQuantityFromResourceReq(req)
	if err != nil {
		return nil, fmt.Errorf("getReqQuantityFromResourceReq failed with error: %v", err)
	}

	reclaimedAllocationInfo := p.state.GetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName)
	if reclaimedAllocationInfo == nil {
		general.Errorf("allocation for pod: %s/%s, container: %s is failed, because pool: %s is not ready",
			req.PodNamespace, req.PodName, req.ContainerName, commonstate.PoolNameReclaim)

		return nil, fmt.Errorf("pool: %s is not ready", commonstate.PoolNameReclaim)
	} else if reclaimedAllocationInfo.AllocationResult.Size() == 0 {
		general.Errorf("allocation for pod: %s/%s, container: %s is failed, because pool: %s is empty",
			req.PodNamespace, req.PodName, req.ContainerName, commonstate.PoolNameReclaim)

		return nil, fmt.Errorf("pool: %s is not empty", commonstate.PoolNameReclaim)
	}

	allocationInfo := p.state.GetAllocationInfo(req.PodUid, req.ContainerName)
	originAllocationInfo := allocationInfo.Clone()
	if util.PodInplaceUpdateResizing(req) {
		if allocationInfo == nil {
			return nil, fmt.Errorf("pod request to cpu inplace update resize, but origin allocationInfo is nil")
		}
		if qosutil.AnnotationsIndicateNUMABinding(req.Annotations) != allocationInfo.CheckNUMABinding() {
			return nil, fmt.Errorf("can not change qos form non-rnb to rnb or vice versa during inplace update resize")
		}
		if allocationInfo.CheckNUMABinding() {
			reqNumaSet, err := machine.NewCPUSetUint64(req.Hint.Nodes...)
			if err != nil {
				return nil, fmt.Errorf("failed to parse request hint numa set: %v", err)
			}
			if !reqNumaSet.Equals(allocationInfo.GetAllocationResultNUMASet()) {
				return nil, fmt.Errorf("can not change the binding numa during inplace update resize")
			}
		}
		general.Infof("pod: %s/%s, container: %s request to cpu inplace update resize allocation, request: %.2f->%.2f",
			req.PodNamespace, req.PodName, req.ContainerName, allocationInfo.RequestQuantity, reqFloat64)
		allocationInfo.RequestQuantity = reqFloat64
		if err := p.updateAllocationInfo(req.PodUid, req.ContainerName, originAllocationInfo, allocationInfo, persistCheckpoint); err != nil {
			return nil, err
		}
	} else {
		err = updateAllocationInfoByReq(req, allocationInfo)
		if err != nil {
			general.Errorf("pod: %s/%s, container: %s updateAllocationInfoByReq failed with error: %v",
				req.PodNamespace, req.PodName, req.ContainerName, err)
			return nil, fmt.Errorf("updateAllocationInfoByReq failed with error: %v", err)
		}

		machineState := p.state.GetMachineState()
		// calculate NUMAs without actual numa_binding reclaimed pods
		nonReclaimActualBindingNUMAs := machineState.GetFilteredNUMASet(state.WrapAllocationMetaFilter((*commonstate.AllocationMeta).CheckReclaimedActualNUMABinding))
		if allocationInfo != nil {
			general.Infof("pod: %s/%s, container: %s with old allocation result: %s, allocate by reclaimedCPUSet: %s",
				req.PodNamespace, req.PodName, req.ContainerName, allocationInfo.AllocationResult.String(), reclaimedAllocationInfo.AllocationResult.String())
		} else {
			general.Infof("pod: %s/%s, container: %s is firstly met, allocate by reclaimedCPUSet: %s",
				req.PodNamespace, req.PodName, req.ContainerName, reclaimedAllocationInfo.AllocationResult.String())

			allocationInfo = &state.AllocationInfo{
				AllocationMeta: commonstate.GenerateGenericContainerAllocationMeta(req,
					commonstate.PoolNameReclaim, apiconsts.PodAnnotationQoSLevelReclaimedCores),
				InitTimestamp:   time.Now().Format(util.QRMTimeFormat),
				RequestQuantity: reqFloat64,
			}

			// calculate NUMAs without non-actual numa_binding reclaimed pods
			reclaimActualBindingNUMAs := machineState.GetFilteredNUMASet(state.WrapAllocationMetaFilter((*commonstate.AllocationMeta).CheckReclaimedNonActualNUMABinding))
			// set reclaimed numa_binding NUMA ID to allocationInfo
			if req.Hint != nil && len(req.Hint.Nodes) == 1 && (reclaimActualBindingNUMAs.Contains(int(req.Hint.Nodes[0])) ||
				!nonReclaimActualBindingNUMAs.Equals(machine.NewCPUSet(int(req.Hint.Nodes[0])))) {
				allocationInfo.SetSpecifiedNUMABindingNUMAID(req.Hint.Nodes)
			}
		}

		// update reclaimed allocation result by pool entry
		err = p.updateReclaimAllocationResultByPoolEntry(allocationInfo, reclaimedAllocationInfo, nonReclaimActualBindingNUMAs)
		if err != nil {
			return nil, err
		}

		// update pod entries directly.
		// if one of subsequent steps is failed, we will delete current allocationInfo from podEntries in defer function of allocation function.
		if err := p.updateAllocationInfo(allocationInfo.PodUid, allocationInfo.ContainerName, originAllocationInfo, allocationInfo, persistCheckpoint); err != nil {
			return nil, err
		}

		// update reclaim non-actual numa_binding reclaim cores allocations if it needs to transfer a non-RNB numa to RNB numa
		podEntries := p.state.GetPodEntries()
		if allocationInfo.CheckActualNUMABinding() &&
			nonReclaimActualBindingNUMAs.Intersection(allocationInfo.AllocationResult).Size() > 0 {
			updatedNonReclaimActualBindingNUMAs := nonReclaimActualBindingNUMAs.Difference(allocationInfo.AllocationResult)
			err := p.updateNonActualNUMABindingReclaimCoresAllocations(podEntries, updatedNonReclaimActualBindingNUMAs, reclaimedAllocationInfo)
			if err != nil {
				general.Errorf("pod: %s/%s, container: %s updateNonActualNUMABindingReclaimCoresAllocations failed with error: %v",
					req.PodNamespace, req.PodName, req.ContainerName, err)
				return nil, err
			}
		}

		updatedMachineState, err := generateMachineStateFromPodEntries(p.machineInfo.CPUTopology, podEntries, machineState)
		if err != nil {
			general.Errorf("pod: %s/%s, container: %s GenerateMachineStateFromPodEntries failed with error: %v",
				req.PodNamespace, req.PodName, req.ContainerName, err)
			return nil, fmt.Errorf("GenerateMachineStateFromPodEntries failed with error: %v", err)
		}

		p.state.SetMachineState(updatedMachineState, persistCheckpoint)
	}

	resp, err := cpuutil.PackAllocationResponse(allocationInfo, string(v1.ResourceCPU), util.OCIPropertyNameCPUSetCPUs, false, true, req, allocationInfo.Annotations)
	if err != nil {
		general.Errorf("pod: %s/%s, container: %s packAllocationResponse failed with error: %v",
			req.PodNamespace, req.PodName, req.ContainerName, err)
		return nil, fmt.Errorf("PackResourceAllocationResponseByAllocationInfo failed with error: %v", err)
	}
	p.clearCPUSetInAllocationResponseIfNeeded(resp, allocationInfo)

	return resp, nil
}

// updateReclaimAllocationResultByPoolEntry updates non-actual numa binding reclaimed allocation result by pool entry
func (p *DynamicPolicy) updateNonActualNUMABindingReclaimCoresAllocations(podEntries state.PodEntries,
	nonReclaimActualBindingNUMAs machine.CPUSet, poolEntry *state.AllocationInfo,
) error {
	nonActualNUMABindingAllocations := podEntries.GetFilteredPodEntries(state.WrapAllocationMetaFilter((*commonstate.AllocationMeta).CheckReclaimedNonActualNUMABinding))
	for _, containerEntries := range nonActualNUMABindingAllocations {
		for _, allocationInfo := range containerEntries {
			err := p.updateReclaimAllocationResultByPoolEntry(allocationInfo, poolEntry, nonReclaimActualBindingNUMAs)
			if err != nil {
				return fmt.Errorf("updateReclaimAllocationResultByPoolEntry with error: %v", err)
			}
		}
	}
	return nil
}

func (p *DynamicPolicy) dedicatedCoresAllocationHandler(ctx context.Context,
	req *pluginapi.ResourceRequest, persistCheckpoint bool,
) (*pluginapi.ResourceAllocationResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("dedicatedCoresAllocationHandler got nil req")
	}

	if util.PodInplaceUpdateResizing(req) {
		return nil, fmt.Errorf("not support inplace update resize for dedicated cores")
	}

	switch req.Annotations[apiconsts.PodAnnotationMemoryEnhancementNumaBinding] {
	case apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable:
		return p.dedicatedCoresWithNUMABindingAllocationHandler(ctx, req, persistCheckpoint)
	default:
		return p.dedicatedCoresWithoutNUMABindingAllocationHandler(ctx, req, persistCheckpoint)
	}
}

func (p *DynamicPolicy) dedicatedCoresWithoutNUMABindingAllocationHandler(_ context.Context,
	_ *pluginapi.ResourceRequest, persistCheckpoint bool,
) (*pluginapi.ResourceAllocationResponse, error) {
	// todo: support dedicated_cores without NUMA binding
	return nil, fmt.Errorf("not support dedicated_cores without NUMA binding")
}

func (p *DynamicPolicy) dedicatedCoresWithNUMABindingAllocationHandler(ctx context.Context,
	req *pluginapi.ResourceRequest, persistCheckpoint bool,
) (*pluginapi.ResourceAllocationResponse, error) {
	if req.ContainerType == pluginapi.ContainerType_SIDECAR {
		return p.allocationSidecarHandler(ctx, req, apiconsts.PodAnnotationQoSLevelDedicatedCores, persistCheckpoint)
	}

	basePodEntries := p.state.GetPodEntries()
	baseMachineState := p.state.GetMachineState()
	oldAllocationInfo := p.state.GetAllocationInfo(req.PodUid, req.ContainerName)
	if oldAllocationInfo != nil {
		if basePodEntries[req.PodUid] != nil {
			delete(basePodEntries[req.PodUid], req.ContainerName)
			if len(basePodEntries[req.PodUid]) == 0 {
				delete(basePodEntries, req.PodUid)
			}
		}
		var err error
		baseMachineState, err = generateMachineStateFromPodEntries(
			p.machineInfo.CPUTopology, basePodEntries, baseMachineState)
		if err != nil {
			general.Errorf("pod: %s/%s, container: %s GenerateMachineStateFromPodEntries failed with error: %v",
				req.PodNamespace, req.PodName, req.ContainerName, err)
			return nil, fmt.Errorf("GenerateMachineStateFromPodEntries failed with error: %v", err)
		}
	}
	machineState := baseMachineState

	podAggregatedRequest, _, err := util.GetPodAggregatedRequestResource(req)
	if err != nil {
		return nil, fmt.Errorf("GetPodAggregatedRequestResource failed with error: %v", err)
	}

	reqInt, reqFloat64, err := util.GetQuantityFromResourceReq(req)
	if err != nil {
		return nil, fmt.Errorf("getReqQuantityFromResourceReq failed with error: %v", err)
	}

	podReclaimEnabled := p.podEnableReclaimOrFallback(ctx, req.PodUid, "allocateNumaBindingCPUs")

	result, hardReclaimCPUs, eligibility, err := p.allocateNumaBindingCPUsWithEligibility(
		podAggregatedRequest, req.Hint, machineState, req.Annotations, podReclaimEnabled)
	if err != nil {
		general.ErrorS(err, "unable to allocate CPUs",
			"podNamespace", req.PodNamespace,
			"podName", req.PodName,
			"containerName", req.ContainerName,
			"podAggregatedRequest", podAggregatedRequest,
			"numCPUsInt", reqInt,
			"numCPUsFloat64", reqFloat64)
		return nil, err
	}
	if !hardReclaimCPUs.IsEmpty() {
		general.InfoS("ramp-up reclaim hard partition carved cpus out of dedicated allocation",
			"podNamespace", req.PodNamespace,
			"podName", req.PodName,
			"containerName", req.ContainerName,
			"hardReclaimCPUs", hardReclaimCPUs.String())
	}

	// avoid running services on not allocatable CPUs.
	notAllocatablePoolsCPUs := state.GetUnitedPoolsCPUs(p.state.GetPodEntries(), state.IsForbiddenPool, commonstate.IsSystemPool)
	result = result.Difference(notAllocatablePoolsCPUs)

	general.InfoS("allocate CPUs successfully",
		"podNamespace", req.PodNamespace,
		"podName", req.PodName,
		"containerName", req.ContainerName,
		"podAggregatedRequest", podAggregatedRequest,
		"numCPUsInt", reqInt,
		"numCPUsFloat64", reqFloat64,
		"result", result.String())

	topologyAwareAssignments, err := machine.GetNumaAwareAssignments(p.machineInfo.CPUTopology, result)
	if err != nil {
		general.ErrorS(err, "unable to calculate topologyAwareAssignments",
			"podNamespace", req.PodNamespace,
			"podName", req.PodName,
			"containerName", req.ContainerName,
			"podAggregatedRequest", podAggregatedRequest,
			"numCPUsInt", reqInt,
			"numCPUsFloat64", reqFloat64,
			"result cpuset", result.String())
		return nil, err
	}
	if qosutil.AnnotationsIndicateNUMAExclusive(req.Annotations) &&
		p.isRampUpReclaimHardPartitionEnabled() &&
		p.state.GetDisableDedicatedCoresOverlapReclaimedCores() {
		for _, numaID := range req.Hint.Nodes {
			numaState := machineState[int(numaID)]
			if numaState == nil {
				return nil, fmt.Errorf("NUMA-exclusive DNB ramp-up missing machine state for NUMA %d", numaID)
			}
			availableInNUMA := numaState.GetAvailableCPUSet(p.reservedCPUs)
			coverageTarget := availableInNUMA
			if eligibility != nil {
				coverageTarget = eligibility.partitionEligiblePerNUMA[int(numaID)]
			}
			floorInNUMA := hardReclaimCPUs.Intersection(coverageTarget)
			allocationInNUMA := result.Intersection(coverageTarget)
			if floorInNUMA.IsEmpty() {
				return nil, fmt.Errorf("NUMA-exclusive DNB ramp-up requires non-empty reclaim floor on NUMA %d", numaID)
			}
			if overlap := allocationInNUMA.Intersection(floorInNUMA); !overlap.IsEmpty() {
				return nil, fmt.Errorf("NUMA-exclusive DNB allocation overlaps reclaim floor on NUMA %d: overlap=%s",
					numaID, overlap.String())
			}
			if covered := allocationInNUMA.Union(floorInNUMA); !covered.Equals(coverageTarget) {
				return nil, fmt.Errorf("NUMA-exclusive DNB allocation and reclaim floor do not cover NUMA %d: allocation=%s floor=%s eligible=%s",
					numaID, allocationInNUMA.String(), floorInNUMA.String(), coverageTarget.String())
			}
		}
	}

	allocationInfo := &state.AllocationInfo{
		AllocationMeta: commonstate.GenerateGenericContainerAllocationMeta(req,
			commonstate.PoolNameDedicated, apiconsts.PodAnnotationQoSLevelDedicatedCores),
		RampUp:                           true,
		AllocationResult:                 result.Clone(),
		OriginalAllocationResult:         result.Clone(),
		TopologyAwareAssignments:         topologyAwareAssignments,
		OriginalTopologyAwareAssignments: machine.DeepcopyCPUAssignment(topologyAwareAssignments),
		InitTimestamp:                    time.Now().Format(util.QRMTimeFormat),
		RequestQuantity:                  reqFloat64,
	}

	numaNumber, err := qosutil.AnnotationsGetNUMANumber(req.Annotations, len(machineState), p.numaNumberAnnotationKey)
	if err != nil {
		return nil, fmt.Errorf("get numa number failed with error: %v", err)
	}
	// Cross NUMA allocation is only possible in the case of exclusive NUMA and non-exclusive NUMA with numa number more than 1
	if !qosutil.AnnotationsIndicateNUMAExclusive(req.Annotations) && numaNumber <= 1 {
		if len(req.Hint.Nodes) != 1 {
			return nil, fmt.Errorf("numa binding without numa exclusive allocation result numa node size is %d, "+
				"not equal to 1", len(req.Hint.Nodes))
		}
		allocationInfo.SetSpecifiedNUMABindingNUMAID(req.Hint.Nodes)
	}

	if len(p.allocationHooks) > 0 {
		if err := p.invokeAllocationHooks(oldAllocationInfo, allocationInfo); err != nil {
			return nil, err
		}
	}
	if basePodEntries[allocationInfo.PodUid] == nil {
		basePodEntries[allocationInfo.PodUid] = make(state.ContainerEntries)
	}
	basePodEntries[allocationInfo.PodUid][allocationInfo.ContainerName] = allocationInfo.Clone()

	updatedMachineState, err := generateMachineStateFromPodEntries(
		p.machineInfo.CPUTopology, basePodEntries, baseMachineState)
	if err != nil {
		general.Errorf("pod: %s/%s, container: %s GenerateMachineStateFromPodEntries failed with error: %v",
			req.PodNamespace, req.PodName, req.ContainerName, err)
		return nil, fmt.Errorf("GenerateMachineStateFromPodEntries failed with error: %v", err)
	}

	planningState := state.NewTransientState(p.machineInfo.CPUTopology)
	if err := planningState.CommitAdvisorState(
		basePodEntries,
		updatedMachineState,
		p.state.GetAllowSharedCoresOverlapReclaimedCores(),
		p.state.GetDisableDedicatedCoresOverlapReclaimedCores(),
		false,
		p.state.GetDefaultShareMaterializationState(),
	); err != nil {
		return nil, fmt.Errorf("initialize DNB ramp-up target state failed: %w", err)
	}
	planningPolicy := p.newRampUpPlanningPolicy(planningState)
	planningRevision := planningState.GetRevision()
	err = planningPolicy.adjustAllocationEntriesWithRampUpFloorAtRevision(
		basePodEntries, updatedMachineState, false, hardReclaimCPUs, false, planningRevision)
	if err != nil {
		general.Errorf("pod: %s/%s, container: %s putContainersAndAdjustAllocationEntriesWithoutAllocation failed with error: %v",
			req.PodNamespace, req.PodName, req.ContainerName, err)
		return nil, fmt.Errorf("adjustallocationentries failed with error: %v", err)
	}
	finalPodEntries := planningState.GetPodEntries()
	finalMachineState := planningState.GetMachineState()
	allocationInfo = finalPodEntries[req.PodUid][req.ContainerName]
	if allocationInfo == nil {
		return nil, fmt.Errorf("DNB allocation missing from planned state: pod=%s container=%s",
			req.PodUid, req.ContainerName)
	}
	reclaimInfo := finalPodEntries[commonstate.PoolNameReclaim][commonstate.FakedContainerName]
	if reclaimInfo == nil || !hardReclaimCPUs.IsSubsetOf(reclaimInfo.AllocationResult) {
		reclaimCPUs := machine.NewCPUSet()
		if reclaimInfo != nil {
			reclaimCPUs = reclaimInfo.AllocationResult
		}
		return nil, fmt.Errorf("planned reclaim %s dropped DNB ramp-up floor %s",
			reclaimCPUs.String(), hardReclaimCPUs.String())
	}
	if overlap := allocationInfo.AllocationResult.Intersection(hardReclaimCPUs); !overlap.IsEmpty() {
		return nil, fmt.Errorf("planned DNB allocation overlaps ramp-up floor: allocation=%s floor=%s overlap=%s",
			allocationInfo.AllocationResult.String(), hardReclaimCPUs.String(), overlap.String())
	}

	resp, err := cpuutil.PackAllocationResponse(allocationInfo, string(v1.ResourceCPU),
		util.OCIPropertyNameCPUSetCPUs, false, true, req, allocationInfo.Annotations)
	if err != nil {
		general.Errorf("pod: %s/%s, container: %s PackResourceAllocationResponseByAllocationInfo failed with error: %v",
			req.PodNamespace, req.PodName, req.ContainerName, err)
		return nil, fmt.Errorf("PackResourceAllocationResponseByAllocationInfo failed with error: %v", err)
	}

	if err := AccompanyResourceRegistry.AllocateAccompanyResource(req, resp); err != nil {
		return nil, fmt.Errorf("accompany resource AugmentAllocationResult failed with error: %v", err)
	}
	if err := p.state.CommitAdvisorState(
		finalPodEntries,
		finalMachineState,
		p.state.GetAllowSharedCoresOverlapReclaimedCores(),
		p.state.GetDisableDedicatedCoresOverlapReclaimedCores(),
		persistCheckpoint,
		planningState.GetDefaultShareMaterializationState(),
	); err != nil {
		return nil, fmt.Errorf("commit DNB allocation and reclaim floor atomically failed: %w", err)
	}
	adjustCtx, cancel := context.WithTimeout(context.Background(), cpuSetAdjustmentHandlerTimeout(p.conf))
	adjustErr := p.runCPUSetAdjustmentHandlers(adjustCtx, dynamicpolicyutil.CPUSetAdjustmentModeAdmission)
	cancel()
	if adjustErr != nil {
		rollbackErr := p.rollbackFailedDNBAllocation(
			req.PodUid, req.ContainerName, oldAllocationInfo, allocationInfo, persistCheckpoint)
		var restoreErr error
		if rollbackErr == nil {
			restoreCtx, restoreCancel := context.WithTimeout(context.Background(), cpuSetAdjustmentHandlerTimeout(p.conf))
			restoreErr = p.runCPUSetAdjustmentHandlers(restoreCtx, dynamicpolicyutil.CPUSetAdjustmentModeRetry)
			restoreCancel()
			if restoreErr != nil {
				p.scheduleCPUSetAdjustmentRetry(dynamicpolicyutil.RetryReasonRestoreFailed)
			}
		}
		var ownershipLost *requestStateOwnershipLostError
		if errors.As(rollbackErr, &ownershipLost) {
			p.scheduleCPUSetAdjustmentRetry(dynamicpolicyutil.RetryReasonOwnershipLost)
			err := fmt.Errorf("apply DNB allocation and reclaim floor failed: %v; state rollback error: %w; machine restore error: %v",
				adjustErr, rollbackErr, restoreErr)
			return nil, &requestStateCompensatedError{err: err}
		}
		err := fmt.Errorf("apply DNB allocation and reclaim floor failed: %v; state rollback error: %v; machine restore error: %v",
			adjustErr, rollbackErr, restoreErr)
		if rollbackErr == nil {
			return nil, &requestStateCompensatedError{err: err}
		}
		return nil, err
	}
	p.clearCPUSetInAllocationResponseIfNeeded(resp, allocationInfo)

	return resp, nil
}

// rollbackFailedDNBAllocation removes only the failed request's committed delta
// from the latest state. It replans pools and the ramp-up reclaim floor from
// that latest view so unrelated allocations committed while adjustment ran are
// preserved and cannot be replaced by a stale whole-state snapshot.
func (p *DynamicPolicy) rollbackFailedDNBAllocation(
	podUID, containerName string,
	previous, failedCandidate *state.AllocationInfo,
	persistCheckpoint bool,
) error {
	latestEntries := p.state.GetPodEntries()
	latestCandidate := latestEntries[podUID][containerName]
	if latestCandidate == nil {
		return nil
	}
	if !reflect.DeepEqual(latestCandidate, failedCandidate) {
		return &requestStateOwnershipLostError{err: fmt.Errorf(
			"allocation %s/%s ownership lost: allocation advanced after failed candidate; skip stale candidate rollback",
			podUID, containerName)}
	}

	if previous != nil {
		latestEntries[podUID][containerName] = previous.Clone()
	} else {
		delete(latestEntries[podUID], containerName)
		if len(latestEntries[podUID]) == 0 {
			delete(latestEntries, podUID)
		}
	}

	latestMachineState, err := generateMachineStateFromPodEntries(
		p.machineInfo.CPUTopology, latestEntries, p.state.GetMachineState())
	if err != nil {
		return fmt.Errorf("recompute machine state without failed DNB allocation: %w", err)
	}
	planningState := state.NewTransientState(p.machineInfo.CPUTopology)
	if err := planningState.CommitAdvisorState(
		latestEntries,
		latestMachineState,
		p.state.GetAllowSharedCoresOverlapReclaimedCores(),
		p.state.GetDisableDedicatedCoresOverlapReclaimedCores(),
		false,
		p.state.GetDefaultShareMaterializationState(),
	); err != nil {
		return fmt.Errorf("initialize failed DNB rollback state: %w", err)
	}
	planningPolicy := p.newRampUpPlanningPolicy(planningState)
	planningRevision := planningState.GetRevision()
	if err := planningPolicy.adjustAllocationEntriesWithRampUpFloorAtRevision(
		latestEntries, latestMachineState, false, machine.NewCPUSet(), false, planningRevision); err != nil {
		return fmt.Errorf("recompute pools and reclaim floor without failed DNB allocation: %w", err)
	}
	return p.state.CommitAdvisorState(
		planningState.GetPodEntries(),
		planningState.GetMachineState(),
		p.state.GetAllowSharedCoresOverlapReclaimedCores(),
		p.state.GetDisableDedicatedCoresOverlapReclaimedCores(),
		persistCheckpoint,
		planningState.GetDefaultShareMaterializationState(),
	)
}

// newRampUpPlanningPolicy creates an isolated policy view for speculative
// pool and machine-state calculation. It intentionally carries no locks or
// runtime handlers, so planning cannot expose transient state or touch cgroups.
func (p *DynamicPolicy) newRampUpPlanningPolicy(planningState state.State) *DynamicPolicy {
	return &DynamicPolicy{
		emitter:                         p.emitter,
		metaServer:                      p.metaServer,
		machineInfo:                     p.machineInfo,
		state:                           planningState,
		enableReclaimNUMABinding:        p.enableReclaimNUMABinding,
		enableSNBHighNumaPreference:     p.enableSNBHighNumaPreference,
		enableCPUAdvisor:                p.enableCPUAdvisor,
		advisorMonitor:                  p.advisorMonitor,
		qosConfig:                       p.qosConfig,
		dynamicConfig:                   p.dynamicConfig,
		conf:                            p.conf,
		numaBindingResultAnnotationKey:  p.numaBindingResultAnnotationKey,
		numaNumberAnnotationKey:         p.numaNumberAnnotationKey,
		numaIDsAnnotationKey:            p.numaIDsAnnotationKey,
		topologyAllocationAnnotationKey: p.topologyAllocationAnnotationKey,
		transitionPeriod:                p.transitionPeriod,
		reservedCPUs:                    p.reservedCPUs.Clone(),
		reservedReclaimedCPUsSize:       p.reservedReclaimedCPUsSize,
		reservedReclaimedCPUSet:         p.reservedReclaimedCPUSet.Clone(),
		reservedReclaimedTopologyAwareAssignments: machine.DeepcopyCPUAssignment(
			p.reservedReclaimedTopologyAwareAssignments),
	}
}

// allocationSidecarHandler currently we set cpuset of sidecar to the cpuset of its main container
func (p *DynamicPolicy) allocationSidecarHandler(_ context.Context,
	req *pluginapi.ResourceRequest, qosLevel string, persistCheckpoint bool,
) (*pluginapi.ResourceAllocationResponse, error) {
	_, reqFloat64, err := util.GetQuantityFromResourceReq(req)
	if err != nil {
		return nil, fmt.Errorf("getReqQuantityFromResourceReq failed with error: %v", err)
	}

	podEntries := p.state.GetPodEntries()
	if podEntries[req.PodUid] == nil {
		general.Infof("there is no pod entry, pod: %s/%s, sidecar: %s, waiting next reconcile",
			req.PodNamespace, req.PodName, req.ContainerName)
		return &pluginapi.ResourceAllocationResponse{}, nil
	}

	mainContainerAllocationInfo := podEntries[req.PodUid].GetMainContainerEntry()

	// todo: consider sidecar without reconcile in vpa
	if mainContainerAllocationInfo == nil {
		general.Infof("main container is not found for pod: %s/%s, sidecar: %s, waiting next reconcile",
			req.PodNamespace, req.PodName, req.ContainerName)
		return &pluginapi.ResourceAllocationResponse{}, nil
	}

	// the sidecar container also support inplace update resize, update the allocation and machine state here
	allocationInfo := &state.AllocationInfo{
		AllocationMeta: commonstate.GenerateGenericContainerAllocationMeta(req,
			commonstate.EmptyOwnerPoolName, qosLevel),
		InitTimestamp:   time.Now().Format(util.QRMTimeFormat),
		RequestQuantity: reqFloat64,
	}
	p.applySidecarAllocationInfoFromMainContainer(allocationInfo, mainContainerAllocationInfo)

	// update pod entries directly.
	// if one of subsequent steps is failed, we will delete current allocationInfo from podEntries in defer function of allocation function.
	if err := p.updateAllocationInfo(allocationInfo.PodUid, allocationInfo.ContainerName, nil, allocationInfo, persistCheckpoint); err != nil {
		return nil, err
	}
	podEntries = p.state.GetPodEntries()

	updatedMachineState, err := generateMachineStateFromPodEntries(p.machineInfo.CPUTopology, podEntries, p.state.GetMachineState())
	if err != nil {
		general.Errorf("pod: %s/%s, container: %s GenerateMachineStateFromPodEntries failed with error: %v",
			req.PodNamespace, req.PodName, req.ContainerName, err)
		return nil, fmt.Errorf("GenerateMachineStateFromPodEntries failed with error: %v", err)
	}
	p.state.SetMachineState(updatedMachineState, persistCheckpoint)

	resp, err := cpuutil.PackAllocationResponse(allocationInfo, string(v1.ResourceCPU), util.OCIPropertyNameCPUSetCPUs, false, true, req, allocationInfo.Annotations)
	if err != nil {
		general.Errorf("pod: %s/%s, container: %s packAllocationResponse failed with error: %v",
			req.PodNamespace, req.PodName, req.ContainerName, err)
		return nil, fmt.Errorf("PackResourceAllocationResponseByAllocationInfo failed with error: %v", err)
	}
	p.clearCPUSetInAllocationResponseIfNeeded(resp, allocationInfo)
	return resp, nil
}

func (p *DynamicPolicy) sharedCoresWithNUMABindingAllocationHandler(ctx context.Context,
	req *pluginapi.ResourceRequest, persistCheckpoint bool,
) (*pluginapi.ResourceAllocationResponse, error) {
	if req.ContainerType == pluginapi.ContainerType_SIDECAR {
		return p.allocationSidecarHandler(ctx, req, apiconsts.PodAnnotationQoSLevelSharedCores, persistCheckpoint)
	}

	// there is no need to delete old allocationInfo for the container if it exists,
	// allocateSharedNumaBindingCPUs will re-calculate pool size and avoid counting same entry twice
	allocationInfo, err := p.allocateSharedNumaBindingCPUs(ctx, req, req.Hint, persistCheckpoint)
	if err != nil || allocationInfo == nil {
		general.ErrorS(err, "unable to allocate CPUs",
			"podNamespace", req.PodNamespace,
			"podName", req.PodName,
			"containerName", req.ContainerName)
		return nil, err
	}

	general.InfoS("allocate CPUs successfully",
		"podNamespace", req.PodNamespace,
		"podName", req.PodName,
		"containerName", req.ContainerName,
		"result", allocationInfo.AllocationResult.String())

	// there is no need to call SetPodEntries and SetMachineState,
	// since they are already done in doAndCheckPutAllocationInfo of allocateSharedNumaBindingCPUs
	resp, err := cpuutil.PackAllocationResponse(allocationInfo,
		string(v1.ResourceCPU), util.OCIPropertyNameCPUSetCPUs, false, true, req, allocationInfo.Annotations)
	if err != nil {
		general.Errorf("pod: %s/%s, container: %s PackResourceAllocationResponseByAllocationInfo failed with error: %v",
			req.PodNamespace, req.PodName, req.ContainerName, err)
		return nil, fmt.Errorf("PackResourceAllocationResponseByAllocationInfo failed with error: %v", err)
	}

	if err := AccompanyResourceRegistry.AllocateAccompanyResource(req, resp); err != nil {
		return nil, fmt.Errorf("accompany resource AugmentAllocationResult failed with error: %v", err)
	}
	p.clearCPUSetInAllocationResponseIfNeeded(resp, allocationInfo)

	return resp, nil
}

type numaBindingPartitionEligibilitySnapshot struct {
	partitionEligiblePerNUMA map[int]machine.CPUSet
}

// allocateNumaBindingCPUs allocates CPUs for NUMA binding containers.
// It considers NUMA affinity, exclusive requirements, and resource package pinning.
// Steps:
//  1. Calculate the initial available CPUSet based on the TopologyHint (NUMA nodes).
//  2. If a Resource Package is specified in annotations:
//     a. If the package has pinned CPUs, restrict the available CPUs to the intersection of NUMA CPUs and Pinned CPUs.
//     b. If the package is not pinned but others are, exclude other packages' pinned CPUs.
//  3. Allocate CPUs from the calculated available set using the topology calculator.
func (p *DynamicPolicy) allocateNumaBindingCPUs(numCPUs int, hint *pluginapi.TopologyHint,
	machineState state.NUMANodeMap, reqAnnotations map[string]string, podReclaimEnabled bool,
) (machine.CPUSet, machine.CPUSet, error) {
	result, hardReclaimCPUs, _, err := p.allocateNumaBindingCPUsWithEligibility(
		numCPUs, hint, machineState, reqAnnotations, podReclaimEnabled)
	return result, hardReclaimCPUs, err
}

func (p *DynamicPolicy) allocateNumaBindingCPUsWithEligibility(numCPUs int, hint *pluginapi.TopologyHint,
	machineState state.NUMANodeMap, reqAnnotations map[string]string, podReclaimEnabled bool,
) (machine.CPUSet, machine.CPUSet, *numaBindingPartitionEligibilitySnapshot, error) {
	distributeEvenlyAcrossNuma := qosutil.AnnotationsIndicateDistributeEvenlyAcrossNuma(reqAnnotations)
	fullPCPUsPairing := qosutil.AnnotationsIndicateFullPCPUsPairing(reqAnnotations)
	numaExclusive := qosutil.AnnotationsIndicateNUMAExclusive(reqAnnotations)
	numaNumber, err := qosutil.AnnotationsGetNUMANumber(reqAnnotations, len(machineState), p.numaNumberAnnotationKey)
	if err != nil {
		return machine.NewCPUSet(), machine.NewCPUSet(), nil, fmt.Errorf("get numa number failed with error: %v", err)
	}

	if hint == nil {
		return machine.NewCPUSet(), machine.NewCPUSet(), nil, fmt.Errorf("hint is nil")
	} else if len(hint.Nodes) == 0 {
		return machine.NewCPUSet(), machine.NewCPUSet(), nil, fmt.Errorf("hint is empty")
	} else if !qosutil.AnnotationsIndicateNUMABinding(reqAnnotations) {
		return machine.NewCPUSet(), machine.NewCPUSet(), nil, fmt.Errorf("request is not NUMA binding, which is unexpected")
	} else if !numaExclusive && numaNumber <= 1 && len(hint.Nodes) > 1 {
		return machine.NewCPUSet(), machine.NewCPUSet(), nil, fmt.Errorf("NUMA not exclusive binding container has request larger than 1 NUMA")
	} else if numaExclusive && fullPCPUsPairing {
		return machine.NewCPUSet(), machine.NewCPUSet(), nil, fmt.Errorf("NUMA exclusive and full pcpus pairing not supported at the same time")
	} else if numaExclusive && distributeEvenlyAcrossNuma {
		return machine.NewCPUSet(), machine.NewCPUSet(), nil, fmt.Errorf("NUMA exclusive and distribute evenly across numa not supported at the same time")
	}

	result := machine.NewCPUSet()
	alignedAvailableCPUs := machine.CPUSet{}
	alignedAvailableCPUsPerNUMA := make(map[uint64]machine.CPUSet)
	hintNodes := hint.Nodes
	pkgName := rputil.GetResourcePackageName(reqAnnotations)
	disableDedicatedOverlap := p.state.GetDisableDedicatedCoresOverlapReclaimedCores()
	coverExclusivePartition := p.isRampUpReclaimHardPartitionEnabled() &&
		numaExclusive && disableDedicatedOverlap
	var eligibility *numaBindingPartitionEligibilitySnapshot
	dedicatedEligiblePerNUMA := make(map[int]machine.CPUSet)
	reclaimEligiblePerNUMA := make(map[int]machine.CPUSet)
	if coverExclusivePartition {
		dedicatedEligiblePerNUMA, reclaimEligiblePerNUMA, err = p.numaBindingPartitionEligibility(machineState, pkgName, hintNodes)
		if err != nil {
			return machine.NewCPUSet(), machine.NewCPUSet(), nil, err
		}
		eligibility = &numaBindingPartitionEligibilitySnapshot{
			partitionEligiblePerNUMA: make(map[int]machine.CPUSet, len(hintNodes)),
		}
		for _, numaID := range hintNodes {
			eligibility.partitionEligiblePerNUMA[int(numaID)] = dedicatedEligiblePerNUMA[int(numaID)].Union(reclaimEligiblePerNUMA[int(numaID)])
		}
	}
	dedicatedEligible := machine.NewCPUSet()
	reclaimEligible := machine.NewCPUSet()

	for _, numaNode := range hintNodes {
		if machineState[int(numaNode)] == nil {
			return machine.NewCPUSet(), machine.NewCPUSet(), nil,
				fmt.Errorf("missing machine state for hinted NUMA %d", numaNode)
		}
		availableCPUs := machineState[int(numaNode)].GetAvailableCPUSet(p.reservedCPUs)
		if coverExclusivePartition {
			availableCPUs = dedicatedEligiblePerNUMA[int(numaNode)]
		} else {
			pinnedCPUSetsInNUMA := make(map[string]machine.CPUSet)
			for resourcePackage, rpState := range machineState[int(numaNode)].ResourcePackageStates {
				if rpState != nil && !rpState.PinnedCPUSet.IsEmpty() {
					pinnedCPUSetsInNUMA[resourcePackage] = rpState.PinnedCPUSet
				}
			}
			if pkgName != "" && !pinnedCPUSetsInNUMA[pkgName].IsEmpty() {
				availableCPUs = availableCPUs.Intersection(pinnedCPUSetsInNUMA[pkgName])
			} else {
				for _, pinnedCPUs := range pinnedCPUSetsInNUMA {
					availableCPUs = availableCPUs.Difference(pinnedCPUs)
				}
			}
		}
		alignedAvailableCPUsPerNUMA[numaNode] = availableCPUs
		alignedAvailableCPUs = alignedAvailableCPUs.Union(availableCPUs)
		if coverExclusivePartition {
			dedicatedEligible = dedicatedEligible.Union(availableCPUs)
			reclaimEligible = reclaimEligible.Union(reclaimEligiblePerNUMA[int(numaNode)])
		}
	}

	// The node-level floor covers every reclaim NUMA and is shared by all
	// ramp-up QoS paths. Dedicated selection only subtracts it from the current
	// hint's available CPUs; CPUs on other NUMAs remain protected for shared
	// ramp-up and the bulkhead reclaim partition.
	hardReclaimCPUs, err := p.deriveRampUpReclaimFloor(machineState, true)
	if err != nil {
		return machine.NewCPUSet(), machine.NewCPUSet(), nil,
			fmt.Errorf("derive node-level ramp-up reclaim floor failed: %w", err)
	}
	hardReclaimCPUs, err = p.selectNumaBindingReclaimPartition(
		hardReclaimCPUs,
		dedicatedEligiblePerNUMA,
		reclaimEligiblePerNUMA,
		hintNodes,
		podReclaimEnabled,
		coverExclusivePartition,
	)
	if err != nil {
		return machine.NewCPUSet(), machine.NewCPUSet(), nil,
			fmt.Errorf("select NUMA binding reclaim partition failed: %w", err)
	}
	if !hardReclaimCPUs.IsEmpty() {
		for numaNode, availableInNUMA := range alignedAvailableCPUsPerNUMA {
			alignedAvailableCPUsPerNUMA[numaNode] = availableInNUMA.Difference(hardReclaimCPUs)
		}
		alignedAvailableCPUs = alignedAvailableCPUs.Difference(hardReclaimCPUs)
		general.InfoS("ramp-up reclaim hard partition applied node-level reclaim floor",
			"hints", hintNodes,
			"hardReclaimCPUs", hardReclaimCPUs.String(),
			"podReclaimEnabled", podReclaimEnabled)
	}

	// Prefer reclaim-free cpus so dedicated_cores avoid landing on cpus currently held by
	// the live reclaim pool: doing so would force a non-atomic reclaim-child shrink in the
	// topology writer and can transiently break the parent-superset invariant
	// (current_disjoint_parent).
	//
	// This only applies to the non-exclusive path. numaExclusive dedicated_cores must own
	// the WHOLE hint NUMA, so subtracting reclaim would break whole-NUMA exclusivity; for
	// exclusive, the reclaim pool is instead vacated from these NUMAs by the topology
	// writer's sibling pre-shrink before the dedicated cgroup grows into them.
	//
	// We only compute the reclaim-free ("preferred") view here; the ordered selection is
	// centralized in takeByTopologyPreferring.
	preferredAvailableCPUs := machine.NewCPUSet()
	var preferredAvailableCPUsPerNUMA map[uint64]machine.CPUSet
	if !numaExclusive {
		reclaimCPUs := machine.NewCPUSet()
		if reclaimInfo := p.state.GetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName); reclaimInfo != nil {
			// GetAllocationInfo already returns a deep copy, so the result can be used directly.
			reclaimCPUs = reclaimInfo.AllocationResult
		}

		if !reclaimCPUs.IsEmpty() {
			preferredAvailableCPUsPerNUMA = make(map[uint64]machine.CPUSet, len(alignedAvailableCPUsPerNUMA))
			for numaNode, cpus := range alignedAvailableCPUsPerNUMA {
				preferredAvailableCPUsPerNUMA[numaNode] = cpus.Difference(reclaimCPUs)
			}
			preferredAvailableCPUs = alignedAvailableCPUs.Difference(reclaimCPUs)
		}
	}

	var alignedCPUs machine.CPUSet

	if numaExclusive {
		// todo: currently we hack dedicated_cores with NUMA binding take up whole NUMA,
		//  and we will modify strategy here if assumption above breaks.
		//
		// When ramp-up reclaim hard partition is enabled, alignedAvailableCPUs has already
		// had the per-NUMA reclaim floor subtracted above, so "whole NUMA" here means
		// "whole NUMA minus the reclaim floor" - the floor is intentionally left for reclaim.
		alignedCPUs = alignedAvailableCPUs.Clone()
	} else {
		var err error

		// Both branches select cpus via takeByTopologyPreferring (directly, or per-NUMA
		// inside allocateEvenlyAcrossNUMAs), which prefers the reclaim-free set first.
		if distributeEvenlyAcrossNuma {
			alignedCPUs, err = p.allocateEvenlyAcrossNUMAs(numCPUs, hintNodes, alignedAvailableCPUsPerNUMA, preferredAvailableCPUsPerNUMA)
			if err != nil {
				return machine.NewCPUSet(), machine.NewCPUSet(), nil, fmt.Errorf("allocateEvenlyAcrossNUMA failed with error: %v", err)
			}
		} else {
			alignedCPUs, err = p.takeByTopologyPreferring(alignedAvailableCPUs, preferredAvailableCPUs, numCPUs)
			if err != nil {
				general.ErrorS(err, "take cpu for NUMA not exclusive binding container failed",
					"hints", hintNodes,
					"alignedAvailableCPUs", alignedAvailableCPUs.String())

				return machine.NewCPUSet(), machine.NewCPUSet(), nil,
					fmt.Errorf("take cpu for NUMA not exclusive binding container failed with err: %v", err)
			}
		}
	}

	general.InfoS("allocate by hints",
		"hints", hintNodes,
		"alignedAvailableCPUs", alignedAvailableCPUs.String(),
		"alignedAllocatedCPUs", alignedCPUs)

	// currently, result equals to alignedCPUs,
	// maybe extend cpus not aligned to meet requirement later
	result = result.Union(alignedCPUs)
	if coverExclusivePartition {
		reclaimInHint := hardReclaimCPUs.Intersection(reclaimEligible)
		partitionEligible := dedicatedEligible.Union(reclaimEligible)
		if result.IsEmpty() {
			return machine.NewCPUSet(), machine.NewCPUSet(), nil,
				fmt.Errorf("exclusive disjoint dedicated result is empty")
		}
		if !result.Intersection(hardReclaimCPUs).IsEmpty() {
			return machine.NewCPUSet(), machine.NewCPUSet(), nil,
				fmt.Errorf("exclusive dedicated result overlaps reclaim partition")
		}
		if !result.Union(reclaimInHint).Equals(partitionEligible) {
			return machine.NewCPUSet(), machine.NewCPUSet(), nil,
				fmt.Errorf("exclusive dedicated and reclaim do not cover eligible partition")
		}
	} else if result.Size() < numCPUs {
		general.Errorf("result cpus: %s in hint NUMA nodes: %d with size: %d can't meet cpus request: %d",
			result.String(), hintNodes, result.Size(), numCPUs)

		return machine.NewCPUSet(), machine.NewCPUSet(), nil, fmt.Errorf("results can't meet cpus request")
	}

	// Invariant: the reclaim floor must never leak into the dedicated_cores result.
	if !hardReclaimCPUs.IsEmpty() && !result.Intersection(hardReclaimCPUs).IsEmpty() {
		return machine.NewCPUSet(), machine.NewCPUSet(), nil,
			fmt.Errorf("ramp-up reclaim hard partition invariant violated: dedicated result %s overlaps reclaim floor %s",
				result.String(), hardReclaimCPUs.String())
	}
	return result, hardReclaimCPUs, eligibility, nil
}

// allocateEvenlyAcrossNUMAs distributes the cpu request evenly across NUMA nodes.
// preferredCPUsPerNUMA, when non-nil, holds each NUMA's preferred (reclaim-free) subset:
// every NUMA takes its per-NUMA share from its preferred set first and only borrows the
// remainder from the full available set when the preferred set is short. A nil/empty
// preferred set for a NUMA degrades to a plain topology-aware take on the full set.
func (p *DynamicPolicy) allocateEvenlyAcrossNUMAs(numCPUs int, hintNodes []uint64,
	availableCPUsPerNUMA, preferredCPUsPerNUMA map[uint64]machine.CPUSet,
) (machine.CPUSet, error) {
	// First check if it is possible to evenly distribute cpus across NUMA nodes
	if numCPUs%len(hintNodes) != 0 {
		return machine.NewCPUSet(), fmt.Errorf("unable to evenly distribute cpus across numa nodes, request: %d, numa nodes: %d",
			numCPUs, len(hintNodes))
	}
	allocated := machine.NewCPUSet()

	cpusReqPerNuma := numCPUs / len(hintNodes)
	for _, numaNode := range hintNodes {
		availableCPUs := availableCPUsPerNUMA[numaNode]

		// Allocate the CPUs in current numa, preferring this NUMA's reclaim-free subset.
		allocatedCPUsInNUMA, err := p.takeByTopologyPreferring(availableCPUs, preferredCPUsPerNUMA[numaNode], cpusReqPerNuma)
		if err != nil {
			return machine.NewCPUSet(), fmt.Errorf("take cpu for distribute_evenly_across_numa container failed with err: %v", err)
		}
		allocated = allocated.Union(allocatedCPUsInNUMA)
	}

	return allocated, nil
}

// takeByTopologyPreferring takes numCPUs from available in a topology-aware way while
// preferring cpus in the preferred subset. It takes from preferred first and borrows the
// remainder from the rest of available only when preferred cannot fully satisfy the
// request. When preferred is empty it degrades to calculator.TakeByTopology over available.
// preferred is expected to be a subset of available.
func (p *DynamicPolicy) takeByTopologyPreferring(
	available, preferred machine.CPUSet, numCPUs int,
) (machine.CPUSet, error) {
	// Keep the preference advisory: reclaim state can be stale or may contain CPUs
	// outside the current resource-package/NUMA-filtered available set.
	preferred = preferred.Intersection(available)
	if preferred.IsEmpty() {
		return calculator.TakeByTopology(p.machineInfo, available, numCPUs, true)
	}

	// Take (at most) the requested count from the preferred set first.
	takenPreferred := preferred
	if preferred.Size() > numCPUs {
		var err error
		takenPreferred, err = calculator.TakeByTopology(p.machineInfo, preferred, numCPUs, true)
		if err != nil {
			return machine.NewCPUSet(), fmt.Errorf("take preferred cpus failed with error: %v", err)
		}
	}

	remainingReq := numCPUs - takenPreferred.Size()
	if remainingReq <= 0 {
		return takenPreferred, nil
	}

	// Borrow the remainder from the non-preferred cpus of the available set.
	remainingAvailable := available.Difference(takenPreferred)
	takenRemaining, err := calculator.TakeByTopology(p.machineInfo, remainingAvailable, remainingReq, true)
	if err != nil {
		return machine.NewCPUSet(), fmt.Errorf("take remaining cpus failed with error: %v", err)
	}

	return takenPreferred.Union(takenRemaining), nil
}

func (p *DynamicPolicy) allocateSharedNumaBindingCPUs(ctx context.Context, req *pluginapi.ResourceRequest,
	hint *pluginapi.TopologyHint, persistCheckpoint bool,
) (*state.AllocationInfo, error) {
	if req == nil {
		return nil, fmt.Errorf("nil req")
	} else if hint == nil {
		return nil, fmt.Errorf("hint is nil")
	} else if len(hint.Nodes) == 0 {
		return nil, fmt.Errorf("hint is empty")
	} else if len(hint.Nodes) > 1 {
		return nil, fmt.Errorf("shared_cores with numa_binding container has request larger than 1 NUMA")
	}

	reqInt, reqFloat64, err := util.GetQuantityFromResourceReq(req)
	if err != nil {
		return nil, fmt.Errorf("getReqQuantityFromResourceReq failed with error: %v", err)
	}

	general.InfoS("allocateSharedNumaBindingCPUs by hints",
		"hints", hint.Nodes,
		"numCPUsInt", reqInt,
		"numCPUsFloat64", reqFloat64)

	allocationInfo := &state.AllocationInfo{
		AllocationMeta: commonstate.GenerateGenericContainerAllocationMeta(req,
			// it will be put to correct pool in doAndCheckPutAllocationInfo
			commonstate.EmptyOwnerPoolName, apiconsts.PodAnnotationQoSLevelSharedCores),
		InitTimestamp:   time.Now().Format(util.QRMTimeFormat),
		RequestQuantity: reqFloat64,
	}
	allocationInfo.SetSpecifiedNUMABindingNUMAID(hint.Nodes)

	if util.PodInplaceUpdateResizing(req) {
		originAllocationInfo := p.state.GetAllocationInfo(allocationInfo.PodUid, allocationInfo.ContainerName)
		if originAllocationInfo == nil {
			general.Errorf("pod: %s/%s, container: %s request to cpu inplace update resize alloation, but no origin allocation info, reject it",
				req.PodNamespace, req.PodName, req.ContainerName)
			return nil, fmt.Errorf("no origion cpu allocation info for inplace update resize")
		}

		if !originAllocationInfo.CheckSharedNUMABinding() {
			general.Errorf("pod: %s/%s, container: %s request to cpu inplace update resize allocation, but origin allocation info is not shared numa binding, reject it",
				req.PodNamespace, req.PodName, req.ContainerName)
			return nil, fmt.Errorf("cannot change from non-snb to snb during inplace update")
		}

		general.Infof("pod: %s/%s, container: %s request to cpu inplace update resize allocation (%.02f->%.02f)",
			req.PodNamespace, req.PodName, req.ContainerName, originAllocationInfo.RequestQuantity, allocationInfo.RequestQuantity)
		if err := p.updateAllocationInfo(allocationInfo.PodUid, allocationInfo.ContainerName, originAllocationInfo, allocationInfo, persistCheckpoint); err != nil {
			return nil, err
		}
		checkedAllocationInfo, err := p.doAndCheckPutAllocationInfoPodResizingAware(originAllocationInfo, allocationInfo, false, true, persistCheckpoint)
		if err != nil {
			general.Errorf("pod: %s/%s, container: %s request to cpu inplace update resize allocation, but doAndCheckPutAllocationInfoPodResizingAware failed: %q",
				req.PodNamespace, req.PodName, req.ContainerName, err)
			p.state.SetAllocationInfo(originAllocationInfo.PodUid, originAllocationInfo.ContainerName, originAllocationInfo, persistCheckpoint)
			return nil, fmt.Errorf("doAndCheckPutAllocationInfo failed with error: %v", err)
		}
		return checkedAllocationInfo, nil
	} else {
		allocationInfo.RampUp = p.shouldSharedCoresRampUp(ctx, req.PodUid)
		if !p.isRampUpReclaimHardPartitionEnabled() {
			if err := p.updateAllocationInfo(allocationInfo.PodUid, allocationInfo.ContainerName, nil, allocationInfo, persistCheckpoint); err != nil {
				return nil, err
			}
		}
		checkedAllocationInfo, err := p.doAndCheckPutAllocationInfo(allocationInfo, true, persistCheckpoint)
		if err != nil {
			return nil, fmt.Errorf("doandcheckputallocationinfo failed with error: %s", strings.ToLower(err.Error()))
		}
		return checkedAllocationInfo, nil
	}
}

// putAllocationsAndAdjustAllocationEntries calculates and generates the latest checkpoint
// - unlike adjustAllocationEntries, it will also consider AllocationInfo
func (p *DynamicPolicy) putAllocationsAndAdjustAllocationEntries(
	allocationInfos []*state.AllocationInfo,
	incrByReq bool,
	persistCheckpoint bool,
) error {
	return p.putAllocationsAndAdjustAllocationEntriesResizeAware(nil, allocationInfos, incrByReq, false, persistCheckpoint)
}

// putAllocationsAndAdjustAllocationEntriesResizeAware adjusts the allocation entries based on the given allocation infos,
// considering resize requests and resource package information.
func (p *DynamicPolicy) putAllocationsAndAdjustAllocationEntriesResizeAware(
	originAllocationInfos,
	allocationInfos []*state.AllocationInfo,
	incrByReq,
	podInplaceUpdateResizing,
	persistCheckpoint bool,
) (err error) {
	start := time.Now()
	podUID, podName, podNamespace, containerName := "", "", "", ""
	rampUp := false
	allocationSize := 0
	qosLevel := ""
	if len(allocationInfos) > 0 && allocationInfos[0] != nil {
		ai := allocationInfos[0]
		podUID, podName, podNamespace, containerName = ai.PodUid, ai.PodName, ai.PodNamespace, ai.ContainerName
		rampUp = ai.RampUp
		allocationSize = ai.AllocationResult.Size()
		qosLevel = ai.QoSLevel
	}
	defer func() {
		general.Infof("cpu allocation: put allocations and adjust entries finished duration=%s err=%v pod=%s/%s uid=%s container=%s qos=%s ramp_up=%t allocation_size=%d allocation_count=%d incr_by_req=%t resizing=%t persist_checkpoint=%t",
			time.Since(start), err, podNamespace, podName, podUID, containerName, qosLevel, rampUp, allocationSize,
			len(allocationInfos), incrByReq, podInplaceUpdateResizing, persistCheckpoint)
	}()

	if len(allocationInfos) == 0 {
		return nil
	}
	if podInplaceUpdateResizing {
		if len(originAllocationInfos) != 1 && len(allocationInfos) != 1 {
			general.Errorf("cannot adjust allocation entries for invalid allocation infos")
			return fmt.Errorf("invalid inplace update resize allocation infos length")
		}
	}

	entries := p.state.GetPodEntries()

	for _, allocationInfo := range allocationInfos {
		if allocationInfo == nil {
			return fmt.Errorf("found nil allocationInfo in input parameter")
		} else if !allocationInfo.CheckShared() {
			return fmt.Errorf("put container with invalid qos level: %s into pool", allocationInfo.QoSLevel)
		} else if entries[allocationInfo.PodUid][allocationInfo.ContainerName] == nil {
			if entries[allocationInfo.PodUid] == nil {
				entries[allocationInfo.PodUid] = make(state.ContainerEntries)
			}
			entries[allocationInfo.PodUid][allocationInfo.ContainerName] = allocationInfo.Clone()
		}

		poolName := allocationInfo.GetSpecifiedPoolName()
		if poolName == commonstate.EmptyOwnerPoolName {
			return fmt.Errorf("allocationInfo points to empty poolName")
		}
	}

	machineState := p.state.GetMachineState()
	numaResourcePackagePinnedCPUSet := machineState.GetNUMAResourcePackagePinnedCPUSet()

	var poolsQuantityMap map[string]map[int]int
	if p.enableCPUAdvisor &&
		!cpuutil.AdvisorDegradation(p.advisorMonitor.GetHealthy(), p.dynamicConfig.GetDynamicConfiguration().EnableReclaim) {
		// if sys advisor is enabled, we believe the pools' ratio that sys advisor indicates
		csetMap, err := entries.GetFilteredPoolsCPUSetMap(state.IsResidentPool, commonstate.IsSystemPool)
		if err != nil {
			return fmt.Errorf("GetFilteredPoolsCPUSetMap failed with error: %v", err)
		}

		poolsQuantityMap = machine.ParseCPUAssignmentQuantityMap(csetMap)
		if podInplaceUpdateResizing {
			// adjust pool resize
			originAllocationInfo := originAllocationInfos[0]
			allocationInfo := allocationInfos[0]

			poolName, targetNumaID, resizeReqFloat64, err := p.calcPoolResizeRequest(originAllocationInfo, allocationInfo, entries)
			if err != nil {
				return fmt.Errorf("calcPoolResizeRequest cannot calc pool resize request: %q", err)
			}
			if _, ok := poolsQuantityMap[poolName]; !ok {
				poolsQuantityMap[poolName] = make(map[int]int)
			}

			// update the pool size
			poolsQuantityMap[poolName][targetNumaID] += int(math.Ceil(resizeReqFloat64))
			// return err will abort the procedure,
			// so there is no need to revert modifications made in parameter poolsQuantityMap
			if len(poolsQuantityMap[poolName]) > 1 {
				return fmt.Errorf("pool %s cross NUMA: %+v", poolName, poolsQuantityMap[poolName])
			}
		} else if incrByReq {
			err := state.CountAllocationInfosToPoolsQuantityMap(numaResourcePackagePinnedCPUSet, allocationInfos, poolsQuantityMap, p.getContainerRequestedCores)
			if err != nil {
				return fmt.Errorf("CountAllocationInfosToPoolsQuantityMap failed with error: %v", err)
			}
		}
	} else {
		// else we do sum(containers req) for each pool to get pools ratio
		var err error
		poolsQuantityMap, err = state.GetSharedQuantityMapFromPodEntries(numaResourcePackagePinnedCPUSet, entries, allocationInfos, p.getContainerRequestedCores)
		if err != nil {
			return fmt.Errorf("GetSharedQuantityMapFromPodEntries failed with error: %v", err)
		}

		if incrByReq || podInplaceUpdateResizing {
			if podInplaceUpdateResizing {
				general.Infof("pod: %s/%s, container: %s request to re-calc pool size for cpu inplace update resize",
					allocationInfos[0].PodNamespace, allocationInfos[0].PodName, allocationInfos[0].ContainerName)
			}
			// if advisor is disabled, qrm can re-calc the pool size exactly. we don't need to adjust the pool size.
			cErr := state.CountAllocationInfosToPoolsQuantityMap(numaResourcePackagePinnedCPUSet, allocationInfos, poolsQuantityMap, p.getContainerRequestedCores)
			if cErr != nil {
				return fmt.Errorf("CountAllocationInfosToPoolsQuantityMap failed with error: %v", cErr)
			}
		}
	}

	isolatedQuantityMap := state.GetIsolatedQuantityMapFromPodEntries(entries, allocationInfos, p.getContainerRequestedCores)
	err = p.adjustPoolsAndIsolatedEntries(poolsQuantityMap, isolatedQuantityMap,
		entries, machineState, persistCheckpoint)
	if err != nil {
		return fmt.Errorf("adjustpoolsandisolatedentries failed with error: %s", strings.ToLower(err.Error()))
	}

	return nil
}

func (p *DynamicPolicy) calcPoolResizeRequest(originAllocation, allocation *state.AllocationInfo, podEntries state.PodEntries) (string, int, float64, error) {
	poolName := allocation.GetPoolName()
	targetNumaID := commonstate.FakedNUMAID

	originPodAggregatedRequest, ok := originAllocation.GetPodAggregatedRequest()
	if !ok {
		containerEntries, ok := podEntries[originAllocation.PodUid]
		if !ok {
			general.Warningf("pod %s/%s container entries not exist", originAllocation.PodNamespace, originAllocation.PodName)
			originPodAggregatedRequest = 0
		} else {
			podAggregatedRequestSum := float64(0)
			for containerName, containerEntry := range containerEntries {
				if containerName == originAllocation.ContainerName {
					podAggregatedRequestSum += originAllocation.RequestQuantity
				} else {
					podAggregatedRequestSum += containerEntry.RequestQuantity
				}
			}
			originPodAggregatedRequest = podAggregatedRequestSum
		}
	}

	podAggregatedRequest, ok := allocation.GetPodAggregatedRequest()
	if !ok {
		containerEntries, ok := podEntries[originAllocation.PodUid]
		if !ok {
			general.Warningf("pod %s/%s container entries not exist", originAllocation.PodNamespace, originAllocation.PodName)
			podAggregatedRequest = 0
		} else {
			podAggregatedRequestSum := float64(0)
			for _, containerEntry := range containerEntries {
				podAggregatedRequestSum += containerEntry.RequestQuantity
			}
			podAggregatedRequest = podAggregatedRequestSum
		}
	}

	poolResizeQuantity := podAggregatedRequest - originPodAggregatedRequest
	if poolResizeQuantity < 0 {
		// We don't need to adjust pool size in inplace update scale in mode, wait advisor to adjust the pool size later.
		general.Infof("pod: %s/%s, container: %s request cpu inplace update scale in (%.02f->%.02f)",
			allocation.PodNamespace, allocation.PodName, allocation.ContainerName, originPodAggregatedRequest, podAggregatedRequest)
		poolResizeQuantity = 0
	} else {
		// We should adjust pool size in inplace update scale out mode with resizeReqFloat64, and then wait advisor to adjust the pool size later.
		general.Infof("pod: %s/%s, container: %s request cpu inplace update scale out (%.02f->%.02f)",
			allocation.PodNamespace, allocation.PodName, allocation.ContainerName, originPodAggregatedRequest, podAggregatedRequest)
	}

	// only support share cores inplace update resize now (include non-binding share cores and share cores with NUMA binding)
	if allocation.CheckSharedNUMABinding() {
		// check snb numa migrate for inplace update resize
		originTargetNumaID, err := state.GetSharedNUMABindingTargetNuma(originAllocation)
		if err != nil {
			return "", 0, 0, fmt.Errorf("failed to get origin target NUMA")
		}
		targetNumaID, err = state.GetSharedNUMABindingTargetNuma(allocation)
		if err != nil {
			return "", 0, 0, fmt.Errorf("failed to get target NUMA")
		}

		// the pod is migrated to a new NUMA if the NUMA changed.
		// the new pool should scale out the whole request size.
		// the old pool would be adjusted by advisor later.
		if originTargetNumaID != targetNumaID {
			poolResizeQuantity = podAggregatedRequest
			general.Infof("pod %s/%s request inplace update resize and it was migrate to a new NUMA (%d->%d), AggregatedPodRequest(%.02f)",
				allocation.PodNamespace, allocation.PodName, originTargetNumaID, targetNumaID, podAggregatedRequest)
		}

		// get snb pool name
		poolName, err = allocation.GetSpecifiedNUMABindingPoolName()
		if err != nil {
			return "", 0, 0, fmt.Errorf("GetSpecifiedNUMABindingPoolName for %s/%s/%s failed with error: %v",
				allocation.PodNamespace, allocation.PodName, allocation.ContainerName, err)
		}
	}

	if poolName == commonstate.EmptyOwnerPoolName {
		return "", 0, 0, fmt.Errorf("get poolName failed for %s/%s/%s",
			allocation.PodNamespace, allocation.PodName, allocation.ContainerName)
	}

	return poolName, targetNumaID, poolResizeQuantity, nil
}

// adjustAllocationEntries calculates and generates the latest checkpoint
// It fetches resource package items and updates the allocation entries accordingly.
func (p *DynamicPolicy) adjustAllocationEntries(
	entries state.PodEntries,
	machineState state.NUMANodeMap,
	persistCheckpoint bool,
) error {
	return p.adjustAllocationEntriesAtRevision(
		entries, machineState, persistCheckpoint, p.state.GetRevision())
}

func (p *DynamicPolicy) adjustAllocationEntriesAtRevision(
	entries state.PodEntries,
	machineState state.NUMANodeMap,
	persistCheckpoint bool,
	expectedRevision uint64,
) error {
	return p.adjustAllocationEntriesWithRampUpFloorAtRevision(
		entries, machineState, persistCheckpoint, machine.NewCPUSet(), true, expectedRevision)
}

func (p *DynamicPolicy) adjustAllocationEntriesWithRampUpFloor(
	entries state.PodEntries,
	machineState state.NUMANodeMap,
	persistCheckpoint bool,
	explicitRampUpFloor machine.CPUSet,
	runCPUSetHandlers bool,
) error {
	return p.adjustAllocationEntriesWithRampUpFloorAtRevision(
		entries, machineState, persistCheckpoint, explicitRampUpFloor,
		runCPUSetHandlers, p.state.GetRevision())
}

func (p *DynamicPolicy) adjustAllocationEntriesWithRampUpFloorAtRevision(
	entries state.PodEntries,
	machineState state.NUMANodeMap,
	persistCheckpoint bool,
	explicitRampUpFloor machine.CPUSet,
	runCPUSetHandlers bool,
	expectedRevision uint64,
) error {
	startTime := time.Now()
	general.Infof("called")
	defer func() {
		general.InfoS("finished", "duration", time.Since(startTime))
	}()

	// since adjustAllocationEntries will cause re-generate pools,
	// if sys advisor is enabled, we believe the pools' ratio that sys advisor indicates,
	// else we do sum(containers req) for each pool to get pools ratio
	var poolsQuantityMap map[string]map[int]int
	dynamicConfig := p.dynamicConfig.GetDynamicConfiguration()
	advisorHealthy := p.enableCPUAdvisor && p.advisorMonitor != nil &&
		!cpuutil.AdvisorDegradation(p.advisorMonitor.GetHealthy(), dynamicConfig.EnableReclaim)
	if advisorHealthy {
		poolsCPUSetMap, err := entries.GetFilteredPoolsCPUSetMap(state.IsResidentPool, commonstate.IsSystemPool)
		if err != nil {
			return fmt.Errorf("GetFilteredPoolsCPUSetMap failed with error: %v", err)
		}
		poolsQuantityMap = machine.ParseCPUAssignmentQuantityMap(poolsCPUSetMap)
	} else {
		var err error
		poolsQuantityMap, err = state.GetSharedQuantityMapFromPodEntries(machineState.GetNUMAResourcePackagePinnedCPUSet(), entries, nil, p.getContainerRequestedCores)
		if err != nil {
			return fmt.Errorf("GetSharedQuantityMapFromPodEntries failed with error: %v", err)
		}
	}
	isolatedQuantityMap := state.GetIsolatedQuantityMapFromPodEntries(entries, nil, p.getContainerRequestedCores)

	err := p.adjustPoolsAndIsolatedEntriesWithRampUpFloorAtRevision(
		poolsQuantityMap, isolatedQuantityMap, entries, machineState, persistCheckpoint,
		explicitRampUpFloor, runCPUSetHandlers, expectedRevision)
	if err != nil {
		return fmt.Errorf("adjustpoolsandisolatedentries failed with error: %w", err)
	}

	return nil
}

// adjustPoolsAndIsolatedEntries works for the following steps
// 1. calculate pools and isolated cpusets according to expectant quantities
// 2. make reclaimed overlap with numa-binding
// 3. apply them to local state
// 4. clean pools
func (p *DynamicPolicy) adjustPoolsAndIsolatedEntries(
	poolsQuantityMap map[string]map[int]int,
	isolatedQuantityMap map[string]map[string]int,
	entries state.PodEntries,
	machineState state.NUMANodeMap,
	persistCheckpoint bool,
) error {
	return p.adjustPoolsAndIsolatedEntriesWithRampUpFloorAtRevision(
		poolsQuantityMap, isolatedQuantityMap, entries, machineState, persistCheckpoint,
		machine.NewCPUSet(), true, p.state.GetRevision())
}

func (p *DynamicPolicy) adjustPoolsAndIsolatedEntriesWithRampUpFloor(
	poolsQuantityMap map[string]map[int]int,
	isolatedQuantityMap map[string]map[string]int,
	entries state.PodEntries,
	machineState state.NUMANodeMap,
	persistCheckpoint bool,
	explicitRampUpFloor machine.CPUSet,
	runCPUSetHandlers bool,
) error {
	return p.adjustPoolsAndIsolatedEntriesWithRampUpFloorAtRevision(
		poolsQuantityMap, isolatedQuantityMap, entries, machineState, persistCheckpoint,
		explicitRampUpFloor, runCPUSetHandlers, p.state.GetRevision())
}

func (p *DynamicPolicy) adjustPoolsAndIsolatedEntriesWithRampUpFloorAtRevision(
	poolsQuantityMap map[string]map[int]int,
	isolatedQuantityMap map[string]map[string]int,
	entries state.PodEntries,
	machineState state.NUMANodeMap,
	persistCheckpoint bool,
	explicitRampUpFloor machine.CPUSet,
	runCPUSetHandlers bool,
	expectedRevision uint64,
) error {
	rampUpReclaimFloor := explicitRampUpFloor.Clone()
	if p.isRampUpReclaimHardPartitionEnabled() && rampUpReclaimFloor.IsEmpty() {
		hasRampUp := false
		for _, containerEntries := range entries {
			if containerEntries.IsPoolEntry() {
				continue
			}
			for _, allocationInfo := range containerEntries {
				if allocationInfo != nil && allocationInfo.RampUp {
					hasRampUp = true
					break
				}
			}
			if hasRampUp {
				break
			}
		}
		if hasRampUp {
			var err error
			rampUpReclaimFloor, err = p.deriveRampUpReclaimFloor(machineState, true)
			if err != nil {
				return fmt.Errorf("derive reclaim floor before allocating pools failed: %w", err)
			}
		}
	}

	availableCPUs := machineState.GetFilteredAvailableCPUSet(p.reservedCPUs, nil,
		state.WrapAllocationMetaFilter((*commonstate.AllocationMeta).CheckDedicatedNUMABindingNUMAExclusive))

	// rpPinnedCPUSet contains the pinned CPU sets for resource packages
	rpPinnedCPUSet := machineState.GetResourcePackagePinnedCPUSet()

	// 扣除不可被用户容器分配的 CPU。这里同时看本轮 entries 和 canonical state，
	// 避免本轮快照缺失 system/forbidden pool 时，default share residual 与 advisor quantity 口径漂移。
	notAllocatablePoolCPUs := p.getNotAllocatablePoolCPUs(entries)
	availableCPUs = availableCPUs.Difference(notAllocatablePoolCPUs)
	hardPartitionWithExplicitFloor := p.isRampUpReclaimHardPartitionEnabled() && !rampUpReclaimFloor.IsEmpty()
	if hardPartitionWithExplicitFloor {
		availableCPUs = availableCPUs.Difference(rampUpReclaimFloor)
	}

	reclaimOverlapShareRatio, err := p.getReclaimOverlapShareRatio(entries)
	if err != nil {
		return fmt.Errorf("reclaimOverlapShareRatio failed with error: %v", err)
	}

	general.Infof("poolsQuantityMap: %#v, isolatedQuantityMap: %#v, rpPinnedCPUSet: %v, availableCPUs: %v, reclaimOverlapShareRatio: %#v",
		poolsQuantityMap, isolatedQuantityMap, rpPinnedCPUSet, availableCPUs, reclaimOverlapShareRatio)

	// When advisor advice has enabled default-share materialization, the default
	// non-NUMA-binding share pool must not participate in the normal pool
	// allocation path. The persisted advisor state, not dynamic config nor the
	// current/request cpuset, is the source of truth for local replans.
	fixedPoolsQuantityMap := copyPoolQuantityMap(poolsQuantityMap)
	defaultSharePlan := defaultShareMaterializationPlanFromState(
		p.state.GetDefaultShareMaterializationState())
	if defaultSharePlan.enabled {
		delete(fixedPoolsQuantityMap, commonstate.PoolNameShare)
	}

	poolsCPUSet, isolatedCPUSet, err := p.groupAndAllocatePools(fixedPoolsQuantityMap, isolatedQuantityMap, availableCPUs, rpPinnedCPUSet, reclaimOverlapShareRatio)
	if err != nil {
		return fmt.Errorf("groupAndAllocatePools failed with error: %v", err)
	}
	if hardPartitionWithExplicitFloor {
		if err := p.validateOwnedPoolsQuantity(
			poolsQuantityMap,
			poolsCPUSet,
			entries,
			machineState.GetNUMAResourcePackagePinnedCPUSet(),
		); err != nil {
			return err
		}
	}

	general.Infof("poolsCPUSet: %v, isolatedCPUSet: %v", poolsCPUSet, isolatedCPUSet)

	err = p.reclaimOverlapNUMABinding(poolsCPUSet, entries)
	if err != nil {
		return fmt.Errorf("reclaimOverlapNUMABinding failed with error: %v", err)
	}

	err = p.applyPoolsAndIsolatedInfo(poolsCPUSet, isolatedCPUSet, entries,
		machineState, state.GetSharedBindingNUMAsFromQuantityMap(poolsQuantityMap), persistCheckpoint, rampUpReclaimFloor,
		defaultSharePlan, expectedRevision)
	if err != nil {
		return fmt.Errorf("applyPoolsAndIsolatedInfo failed with error: %w", err)
	}

	err = p.cleanPools()
	if err != nil {
		return fmt.Errorf("cleanPools failed with error: %v", err)
	}

	if runCPUSetHandlers {
		ctx, cancel := context.WithTimeout(context.Background(), cpuSetAdjustmentHandlerTimeout(p.conf))
		defer cancel()
		if err := p.runCPUSetAdjustmentHandlers(ctx, dynamicpolicyutil.CPUSetAdjustmentModeAdmission); err != nil {
			return fmt.Errorf("runCPUSetAdjustmentHandlers failed with error: %v", err)
		}
	}

	return nil
}

func (p *DynamicPolicy) validateOwnedPoolsQuantity(
	poolsQuantityMap map[string]map[int]int,
	poolsCPUSet map[string]machine.CPUSet,
	entries state.PodEntries,
	numaResourcePackagePinnedCPUSet map[int]map[string]machine.CPUSet,
) error {
	ownedPools := make(map[string]struct{})
	for _, containerEntries := range entries {
		if containerEntries.IsPoolEntry() {
			continue
		}
		for _, allocationInfo := range containerEntries {
			if allocationInfo == nil {
				continue
			}
			if !allocationInfo.CheckSharedNUMABinding() {
				continue
			}
			poolName, _, err := state.GetCanonicalSharedNUMABindingPoolKey(
				numaResourcePackagePinnedCPUSet, allocationInfo)
			if err != nil {
				return fmt.Errorf("get canonical shared numa-binding pool key for %s/%s/%s failed: %s",
					strings.ToLower(allocationInfo.PodNamespace),
					strings.ToLower(allocationInfo.PodName),
					strings.ToLower(allocationInfo.ContainerName),
					strings.ToLower(err.Error()))
			}
			if poolName != commonstate.EmptyOwnerPoolName {
				ownedPools[poolName] = struct{}{}
			}
		}
	}

	for poolName, numaQuantities := range poolsQuantityMap {
		if _, ok := ownedPools[poolName]; !ok {
			continue
		}

		requested := 0
		for _, quantity := range numaQuantities {
			if quantity > 0 {
				requested += quantity
			}
		}
		if requested == 0 {
			continue
		}

		allocated := poolsCPUSet[poolName]
		for numaID, quantity := range numaQuantities {
			if numaID == commonstate.FakedNUMAID || quantity <= 0 {
				continue
			}
			numaAllocated := allocated.Intersection(p.machineInfo.CPUDetails.CPUsInNUMANodes(numaID)).Size()
			if numaAllocated < quantity {
				return fmt.Errorf("insufficient capacity for owned pool %q in numa %d: requested %d cpus, allocated %d",
					strings.ToLower(poolName), numaID, quantity, numaAllocated)
			}
		}
		if allocated.Size() < requested {
			return fmt.Errorf("insufficient capacity for owned pool %q: requested %d cpus, allocated %d",
				strings.ToLower(poolName), requested, allocated.Size())
		}
	}
	return nil
}

func (p *DynamicPolicy) getNotAllocatablePoolCPUs(entries state.PodEntries) machine.CPUSet {
	notAllocatablePoolCPUs := state.GetUnitedPoolsCPUs(entries, state.IsForbiddenPool, commonstate.IsSystemPool)
	if p == nil || p.state == nil {
		return notAllocatablePoolCPUs
	}

	return notAllocatablePoolCPUs.Union(
		state.GetUnitedPoolsCPUs(p.state.GetPodEntries(), state.IsForbiddenPool, commonstate.IsSystemPool),
	)
}

func (p *DynamicPolicy) groupAndAllocatePools(
	poolsQuantityMap map[string]map[int]int,
	isolatedQuantityMap map[string]map[string]int,
	availableCPUs machine.CPUSet,
	rpPinnedCPUSet map[string]machine.CPUSet,
	reclaimOverlapShareRatio map[string]float64,
) (map[string]machine.CPUSet, map[string]map[string]machine.CPUSet, error) {
	// 1. Separate pools into pinned and common
	pinnedPoolsQuantityMap := make(map[string]map[int]int)
	commonPoolsQuantityMap := make(map[string]map[int]int)
	pinnedCPUSets := machine.NewCPUSet()

	// Accumulate all pinned cpusets from resource packages
	for _, cset := range rpPinnedCPUSet {
		pinnedCPUSets = pinnedCPUSets.Union(cset)
	}

	for poolName, quantityMap := range poolsQuantityMap {
		_, pkgName := rputil.UnwrapOwnerPoolName(poolName)
		if pkgName != "" && !rpPinnedCPUSet[pkgName].IsEmpty() {
			pinnedPoolsQuantityMap[poolName] = quantityMap
		} else {
			commonPoolsQuantityMap[poolName] = quantityMap
		}
	}

	// 2. Calculate common available CPUs
	// For pools without pinned cpuset, availableCPUs needs to deduct allocated pinned cpuset
	commonAvailableCPUs := availableCPUs.Difference(pinnedCPUSets)

	// 3. Process Pinned Pools
	poolsCPUSet := make(map[string]machine.CPUSet)

	// Group pinned pools by package to call generatePoolsAndIsolation with correct constraints
	pinnedPoolsByPkg := make(map[string]map[string]map[int]int)
	for poolName, quantityMap := range pinnedPoolsQuantityMap {
		_, pkgName := rputil.UnwrapOwnerPoolName(poolName)
		if pinnedPoolsByPkg[pkgName] == nil {
			pinnedPoolsByPkg[pkgName] = make(map[string]map[int]int)
		}
		pinnedPoolsByPkg[pkgName][poolName] = quantityMap
	}

	for pkgName, poolsMap := range pinnedPoolsByPkg {
		pkgAvailableCPUs := availableCPUs.Intersection(rpPinnedCPUSet[pkgName])

		general.Infof("pkgName: %s, poolsMap: %#v, pkgAvailableCPUs: %v", pkgName, poolsMap, pkgAvailableCPUs)
		// Call generatePoolsAndIsolation for this package
		// Pass nil for isolatedQuantityMap as we assume isolated containers go to common
		pPools, _, err := p.generatePoolsAndIsolation(poolsMap, nil, pkgAvailableCPUs, reclaimOverlapShareRatio)
		if err != nil {
			return nil, nil, fmt.Errorf("generatePoolsAndIsolation for pkg %s failed with error: %v", pkgName, err)
		}
		for k, v := range pPools {
			poolsCPUSet[k] = v
		}
	}

	// 4. Process Common Pools
	// Pass rpPinnedCPUSet to generatePoolsAndIsolation to handle pinned resources
	general.Infof("commonPoolsQuantityMap: %#v, commonAvailableCPUs: %v", commonPoolsQuantityMap, commonAvailableCPUs)
	cPools, cIso, err := p.generatePoolsAndIsolation(commonPoolsQuantityMap, isolatedQuantityMap, commonAvailableCPUs, reclaimOverlapShareRatio)
	if err != nil {
		return nil, nil, fmt.Errorf("generatePoolsAndIsolation failed with error: %v", err)
	}

	for k, v := range cPools {
		poolsCPUSet[k] = v
	}
	isolatedCPUSet := cIso

	return poolsCPUSet, isolatedCPUSet, nil
}

// reclaimOverlapNUMABinding unions calculated reclaim pool in empty NUMAs
// with the intersection of previous reclaim pool and non-ramp-up dedicated_cores numa_binding containers
func (p *DynamicPolicy) reclaimOverlapNUMABinding(poolsCPUSet map[string]machine.CPUSet, entries state.PodEntries) error {
	// reclaimOverlapNUMABinding only works with cpu advisor and reclaim enabled
	if !(p.enableCPUAdvisor && p.dynamicConfig.GetDynamicConfiguration().EnableReclaim) {
		return nil
	}

	if entries.CheckPoolEmpty(commonstate.PoolNameReclaim) {
		return fmt.Errorf("reclaim pool misses in current entries")
	}

	curReclaimCPUSet := entries[commonstate.PoolNameReclaim][commonstate.FakedContainerName].AllocationResult.Clone()
	nonOverlapReclaimCPUSet := poolsCPUSet[commonstate.PoolNameReclaim].Clone()
	general.Infof("curReclaimCPUSet: %s", curReclaimCPUSet.String())

	for _, containerEntries := range entries {
		if containerEntries.IsPoolEntry() {
			continue
		}

		for _, allocationInfo := range containerEntries {
			if !(allocationInfo != nil && allocationInfo.CheckDedicatedNUMABinding() && allocationInfo.CheckMainContainer()) {
				continue
			} else if allocationInfo.RampUp {
				general.Infof("dedicated numa_binding pod: %s/%s container: %s is in ramp up, not to overlap reclaim pool with it",
					allocationInfo.PodNamespace, allocationInfo.PodName, allocationInfo.ContainerName)
				continue
			}

			poolsCPUSet[commonstate.PoolNameReclaim] = poolsCPUSet[commonstate.PoolNameReclaim].Union(curReclaimCPUSet.Intersection(allocationInfo.AllocationResult))
		}
	}

	if poolsCPUSet[commonstate.PoolNameReclaim].IsEmpty() {
		return fmt.Errorf("reclaim pool is empty after overlapping with dedicated_cores numa_binding containers")
	}

	general.Infof("nonOverlapReclaimCPUSet: %s, finalReclaimCPUSet: %s", nonOverlapReclaimCPUSet.String(), poolsCPUSet[commonstate.PoolNameReclaim].String())
	return nil
}

// copyPoolQuantityMap deep-copies the pools quantity map so that the caller's
// map is never mutated (e.g. when we delete the default share pool for the
// residual-backfill path).
func copyPoolQuantityMap(in map[string]map[int]int) map[string]map[int]int {
	out := make(map[string]map[int]int, len(in))
	for poolName, byNUMA := range in {
		copied := make(map[int]int, len(byNUMA))
		for numaID, quantity := range byNUMA {
			copied[numaID] = quantity
		}
		out[poolName] = copied
	}
	return out
}

type defaultShareMaterializationPlan struct {
	enabled         bool
	advisedQuantity int
	eligibleCPUSet  machine.CPUSet
}

func defaultShareMaterializationPlanFromState(
	materializationState state.DefaultShareMaterializationState,
) defaultShareMaterializationPlan {
	if !materializationState.Enabled {
		return defaultShareMaterializationPlan{}
	}
	return defaultShareMaterializationPlan{
		enabled:         true,
		advisedQuantity: materializationState.AdvisedQuantity,
	}
}

func (p defaultShareMaterializationPlan) materializationState(fallback state.DefaultShareMaterializationState) state.DefaultShareMaterializationState {
	if !p.enabled {
		return fallback
	}
	return state.DefaultShareMaterializationState{
		Enabled:         true,
		AdvisedQuantity: p.advisedQuantity,
	}
}

// buildDefaultShareEligibleCPUSet derives the default-share source of truth
// from the complete physical topology and the entries being finalized. It must
// not start from NUMANodeState.DefaultCPUSet: that field can still describe the
// old placement while an exclusive DNB container is migrating between NUMAs.
func (p *DynamicPolicy) buildDefaultShareEligibleCPUSet(
	finalizedEntries state.PodEntries,
	machineState state.NUMANodeMap,
	rampUpReclaimFloor machine.CPUSet,
) machine.CPUSet {
	eligible := p.machineInfo.CPUDetails.CPUs().Difference(p.reservedCPUs)

	exclusiveNUMAs := sets.NewInt()
	for _, containerEntries := range finalizedEntries {
		if containerEntries.IsPoolEntry() {
			continue
		}
		for _, allocationInfo := range containerEntries {
			if allocationInfo == nil || !allocationInfo.CheckDedicatedNUMABindingNUMAExclusive() {
				continue
			}
			for numaID := range allocationInfo.TopologyAwareAssignments {
				exclusiveNUMAs.Insert(numaID)
			}
			if len(allocationInfo.TopologyAwareAssignments) == 0 {
				for _, numaID := range p.machineInfo.CPUDetails.NUMANodes().ToSliceNoSortInt() {
					if !allocationInfo.AllocationResult.Intersection(
						p.machineInfo.CPUDetails.CPUsInNUMANodes(numaID)).IsEmpty() {
						exclusiveNUMAs.Insert(numaID)
					}
				}
			}
		}
	}
	eligible = eligible.Difference(
		p.machineInfo.CPUDetails.CPUsInNUMANodes(exclusiveNUMAs.UnsortedList()...))
	eligible = eligible.Difference(
		state.GetUnitedPoolsCPUs(finalizedEntries, state.IsForbiddenPool, commonstate.IsSystemPool))
	for _, pinned := range machineState.GetResourcePackagePinnedCPUSet() {
		eligible = eligible.Difference(pinned)
	}
	return eligible.Difference(rampUpReclaimFloor)
}

// unionPoolCPUSet unions all pool cpusets except the excluded pool.
func unionPoolCPUSet(pools map[string]machine.CPUSet, excludedPool string) machine.CPUSet {
	result := machine.NewCPUSet()
	for poolName, cpus := range pools {
		if poolName == excludedPool {
			continue
		}
		result = result.Union(cpus)
	}
	return result
}

// unionIsolatedCPUSet unions all isolated container cpusets.
func unionIsolatedCPUSet(isolated map[string]map[string]machine.CPUSet) machine.CPUSet {
	result := machine.NewCPUSet()
	for _, containers := range isolated {
		for _, cpus := range containers {
			result = result.Union(cpus)
		}
	}
	return result
}

func isolatedCPUSetFromPodEntries(entries state.PodEntries) map[string]map[string]machine.CPUSet {
	isolated := make(map[string]map[string]machine.CPUSet)
	for podUID, containers := range entries {
		if containers.IsPoolEntry() {
			continue
		}
		for containerName, allocationInfo := range containers {
			if allocationInfo == nil || allocationInfo.CheckDedicatedNUMABinding() || !allocationInfo.CheckDedicated() {
				continue
			}
			if isolated[podUID] == nil {
				isolated[podUID] = make(map[string]machine.CPUSet)
			}
			isolated[podUID][containerName] = allocationInfo.AllocationResult.Clone()
		}
	}
	return isolated
}

// materializeDefaultShareCPUSet computes the residual cpuset for the default
// share pool by subtracting every fixed pool (except the default share pool
// itself) and every isolated container cpuset from availableCPUs, then fails
// closed when the expected quantity produced by SysAdvisor is smaller than the
// residual size.
//
// Mutual-exclusion premise: this residual backfill relies on advisor-side
// default-share materialization and AllowSharedCoresOverlapReclaimedCores being
// mutually exclusive. That mutual exclusion is enforced on the SysAdvisor side by
// validateDefaultShareBackfillConfig (assembler_common.go) at the quantity
// layer: when the gate is enabled but overlap is allowed it errors out and no
// default share quantity is produced. Therefore we deliberately do NOT
// re-validate the overlap flag here. Even if both flags were mis-enabled
// together, the residual size could exceed the expected quantity and the check
// below makes QRM fail closed (reject the allocation) rather than silently
// produce a wrong cpuset. A larger expected quantity is accepted because QRM may
// observe a newly allocated fixed pool before SysAdvisor shrinks the default
// share quantity; in that case the residual cpuset is the fresher CPUSet-level
// source of truth.
func materializeDefaultShareCPUSet(expectedQuantity int, availableCPUs machine.CPUSet,
	pools map[string]machine.CPUSet, isolated map[string]map[string]machine.CPUSet,
) (machine.CPUSet, error) {
	fixed := unionPoolCPUSet(pools, commonstate.PoolNameShare).Union(unionIsolatedCPUSet(isolated))
	residual := availableCPUs.Difference(fixed)
	if residual.Size() > expectedQuantity {
		return machine.NewCPUSet(), fmt.Errorf(
			"default share quantity %d is smaller than residual cpuset size %d, available: %s, fixed: %s, residual: %s",
			expectedQuantity, residual.Size(), availableCPUs.String(), fixed.String(), residual.String())
	}
	if residual.Size() < expectedQuantity {
		general.InfoS("default share residual cpuset shrank before advisor quantity",
			"expectedQuantity", expectedQuantity,
			"actualSize", residual.Size(),
			"availableCPUs", availableCPUs.String(),
			"fixedCPUs", fixed.String(),
			"residualCPUs", residual.String(),
		)
	}
	general.InfoS("default share residual cpuset allocated",
		"expectedQuantity", expectedQuantity,
		"actualSize", residual.Size(),
	)
	return residual, nil
}

// finalizeDefaultShareEntry re-derives the default share pool cpuset from the
// post-overlap / post-revise pool entries and installs it into newPodEntries.
// It reads each non-share pool's AllocationResult directly from newPodEntries
// so that any adjustment made by reclaimOverlapNUMABinding / reviseReclaimPool /
// explicitRampUpFloor is naturally reflected in the residual. If QRM observes a
// newly allocated fixed pool before SysAdvisor shrinks the default share
// quantity, the computed residual still replaces the old default share entry.
func (p *DynamicPolicy) finalizeDefaultShareEntry(
	newPodEntries state.PodEntries,
	expectedQuantity int, candidate machine.CPUSet,
) error {
	finalPools := make(map[string]machine.CPUSet)
	for poolName, entries := range newPodEntries {
		if !entries.IsPoolEntry() || poolName == commonstate.PoolNameShare {
			continue
		}
		finalPools[poolName] = entries[commonstate.FakedContainerName].AllocationResult
	}
	share, err := materializeDefaultShareCPUSet(
		expectedQuantity, candidate, finalPools, isolatedCPUSetFromPodEntries(newPodEntries))
	if err != nil {
		return err
	}
	topologyAwareAssignments, err := machine.GetNumaAwareAssignments(p.machineInfo.CPUTopology, share)
	if err != nil {
		return fmt.Errorf("calculate default share topology assignments: %w", err)
	}
	newPodEntries[commonstate.PoolNameShare] = state.ContainerEntries{
		commonstate.FakedContainerName: {
			AllocationMeta:                   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameShare),
			AllocationResult:                 share.Clone(),
			OriginalAllocationResult:         share.Clone(),
			TopologyAwareAssignments:         topologyAwareAssignments,
			OriginalTopologyAwareAssignments: machine.DeepcopyCPUAssignment(topologyAwareAssignments),
		},
	}
	return nil
}

// applyPoolsAndIsolatedInfo generates the latest checkpoint by pools and isolated cpusets calculation results.
// 1. construct entries for isolated containers (probably be dedicated_cores not numa_binding )
// 2. construct entries for all pools
// 3. construct entries for shared_cores, reclaimed_cores, numa_binding dedicated_cores containers
func (p *DynamicPolicy) applyPoolsAndIsolatedInfo(poolsCPUSet map[string]machine.CPUSet,
	isolatedCPUSet map[string]map[string]machine.CPUSet, curEntries state.PodEntries,
	machineState state.NUMANodeMap, sharedBindingNUMAs sets.Int, persistCheckpoint bool,
	explicitRampUpFloor machine.CPUSet,
	defaultSharePlan defaultShareMaterializationPlan,
	stateRevision uint64,
) error {
	allowSharedCoresOverlapReclaimedCores := p.state.GetAllowSharedCoresOverlapReclaimedCores()
	disableDedicatedCoresOverlapReclaimedCores := p.state.GetDisableDedicatedCoresOverlapReclaimedCores()
	newPodEntries := make(state.PodEntries)
	unionDedicatedIsolatedCPUSet := machine.NewCPUSet()

	// calculate NUMAs without actual numa_binding reclaimed pods
	nonReclaimActualBindingNUMAs := p.state.GetMachineState().GetFilteredNUMASet(state.WrapAllocationMetaFilter((*commonstate.AllocationMeta).CheckReclaimedActualNUMABinding))
	// 1. construct entries for isolated containers (probably be dedicated_cores not numa_binding )
	for podUID, containerEntries := range isolatedCPUSet {
		for containerName, isolatedCPUs := range containerEntries {
			allocationInfo := curEntries[podUID][containerName]
			if allocationInfo == nil {
				general.Errorf("isolated pod: %s, container: %s without entry in current checkpoint", podUID, containerName)
				continue
			} else if !allocationInfo.CheckDedicated() || allocationInfo.CheckNUMABinding() {
				general.Errorf("isolated pod: %s, container: %s isn't dedicated_cores without NUMA binding", podUID, containerName)
				continue
			}

			topologyAwareAssignments, err := machine.GetNumaAwareAssignments(p.machineInfo.CPUTopology, isolatedCPUs)
			if err != nil {
				general.ErrorS(err, "Unable to calculate topologyAwareAssignments",
					"podNamespace", allocationInfo.PodNamespace,
					"podName", allocationInfo.PodName,
					"containerName", allocationInfo.ContainerName,
					"result cpuset", isolatedCPUs.String())
				continue
			}

			general.InfoS("isolate info",
				"podNamespace", allocationInfo.PodNamespace,
				"podName", allocationInfo.PodName,
				"containerName", allocationInfo.ContainerName,
				"result cpuset", isolatedCPUs.String(),
				"result cpuset size", isolatedCPUs.Size(),
				"qosLevel", allocationInfo.QoSLevel)

			if newPodEntries[podUID] == nil {
				newPodEntries[podUID] = make(state.ContainerEntries)
			}

			newPodEntries[podUID][containerName] = allocationInfo.Clone()
			newPodEntries[podUID][containerName].OwnerPoolName = commonstate.PoolNameDedicated
			newPodEntries[podUID][containerName].AllocationResult = isolatedCPUs.Clone()
			newPodEntries[podUID][containerName].OriginalAllocationResult = isolatedCPUs.Clone()
			newPodEntries[podUID][containerName].TopologyAwareAssignments = topologyAwareAssignments
			newPodEntries[podUID][containerName].OriginalTopologyAwareAssignments = machine.DeepcopyCPUAssignment(topologyAwareAssignments)

			unionDedicatedIsolatedCPUSet = unionDedicatedIsolatedCPUSet.Union(isolatedCPUs)
		}
	}

	// 2. construct entries for all pools
	rampUpReclaimFloor := explicitRampUpFloor.Clone()
	if !rampUpReclaimFloor.IsEmpty() {
		poolsCPUSet[commonstate.PoolNameReclaim] = poolsCPUSet[commonstate.PoolNameReclaim].Union(rampUpReclaimFloor)
	}
	if poolsCPUSet[commonstate.PoolNameReclaim].IsEmpty() {
		return fmt.Errorf("entry: %s is empty", commonstate.PoolNameReclaim)
	}

	for poolName, cset := range poolsCPUSet {
		general.Infof("try to apply pool %s: %s", poolName, cset.String())
		topologyAwareAssignments, err := machine.GetNumaAwareAssignments(p.machineInfo.CPUTopology, cset)
		if err != nil {
			return fmt.Errorf("unable to calculate topologyAwareAssignments for pool: %s, result cpuset: %s, error: %v",
				poolName, cset.String(), err)
		}

		allocationInfo := curEntries[poolName][commonstate.FakedContainerName]
		if allocationInfo != nil {
			general.Infof("pool: %s allocation result transform from %s(size: %d) to %s(size: %d)",
				poolName, allocationInfo.AllocationResult.String(), allocationInfo.AllocationResult.Size(),
				cset.String(), cset.Size())
		}

		if newPodEntries[poolName] == nil {
			newPodEntries[poolName] = make(state.ContainerEntries)
		}
		newPodEntries[poolName][commonstate.FakedContainerName] = &state.AllocationInfo{
			AllocationMeta:                   commonstate.GenerateGenericPoolAllocationMeta(poolName),
			AllocationResult:                 cset.Clone(),
			OriginalAllocationResult:         cset.Clone(),
			TopologyAwareAssignments:         topologyAwareAssignments,
			OriginalTopologyAwareAssignments: machine.DeepcopyCPUAssignment(topologyAwareAssignments),
		}

		for numaID, cpus := range topologyAwareAssignments {
			_ = p.emitter.StoreInt64(util.MetricNamePoolSize, int64(cpus.Size()),
				metrics.MetricTypeNameRaw, metrics.MetricTag{Key: "poolName", Val: poolName},
				metrics.MetricTag{Key: "pool_type", Val: commonstate.GetPoolType(poolName)},
				metrics.MetricTag{Key: "numa_id", Val: strconv.Itoa(numaID)})
		}
	}

	// revise reclaim pool size to avoid reclaimed_cores and numa_binding containers
	// in NUMAs without cpuset actual binding
	err := p.reviseReclaimPool(
		newPodEntries,
		nonReclaimActualBindingNUMAs,
		unionDedicatedIsolatedCPUSet,
		allowSharedCoresOverlapReclaimedCores,
	)
	if err != nil {
		return err
	}

	// backfill the default non-NUMA-binding share pool with residual CPUs
	// after every fixed pool has been settled (post reclaimOverlapNUMABinding,
	// post reviseReclaimPool, post explicitRampUpFloor merge). This must happen
	// before rampUpCPUs computation and before any SetPodEntries/StoreState so
	// that a quantity mismatch fails closed without persisting a partial
	// checkpoint.
	if defaultSharePlan.enabled {
		eligibilityEntries := make(state.PodEntries, len(newPodEntries)+len(curEntries))
		for podUID, containerEntries := range newPodEntries {
			eligibilityEntries[podUID] = containerEntries
		}
		for podUID, containerEntries := range curEntries {
			if !containerEntries.IsPoolEntry() {
				eligibilityEntries[podUID] = containerEntries
			}
		}
		defaultSharePlan.eligibleCPUSet = p.buildDefaultShareEligibleCPUSet(
			eligibilityEntries, machineState, rampUpReclaimFloor)
		if err := p.finalizeDefaultShareEntry(
			newPodEntries, defaultSharePlan.advisedQuantity, defaultSharePlan.eligibleCPUSet,
		); err != nil {
			return err
		}
	}

	sharedBindingNUMACPUs := p.machineInfo.CPUDetails.CPUsInNUMANodes(sharedBindingNUMAs.UnsortedList()...)
	notAllocatablePoolsCPUs := state.GetUnitedPoolsCPUs(newPodEntries, state.IsForbiddenPool, commonstate.IsSystemPool)
	// rampUpCPUs include reclaim pool in NUMAs without NUMA_binding cpus
	rampUpCPUs := machineState.GetFilteredAvailableCPUSet(p.reservedCPUs,
		nil,
		state.WrapAllocationMetaFilter((*commonstate.AllocationMeta).CheckDedicatedNUMABindingNUMAExclusive)).
		Difference(unionDedicatedIsolatedCPUSet).
		Difference(sharedBindingNUMACPUs).
		Difference(notAllocatablePoolsCPUs)
	rampUpCPUs = rampUpCPUs.Difference(rampUpReclaimFloor)

	rampUpCPUsTopologyAwareAssignments, err := machine.GetNumaAwareAssignments(p.machineInfo.CPUTopology, rampUpCPUs)
	if err != nil {
		return fmt.Errorf("unable to calculate topologyAwareAssignments for rampUpCPUs, result cpuset: %s, error: %v",
			rampUpCPUs.String(), err)
	}

	// 3. construct entries for shared_cores, reclaimed_cores, numa_binding dedicated_cores containers
	for podUID, containerEntries := range curEntries {
		if containerEntries.IsPoolEntry() {
			continue
		}

	containerLoop:
		for containerName, allocationInfo := range containerEntries {
			if allocationInfo == nil {
				general.Errorf("pod: %s, container: %s has nil allocationInfo", podUID, containerName)
				continue
			}

			if newPodEntries[podUID][containerName] != nil {
				general.Infof("pod: %s/%s, container: %s, qosLevel: %s is isolated, ignore original allocationInfo",
					allocationInfo.PodNamespace, allocationInfo.PodName, allocationInfo.ContainerName, allocationInfo.QoSLevel)
				continue
			}

			if newPodEntries[podUID] == nil {
				newPodEntries[podUID] = make(state.ContainerEntries)
			}

			newPodEntries[podUID][containerName] = allocationInfo.Clone()
			// adapt to old checkpoint without RequestQuantity property
			newPodEntries[podUID][containerName].RequestQuantity = p.getContainerRequestedCores(allocationInfo)
			switch allocationInfo.QoSLevel {
			case apiconsts.PodAnnotationQoSLevelDedicatedCores:
				newPodEntries[podUID][containerName].OwnerPoolName = allocationInfo.GetPoolName()

				// for numa_binding containers, we just clone checkpoint already exist
				if allocationInfo.CheckDedicatedNUMABinding() {
					continue containerLoop
				}

				// dedicated_cores without numa_binding is not isolated, we will try to isolate it in next adjustment.
				general.Warningf("pod: %s/%s, container: %s is dedicated_cores without numa_binding but not isolated, "+
					"we put it into fallback pool: %s temporary",
					allocationInfo.PodNamespace, allocationInfo.PodName, allocationInfo.ContainerName, rampUpCPUs.String())

				newPodEntries[podUID][containerName].OwnerPoolName = commonstate.PoolNameFallback
				newPodEntries[podUID][containerName].AllocationResult = rampUpCPUs.Clone()
				newPodEntries[podUID][containerName].OriginalAllocationResult = rampUpCPUs.Clone()
				newPodEntries[podUID][containerName].TopologyAwareAssignments = machine.DeepcopyCPUAssignment(rampUpCPUsTopologyAwareAssignments)
				newPodEntries[podUID][containerName].OriginalTopologyAwareAssignments = machine.DeepcopyCPUAssignment(rampUpCPUsTopologyAwareAssignments)

			case apiconsts.PodAnnotationQoSLevelSystemCores:
				poolCPUSet, topologyAwareAssignments, err := p.getSystemPoolCPUSetAndNumaAwareAssignments(newPodEntries, allocationInfo)
				if err != nil {
					return fmt.Errorf("pod: %s/%s, container: %s is system_cores, "+
						"getSystemPoolCPUSetAndNumaAwareAssignments failed with error: %v",
						allocationInfo.PodNamespace, allocationInfo.PodName,
						allocationInfo.ContainerName, err)
				}

				newPodEntries[podUID][containerName].AllocationResult = poolCPUSet
				newPodEntries[podUID][containerName].OriginalAllocationResult = poolCPUSet.Clone()
				newPodEntries[podUID][containerName].TopologyAwareAssignments = topologyAwareAssignments
				newPodEntries[podUID][containerName].OriginalTopologyAwareAssignments = machine.DeepcopyCPUAssignment(topologyAwareAssignments)

			case apiconsts.PodAnnotationQoSLevelSharedCores:
				var ownerPoolName string
				if allocationInfo.CheckSharedNUMABinding() {
					ownerPoolName = allocationInfo.GetOwnerPoolName()

					if ownerPoolName == commonstate.EmptyOwnerPoolName {
						var err error
						// why do we integrate GetOwnerPoolName + GetSpecifiedNUMABindingPoolName into GetPoolName for SharedNUMABinding containers?
						// it's because we reply on GetSpecifiedPoolName (in GetPoolName) when calling CheckNUMABindingAntiAffinity,
						// At that time, NUMA hint for the candidate container isn't confirmed, so we can't implement NUMA hint aware logic in GetSpecifiedPoolName.
						ownerPoolName, err = allocationInfo.GetSpecifiedNUMABindingPoolName()
						if err != nil {
							return fmt.Errorf("pod: %s/%s, container: %s is shared_cores with numa_binding, "+
								"GetSpecifiedNUMABindingPoolName failed with error: %v",
								allocationInfo.PodNamespace, allocationInfo.PodName,
								allocationInfo.ContainerName, err)
						}

						pkgName := allocationInfo.GetResourcePackageName()
						if pkgName != "" {
							numaSet, err := machine.Parse(allocationInfo.Annotations[cpuconsts.CPUStateAnnotationKeyNUMAHint])
							if err != nil {
								return fmt.Errorf("parse numaHintStr: %s failed with error: %v",
									allocationInfo.Annotations[cpuconsts.CPUStateAnnotationKeyNUMAHint], err)
							}

							if numaSet.Size() == 1 {
								targetNUMAID := numaSet.ToSliceNoSortInt()[0]
								if pinnedSets, ok := machineState.GetNUMAResourcePackagePinnedCPUSet()[targetNUMAID]; ok {
									if cpuSet, exists := pinnedSets[pkgName]; exists && cpuSet.Size() > 0 {
										ownerPoolName = rputil.WrapOwnerPoolName(ownerPoolName, pkgName)
									}
								}
							}
						}
					} // else already in a numa_binding share pool or isolated
				} else {
					ownerPoolName = allocationInfo.GetPoolName()
				}

				if allocationInfo.RampUp {
					if allocationInfo.CheckSharedNUMABinding() {
						snbRampUpCPUs, snbRampUpCPUsTopologyAwareAssignments, err := p.getSharedNUMABindingRampUpCPUSet(
							allocationInfo, machineState, unionDedicatedIsolatedCPUSet, notAllocatablePoolsCPUs, rampUpReclaimFloor)
						if err != nil {
							return err
						}
						general.Infof("pod: %s/%s container: %s is in SNB ramp up, set its allocation result from %s to SNB rampUpCPUs: %s",
							allocationInfo.PodNamespace, allocationInfo.PodName, allocationInfo.ContainerName,
							allocationInfo.AllocationResult.String(), snbRampUpCPUs.String())

						newPodEntries[podUID][containerName].OwnerPoolName = commonstate.EmptyOwnerPoolName
						newPodEntries[podUID][containerName].AllocationResult = snbRampUpCPUs.Clone()
						newPodEntries[podUID][containerName].OriginalAllocationResult = snbRampUpCPUs.Clone()
						newPodEntries[podUID][containerName].TopologyAwareAssignments = machine.DeepcopyCPUAssignment(snbRampUpCPUsTopologyAwareAssignments)
						newPodEntries[podUID][containerName].OriginalTopologyAwareAssignments = machine.DeepcopyCPUAssignment(snbRampUpCPUsTopologyAwareAssignments)
					} else {
						general.Infof("pod: %s/%s container: %s is in ramp up, set its allocation result from %s to rampUpCPUs: %s",
							allocationInfo.PodNamespace, allocationInfo.PodName, allocationInfo.ContainerName,
							allocationInfo.AllocationResult.String(), rampUpCPUs.String())

						newPodEntries[podUID][containerName].OwnerPoolName = commonstate.EmptyOwnerPoolName
						newPodEntries[podUID][containerName].AllocationResult = rampUpCPUs.Clone()
						newPodEntries[podUID][containerName].OriginalAllocationResult = rampUpCPUs.Clone()
						newPodEntries[podUID][containerName].TopologyAwareAssignments = machine.DeepcopyCPUAssignment(rampUpCPUsTopologyAwareAssignments)
						newPodEntries[podUID][containerName].OriginalTopologyAwareAssignments = machine.DeepcopyCPUAssignment(rampUpCPUsTopologyAwareAssignments)
					}
				} else {
					poolEntry, err := p.getAllocationPoolEntry(allocationInfo, ownerPoolName, newPodEntries)
					if err != nil {
						return err
					}

					general.Infof("put pod: %s/%s container: %s to pool: %s, set its allocation result from %s to %s",
						allocationInfo.PodNamespace, allocationInfo.PodName, allocationInfo.ContainerName,
						ownerPoolName, allocationInfo.AllocationResult.String(), poolEntry.AllocationResult.String())

					if allocationInfo.CheckSharedNUMABinding() {
						poolEntry.QoSLevel = apiconsts.PodAnnotationQoSLevelSharedCores
						// set SharedNUMABinding declarations to pool entry containing SharedNUMABinding containers,
						// in order to differentiate them from non-binding share cores pools during GetFilteredPoolsCPUSetMap.
						poolEntry.Annotations = general.MergeMap(poolEntry.Annotations, map[string]string{
							apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
						})
					}

					newPodEntries[podUID][containerName].OwnerPoolName = ownerPoolName
					newPodEntries[podUID][containerName].AllocationResult = poolEntry.AllocationResult.Clone()
					newPodEntries[podUID][containerName].OriginalAllocationResult = poolEntry.OriginalAllocationResult.Clone()
					newPodEntries[podUID][containerName].TopologyAwareAssignments = machine.DeepcopyCPUAssignment(poolEntry.TopologyAwareAssignments)
					newPodEntries[podUID][containerName].OriginalTopologyAwareAssignments = machine.DeepcopyCPUAssignment(poolEntry.TopologyAwareAssignments)
				}
			case apiconsts.PodAnnotationQoSLevelReclaimedCores:
				poolEntry, err := p.getAllocationPoolEntry(allocationInfo, allocationInfo.OwnerPoolName, newPodEntries)
				if err != nil {
					return err
				}

				err = p.updateReclaimAllocationResultByPoolEntry(newPodEntries[podUID][containerName], poolEntry, nonReclaimActualBindingNUMAs)
				if err != nil {
					return err
				}
			default:
				return fmt.Errorf("invalid qosLevel: %s for pod: %s/%s container: %s",
					allocationInfo.QoSLevel, allocationInfo.PodNamespace,
					allocationInfo.PodName, allocationInfo.ContainerName)
			}
		}
	}

	// trigger allocation hooks for non-pool containers before committing to state.
	if err := p.invokeAllocationHooksForPodEntries(curEntries, newPodEntries); err != nil {
		return err
	}

	// use pod entries generated above to generate machine state info, and store in local state
	machineState, err = generateMachineStateFromPodEntries(p.machineInfo.CPUTopology, newPodEntries, machineState)
	if err != nil {
		return fmt.Errorf("calculate machineState by newPodEntries failed with error: %v", err)
	}
	return p.state.CommitAdvisorStateIfRevision(
		stateRevision,
		newPodEntries,
		machineState,
		allowSharedCoresOverlapReclaimedCores,
		disableDedicatedCoresOverlapReclaimedCores,
		persistCheckpoint,
		defaultSharePlan.materializationState(p.state.GetDefaultShareMaterializationState()),
	)
}

func (p *DynamicPolicy) getSharedNUMABindingRampUpCPUSet(
	allocationInfo *state.AllocationInfo,
	machineState state.NUMANodeMap,
	unionDedicatedIsolatedCPUSet machine.CPUSet,
	notAllocatablePoolsCPUs machine.CPUSet,
	rampUpReclaimFloor machine.CPUSet,
) (machine.CPUSet, map[int]machine.CPUSet, error) {
	if allocationInfo == nil {
		return machine.NewCPUSet(), nil, fmt.Errorf("nil allocationInfo")
	}
	numaHintStr := allocationInfo.Annotations[cpuconsts.CPUStateAnnotationKeyNUMAHint]
	numaSet, err := machine.Parse(numaHintStr)
	if err != nil {
		return machine.NewCPUSet(), nil, fmt.Errorf("parse SNB numa hint %q failed: %w", numaHintStr, err)
	}
	if numaSet.Size() != 1 {
		return machine.NewCPUSet(), nil, fmt.Errorf("SNB ramp-up requires exactly one NUMA hint, got %s", numaSet.String())
	}

	numaID := numaSet.ToSliceNoSortInt()[0]
	numaState := machineState[numaID]
	if numaState == nil {
		return machine.NewCPUSet(), nil, fmt.Errorf("SNB ramp-up missing machine state for NUMA %d", numaID)
	}

	snbRampUpCPUs := numaState.GetAvailableCPUSet(p.reservedCPUs).
		Difference(unionDedicatedIsolatedCPUSet).
		Difference(notAllocatablePoolsCPUs).
		Difference(rampUpReclaimFloor)
	if snbRampUpCPUs.IsEmpty() {
		return machine.NewCPUSet(), nil, fmt.Errorf("SNB ramp-up CPUs are empty for NUMA %d", numaID)
	}

	assignments, err := machine.GetNumaAwareAssignments(p.machineInfo.CPUTopology, snbRampUpCPUs)
	if err != nil {
		return machine.NewCPUSet(), nil, fmt.Errorf("calculate SNB ramp-up assignments for NUMA %d CPUs %s failed: %w",
			numaID, snbRampUpCPUs.String(), err)
	}
	if len(assignments) != 1 {
		return machine.NewCPUSet(), nil, fmt.Errorf("SNB ramp-up assignments crossed NUMA: %+v", assignments)
	}
	if _, ok := assignments[numaID]; !ok {
		return machine.NewCPUSet(), nil, fmt.Errorf("SNB ramp-up assignments %+v do not match hinted NUMA %d", assignments, numaID)
	}

	return snbRampUpCPUs, assignments, nil
}

func (p *DynamicPolicy) generateNUMABindingPoolsCPUSetInPlace(poolsCPUSet map[string]machine.CPUSet,
	poolsQuantityMap map[string]map[int]int, availableCPUs machine.CPUSet,
) (machine.CPUSet, error) {
	numaToPoolQuantityMap := make(map[int]map[string]int)
	originalAvailableCPUSet := availableCPUs.Clone()
	enableReclaim := p.dynamicConfig.GetDynamicConfiguration().EnableReclaim

	for poolName, numaToQuantity := range poolsQuantityMap {
		for numaID, quantity := range numaToQuantity {
			if numaID == commonstate.FakedNUMAID {
				// only deal with numa_binding pools
				continue
			}

			if numaToPoolQuantityMap[numaID] == nil {
				numaToPoolQuantityMap[numaID] = make(map[string]int)
			}

			numaToPoolQuantityMap[numaID][poolName] = quantity
		}
	}

	for numaID, numaPoolsToQuantityMap := range numaToPoolQuantityMap {
		numaPoolsTotalQuantity := general.SumUpMapValues(numaPoolsToQuantityMap)
		numaCPUs := p.machineInfo.CPUDetails.CPUsInNUMANodes(numaID).Difference(p.reservedCPUs)
		numaAvailableCPUs := numaCPUs.Intersection(availableCPUs)
		availableSize := numaAvailableCPUs.Size()

		general.Infof("numaID: %d, numaPoolsTotalQuantity: %d, availableSize: %d, enableReclaim: %v",
			numaID, numaPoolsTotalQuantity, availableSize, enableReclaim)

		var tErr error
		var leftCPUs machine.CPUSet
		if numaPoolsTotalQuantity <= availableSize && enableReclaim && !p.state.GetAllowSharedCoresOverlapReclaimedCores() {
			leftCPUs, tErr = p.takeCPUsForPoolsInPlace(numaPoolsToQuantityMap, poolsCPUSet, numaAvailableCPUs)
			if tErr != nil {
				return originalAvailableCPUSet, fmt.Errorf("allocate cpus for numa_binding pools in NUMA: %d failed with error: %v",
					numaID, tErr)
			}
		} else {
			// numaPoolsTotalQuantity > availableSize || !enableReclaim || p.state.GetAllowSharedCoresOverlapReclaimedCores()
			// both allocate all numaAvailableCPUs proportionally
			leftCPUs, tErr = p.generateProportionalPoolsCPUSetInPlace(numaPoolsToQuantityMap, poolsCPUSet, numaAvailableCPUs)

			if tErr != nil {
				return originalAvailableCPUSet, fmt.Errorf("generateProportionalPoolsCPUSetInPlace for numa_binding pools in NUMA: %d failed with error: %v",
					numaID, tErr)
			}
		}

		availableCPUs = availableCPUs.Difference(numaCPUs).Union(leftCPUs)
	}

	return availableCPUs, nil
}

// generatePoolsAndIsolation is used to generate cpuset pools and isolated cpuset
// 1. allocate isolated cpuset for pod/containers, and divide total cores evenly if not possible to allocate
// 2. use the left cores to allocate among different pools
// 3. apportion to other pools if reclaimed is disabled
func (p *DynamicPolicy) generatePoolsAndIsolation(
	poolsQuantityMap map[string]map[int]int,
	isolatedQuantityMap map[string]map[string]int, availableCPUs machine.CPUSet,
	reclaimOverlapShareRatio map[string]float64) (poolsCPUSet map[string]machine.CPUSet,
	isolatedCPUSet map[string]map[string]machine.CPUSet, err error,
) {
	poolsBindingNUMAs := sets.NewInt()
	poolsToSkip := make([]string, 0, len(poolsQuantityMap))
	nonBindingPoolsQuantityMap := make(map[string]int)
	for poolName, numaToQuantity := range poolsQuantityMap {
		if len(numaToQuantity) > 1 {
			err = fmt.Errorf("pool: %s cross NUMAs: %+v", poolName, numaToQuantity)
			return
		} else if len(numaToQuantity) == 1 {
			for numaID, quantity := range numaToQuantity {
				if quantity == 0 {
					poolsToSkip = append(poolsToSkip, poolName)
				} else {
					if numaID != commonstate.FakedNUMAID {
						poolsBindingNUMAs.Insert(numaID)
					} else {
						nonBindingPoolsQuantityMap[poolName] = quantity
					}
				}
			}
		} else {
			poolsToSkip = append(poolsToSkip, poolName)
		}
	}

	for _, poolName := range poolsToSkip {
		general.Warningf("pool: %s with 0 quantity, skip generate", poolName)
		delete(poolsQuantityMap, poolName)
	}

	// clear isolated map with zero quantity
	for podUID, containerEntries := range isolatedQuantityMap {
		for containerName, quantity := range containerEntries {
			if quantity == 0 {
				general.Warningf("isolated pod: %s, container: %s with 0 quantity, skip generate it", podUID, containerName)
				delete(containerEntries, containerName)
			}
		}
		if len(containerEntries) == 0 {
			general.Warningf(" isolated pod: %s all container entries skipped", podUID)
			delete(isolatedQuantityMap, podUID)
		}
	}

	poolsCPUSet = make(map[string]machine.CPUSet)
	var nbpErr error
	availableCPUs, nbpErr = p.generateNUMABindingPoolsCPUSetInPlace(poolsCPUSet, poolsQuantityMap, availableCPUs)
	if nbpErr != nil {
		err = fmt.Errorf("generateNUMABindingPoolsCPUSetInPlace failed with error: %v", nbpErr)
		return
	}

	nonBindingAvailableCPUs := machine.NewCPUSet()
	for _, numaID := range p.machineInfo.CPUDetails.NUMANodes().ToSliceNoSortInt() {
		if poolsBindingNUMAs.Has(numaID) {
			continue
		}

		nonBindingAvailableCPUs = nonBindingAvailableCPUs.Union(p.machineInfo.CPUDetails.CPUsInNUMANodes(numaID).Intersection(availableCPUs))
	}
	availableCPUs = availableCPUs.Difference(nonBindingAvailableCPUs)

	nonBindingAvailableSize := nonBindingAvailableCPUs.Size()
	nonBindingPoolsTotalQuantity := general.SumUpMapValues(nonBindingPoolsQuantityMap)

	isolatedCPUSet = make(map[string]map[string]machine.CPUSet)
	isolatedTotalQuantity := general.SumUpMultipleMapValues(isolatedQuantityMap)

	general.Infof("isolatedTotalQuantity: %d, nonBindingPoolsTotalQuantity: %d, nonBindingAvailableSize: %d",
		isolatedTotalQuantity, nonBindingPoolsTotalQuantity, nonBindingAvailableSize)

	// preferredCPUsByPool lets a source share pool preferentially reclaim the CPUs it
	// historically lent to shared_cores isolation and dedicated containers. In overlap mode,
	// proportional allocation still uses these preferences before reclaim overlap is computed
	// reversely from the final share-type pools.
	preferredCPUsByPool := buildIsolationSourcePreferredCPUs(p.state.GetPodEntries())
	dedicatedPreferredCPUsByPool, preferredCPUsByContainer := buildDedicatedSourcePreferredCPUs(p.state.GetPodEntries())
	for poolName, cset := range dedicatedPreferredCPUsByPool {
		preferredCPUsByPool[poolName] = preferredCPUsByPool[poolName].Union(cset)
	}
	historicalPoolPreferredCPUs := buildHistoricalPoolEntryPreferredCPUs(p.state.GetPodEntries(), commonstate.PoolNameReclaim)
	for poolName, cset := range historicalPoolPreferredCPUs {
		preferredCPUsByPool[poolName] = preferredCPUsByPool[poolName].Union(cset)
	}

	var tErr error
	if nonBindingPoolsTotalQuantity+isolatedTotalQuantity <= nonBindingAvailableSize {
		general.Infof("all pools and isolated containers could be allocated")

		isolatedCPUSet, nonBindingAvailableCPUs, tErr = p.takeCPUsForContainersWithPreferred(isolatedQuantityMap, nonBindingAvailableCPUs, preferredCPUsByContainer)
		if tErr != nil {
			err = fmt.Errorf("allocate isolated cpus for dedicated_cores failed with error: %v", tErr)
			return
		}

		if !p.state.GetAllowSharedCoresOverlapReclaimedCores() {
			nonBindingAvailableCPUs, tErr = p.takeCPUsForPoolsInPlaceWithPreferred(nonBindingPoolsQuantityMap, poolsCPUSet, nonBindingAvailableCPUs, preferredCPUsByPool)
			if tErr != nil {
				err = fmt.Errorf("allocate cpus for pools failed with error: %v", tErr)
				return
			}
		} else {
			general.Infof("allowSharedCoresOverlapReclaimedCores is true, take all nonBindingAvailableCPUs for pools")
			nonBindingAvailableCPUs, tErr = p.generateProportionalPoolsCPUSetInPlaceWithPreferred(nonBindingPoolsQuantityMap, poolsCPUSet, nonBindingAvailableCPUs, preferredCPUsByPool)

			if tErr != nil {
				err = fmt.Errorf("generateProportionalPoolsCPUSetInPlaceWithPreferred pools failed with error: %v", tErr)
				return
			}
		}
	} else if nonBindingPoolsTotalQuantity <= nonBindingAvailableSize {
		general.Infof("all pools could be allocated, all isolated containers would be put to pools")

		if !p.state.GetAllowSharedCoresOverlapReclaimedCores() {
			nonBindingAvailableCPUs, tErr = p.takeCPUsForPoolsInPlaceWithPreferred(nonBindingPoolsQuantityMap, poolsCPUSet, nonBindingAvailableCPUs, preferredCPUsByPool)
			if tErr != nil {
				err = fmt.Errorf("allocate cpus for pools failed with error: %v", tErr)
				return
			}
		} else {
			general.Infof("allowSharedCoresOverlapReclaimedCores is true, take all nonBindingAvailableCPUs for pools")
			nonBindingAvailableCPUs, tErr = p.generateProportionalPoolsCPUSetInPlaceWithPreferred(nonBindingPoolsQuantityMap, poolsCPUSet, nonBindingAvailableCPUs, preferredCPUsByPool)

			if tErr != nil {
				err = fmt.Errorf("generateProportionalPoolsCPUSetInPlaceWithPreferred pools failed with error: %v", tErr)
				return
			}
		}
	} else if nonBindingPoolsTotalQuantity > 0 {
		general.Infof("can't allocate for all pools")

		nonBindingAvailableCPUs, tErr = p.generateProportionalPoolsCPUSetInPlaceWithPreferred(nonBindingPoolsQuantityMap, poolsCPUSet, nonBindingAvailableCPUs, preferredCPUsByPool)

		if tErr != nil {
			err = fmt.Errorf("generateProportionalPoolsCPUSetInPlaceWithPreferred pools failed with error: %v", tErr)
			return
		}
	}

	availableCPUs = availableCPUs.Union(nonBindingAvailableCPUs)

	// deal with reserve pool
	if poolsCPUSet[commonstate.PoolNameReserve].IsEmpty() {
		poolsCPUSet[commonstate.PoolNameReserve] = p.reservedCPUs.Clone()
		general.Infof("set pool %s:%s", commonstate.PoolNameReserve, poolsCPUSet[commonstate.PoolNameReserve].String())
	} else {
		err = fmt.Errorf("static pool %s result: %s is generated dynamically", commonstate.PoolNameReserve, poolsCPUSet[commonstate.PoolNameReserve].String())
		return
	}

	enableReclaim := p.dynamicConfig.GetDynamicConfiguration().EnableReclaim
	allowOverlap := p.state.GetAllowSharedCoresOverlapReclaimedCores()
	if !enableReclaim {
		// Reclaim disabled keeps the legacy downgrade behavior: use leftover CPUs as a
		// temporary reclaim bucket, then apportion them back to non-binding pools.
		poolsCPUSet[commonstate.PoolNameReclaim] = poolsCPUSet[commonstate.PoolNameReclaim].Union(availableCPUs)
	}

	general.Infof("poolsCPUSet before reclaim apportion/overlap: %+v", poolsCPUSet)

	if !allowOverlap {
		if !enableReclaim && poolsCPUSet[commonstate.PoolNameReclaim].Size() > p.reservedReclaimedCPUsSize {
			poolsCPUSet[commonstate.PoolNameReclaim] = p.apportionReclaimedPool(
				poolsCPUSet, poolsCPUSet[commonstate.PoolNameReclaim].Clone(), nonBindingPoolsQuantityMap)
			general.Infof("apportionReclaimedPool finished, current %s pool: %s",
				commonstate.PoolNameReclaim, poolsCPUSet[commonstate.PoolNameReclaim].String())
		}
	} else {
		// p.state.GetAllowSharedCoresOverlapReclaimedCores() == true
		poolsCPUSet[commonstate.PoolNameReclaim] = poolsCPUSet[commonstate.PoolNameReclaim].Union(availableCPUs)
		for poolName, cset := range poolsCPUSet {
			if ratio, found := reclaimOverlapShareRatio[poolName]; found && ratio > 0 {

				req := int(math.Ceil(float64(cset.Size()) * ratio))

				// if p.state.GetAllowSharedCoresOverlapReclaimedCores() == false, we will take cpus for reclaim pool lastly,
				// else we also should take cpus for reclaim pool reversely overlapping with share type pool to aviod cpuset jumping obviously
				var tErr error
				overlapCPUs, _, tErr := calculator.TakeByNUMABalanceReversely(p.machineInfo, cset, req)
				if tErr != nil {
					err = fmt.Errorf("take overlapCPUs from: %s to %s by ratio: %.4f failed with err: %v",
						poolName, commonstate.PoolNameReclaim, ratio, tErr)
					return
				}

				general.Infof("merge overlapCPUs: %s from pool: %s to %s by ratio: %.4f",
					overlapCPUs.String(), poolName, commonstate.PoolNameReclaim, ratio)
				poolsCPUSet[commonstate.PoolNameReclaim] = poolsCPUSet[commonstate.PoolNameReclaim].Union(overlapCPUs)
			}
		}
	}

	general.Infof("poolsCPUSet after reclaim apportion/overlap: %+v", poolsCPUSet)

	currentPodEntries := p.state.GetPodEntries()
	if enableReclaim && !allowOverlap && poolsCPUSet[commonstate.PoolNameReclaim].IsEmpty() {
		if cset, cErr := currentPodEntries.GetCPUSetForPool(commonstate.PoolNameReclaim); cErr == nil && !cset.IsEmpty() {
			allocatedNonReclaimCPUs := machine.NewCPUSet()
			for poolName, poolCPUSet := range poolsCPUSet {
				if poolName == commonstate.PoolNameReclaim {
					continue
				}
				allocatedNonReclaimCPUs = allocatedNonReclaimCPUs.Union(poolCPUSet)
			}
			for _, containerCPUSetByName := range isolatedCPUSet {
				for _, containerCPUSet := range containerCPUSetByName {
					allocatedNonReclaimCPUs = allocatedNonReclaimCPUs.Union(containerCPUSet)
				}
			}
			poolsCPUSet[commonstate.PoolNameReclaim] = cset.Difference(allocatedNonReclaimCPUs)
			general.Infof("preserve current %s pool after deducting non-reclaim allocations, previous: %s, deducted: %s, current: %s",
				commonstate.PoolNameReclaim, cset.String(), allocatedNonReclaimCPUs.String(), poolsCPUSet[commonstate.PoolNameReclaim].String())
		}
	}

	if poolsCPUSet[commonstate.PoolNameReclaim].IsEmpty() {
		// for reclaimed pool, we must make them exist when the node isn't in hybrid mode even if cause overlap
		general.Infof("fallback takeByNUMABalance in generatePoolsAndIsolation for reclaimedCPUSet: %s", p.reservedReclaimedCPUSet.String())
		poolsCPUSet[commonstate.PoolNameReclaim] = p.reservedReclaimedCPUSet.Clone()
	}

	// deal with forbidden pools
	for _, poolName := range state.ForbiddenPools.List() {
		cset, err := currentPodEntries.GetCPUSetForPool(poolName)
		if err != nil {
			general.Infof("can't get CPUSet for pool %s: %v", poolName, err)
			continue
		}
		poolsCPUSet[poolName] = cset.Clone()
	}

	// add system exclusive pools, so that system cores pod with pool set to one of them could be allocated
	for poolName, entry := range currentPodEntries {
		if !commonstate.IsSystemPool(poolName) {
			continue
		}
		allocationInfo := entry.GetPoolEntry()
		if allocationInfo == nil {
			continue
		}
		poolsCPUSet[poolName] = allocationInfo.AllocationResult.Clone()
	}

	return
}

func (p *DynamicPolicy) generateProportionalPoolsCPUSetInPlace(poolsQuantityMap map[string]int,
	poolsCPUSet map[string]machine.CPUSet, availableCPUs machine.CPUSet,
) (machine.CPUSet, error) {
	availableSize := availableCPUs.Size()

	proportionalPoolsQuantityMap, totalProportionalPoolsQuantity := getProportionalPoolsQuantityMap(poolsQuantityMap, availableSize)

	general.Infof("poolsQuantityMap: %v, proportionalPoolsQuantityMap: %v", poolsQuantityMap, proportionalPoolsQuantityMap)

	// availableSize can't satisfy every pool has at least one cpu,
	// we make all pools equals to availableCPUs in this case.
	if totalProportionalPoolsQuantity > availableSize {
		for poolName := range poolsQuantityMap {
			if _, found := poolsCPUSet[poolName]; found {
				return availableCPUs.Clone(), fmt.Errorf("duplicated pool: %s", poolName)
			}

			poolsCPUSet[poolName] = availableCPUs.Clone()
		}

		return machine.NewCPUSet(), nil
	} else {
		var err error
		availableCPUs, err = p.takeCPUsForPoolsInPlace(proportionalPoolsQuantityMap, poolsCPUSet, availableCPUs)
		if err != nil {
			return availableCPUs, err
		}
	}

	return availableCPUs, nil
}

func getProportionalPoolsQuantityMap(originalPoolsQuantityMap map[string]int, availableSize int) (map[string]int, int) {
	totalProportionalPoolsQuantity := 0
	originalPoolsTotalQuantity := general.SumUpMapValues(originalPoolsQuantityMap)
	proportionalPoolsQuantityMap := make(map[string]int)

	for poolName, poolQuantity := range originalPoolsQuantityMap {
		proportionalSize := general.Max(getProportionalSize(poolQuantity, originalPoolsTotalQuantity, availableSize, true /*ceil*/), 1)
		proportionalPoolsQuantityMap[poolName] = proportionalSize
		totalProportionalPoolsQuantity += proportionalSize
	}

	poolNames := make([]string, 0, len(proportionalPoolsQuantityMap))

	for poolName := range proportionalPoolsQuantityMap {
		poolNames = append(poolNames, poolName)
	}

	sort.Slice(poolNames, func(x, y int) bool {
		// sort in descending order
		return proportionalPoolsQuantityMap[poolNames[x]] > proportionalPoolsQuantityMap[poolNames[y]]
	})

	// corner case: after divide, the total count goes to be bigger than available total
	for totalProportionalPoolsQuantity > availableSize {
		curTotalProportionalPoolsQuantity := totalProportionalPoolsQuantity

		for _, poolName := range poolNames {
			quantity := proportionalPoolsQuantityMap[poolName]

			if quantity > 1 && totalProportionalPoolsQuantity > 0 {
				quantity--
				totalProportionalPoolsQuantity--
				proportionalPoolsQuantityMap[poolName] = quantity

				if totalProportionalPoolsQuantity == availableSize {
					break
				}
			}
		}

		// availableSize can't satisfy every pool has at least one cpu
		if curTotalProportionalPoolsQuantity == totalProportionalPoolsQuantity {
			break
		}
	}

	return proportionalPoolsQuantityMap, totalProportionalPoolsQuantity
}

// apportionReclaimedPool tries to allocate reclaimed cores to none-binding && none-reclaimed pools.
// if we disable reclaim on current node, this could be used a down-grade strategy
// to disable reclaimed workloads in emergency
func (p *DynamicPolicy) apportionReclaimedPool(poolsCPUSet map[string]machine.CPUSet, reclaimedCPUs machine.CPUSet, nonBindingPoolsQuantityMap map[string]int) machine.CPUSet {
	totalSize := 0
	for poolName, poolCPUs := range poolsCPUSet {
		if state.ResidentPools.Has(poolName) {
			continue
		} else if _, found := nonBindingPoolsQuantityMap[poolName]; !found {
			// numa-binding && none-reclaimed pools already handled in generateNUMABindingPoolsCPUSetInPlace
			continue
		}
		totalSize += poolCPUs.Size()
	}

	availableSize := reclaimedCPUs.Size() - p.reservedReclaimedCPUsSize
	if availableSize <= 0 || totalSize == 0 {
		return reclaimedCPUs
	}

	for poolName, poolCPUs := range poolsCPUSet {
		if state.ResidentPools.Has(poolName) {
			continue
		} else if _, found := nonBindingPoolsQuantityMap[poolName]; !found {
			// numa-binding && none-reclaimed pools already handled in generateNUMABindingPoolsCPUSetInPlace
			continue
		}

		proportionalSize := general.Max(getProportionalSize(poolCPUs.Size(), totalSize, availableSize, false /*ceil*/), 1)

		var err error
		var cpuset machine.CPUSet
		cpuset, reclaimedCPUs, err = calculator.TakeHTByNUMABalance(p.machineInfo, reclaimedCPUs, proportionalSize)
		if err != nil {
			general.Errorf("take %d cpus from reclaimedCPUs: %s, size: %d failed with error: %v",
				proportionalSize, reclaimedCPUs.String(), reclaimedCPUs.Size(), err)
			return reclaimedCPUs
		}

		poolsCPUSet[poolName] = poolCPUs.Union(cpuset)
		general.Infof("take %s to %s; prev: %s, current: %s", cpuset.String(), poolName, poolCPUs.String(), poolsCPUSet[poolName].String())

		if reclaimedCPUs.Size() <= p.reservedReclaimedCPUsSize {
			break
		}
	}

	return reclaimedCPUs
}

func (p *DynamicPolicy) takeCPUsForPoolsInPlace(poolsQuantityMap map[string]int,
	poolsCPUSet map[string]machine.CPUSet,
	availableCPUs machine.CPUSet,
) (machine.CPUSet, error) {
	originalAvailableCPUSet := availableCPUs.Clone()
	var poolsCPUSetToAdd map[string]machine.CPUSet
	var tErr error
	poolsCPUSetToAdd, availableCPUs, tErr = p.takeCPUsForPools(poolsQuantityMap, availableCPUs)
	if tErr != nil {
		return originalAvailableCPUSet, fmt.Errorf("allocate cpus for pools failed with error: %v", tErr)
	}

	for poolName, cset := range poolsCPUSetToAdd {
		if _, found := poolsCPUSet[poolName]; found {
			return originalAvailableCPUSet, fmt.Errorf("duplicated pool: %s", poolName)
		}

		poolsCPUSet[poolName] = cset
	}

	return availableCPUs, nil
}

// takeCPUsForPools tries to allocate cpuset for each given pool,
// and it will consider the total available cpuset during calculation.
// the returned value includes cpuset pool map and remaining available cpuset.
func (p *DynamicPolicy) takeCPUsForPools(poolsQuantityMap map[string]int,
	availableCPUs machine.CPUSet,
) (map[string]machine.CPUSet, machine.CPUSet, error) {
	poolsCPUSet := make(map[string]machine.CPUSet)
	clonedAvailableCPUs := availableCPUs.Clone()

	// to avoid random map iteration sequence to generate pools randomly
	sortedPoolNames := general.GetSortedMapKeys(poolsQuantityMap)
	for _, poolName := range sortedPoolNames {
		req := poolsQuantityMap[poolName]
		general.Infof("allocated for pool: %s with req: %d", poolName, req)

		var err error
		var cset machine.CPUSet
		cset, availableCPUs, err = calculator.TakeByNUMABalance(p.machineInfo, availableCPUs, req)
		if err != nil {
			return nil, clonedAvailableCPUs, fmt.Errorf("take cpu for pool: %s of req: %d failed with error: %v",
				poolName, req, err)
		}
		poolsCPUSet[poolName] = cset
	}
	return poolsCPUSet, availableCPUs, nil
}

// takeCPUsForContainers tries to allocate cpuset for the given pod/container combinations,
// and it will consider the total available cpuset during calculation.
// the returned value includes cpuset map for pod/container combinations and remaining available cpuset.
func (p *DynamicPolicy) takeCPUsForContainers(containersQuantityMap map[string]map[string]int,
	availableCPUs machine.CPUSet,
) (map[string]map[string]machine.CPUSet, machine.CPUSet, error) {
	containersCPUSet := make(map[string]map[string]machine.CPUSet)
	clonedAvailableCPUs := availableCPUs.Clone()

	for podUID, containerQuantities := range containersQuantityMap {
		if len(containerQuantities) > 0 {
			containersCPUSet[podUID] = make(map[string]machine.CPUSet)
		}

		for containerName, quantity := range containerQuantities {
			general.Infof("allocated for pod: %s container: %s with req: %d", podUID, containerName, quantity)

			var err error
			var cset machine.CPUSet
			cset, availableCPUs, err = calculator.TakeByNUMABalance(p.machineInfo, availableCPUs, quantity)
			if err != nil {
				return nil, clonedAvailableCPUs, fmt.Errorf("take cpu for pod: %s container: %s of req: %d failed with error: %v",
					podUID, containerName, quantity, err)
			}
			containersCPUSet[podUID][containerName] = cset
		}
	}
	return containersCPUSet, availableCPUs, nil
}

func (p *DynamicPolicy) shouldSharedCoresRampUp(ctx context.Context, podUID string) bool {
	if p.isSharedCoresRampUpDisabled() {
		general.Infof("shared cores ramp up is disabled by dynamic config, podUID: %s", podUID)
		return false
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	pod, err := p.metaServer.GetPod(ctx, podUID)

	if err != nil {
		general.Errorf("get pod: %s failed with error: %v during admission, try to ramp up it", podUID, err)
		return true
	} else if pod == nil {
		general.Infof("can't get pod: %s from metaServer, not try to ramp up it", podUID)
		return false
	} else if !native.PodIsPending(pod) {
		general.Infof("pod: %s/%s isn't pending(not admit firstly), not try to ramp up it", pod.Namespace, pod.Name)
		return false
	} else {
		general.Infof("pod: %s/%s isn't active, try to ramp up it", pod.Namespace, pod.Name)
		return true
	}
}

func (p *DynamicPolicy) isSharedCoresRampUpDisabled() bool {
	if p.dynamicConfig == nil {
		return false
	}
	dyn := p.dynamicConfig.GetDynamicConfiguration()
	return dyn != nil && dyn.DisableSharedCoresRampUp
}

func (p *DynamicPolicy) doAndCheckPutAllocationInfoPodResizingAware(originAllocationInfo, allocationInfo *state.AllocationInfo, incrByReq, podInplaceUpdateResizing, persistCheckpoint bool) (*state.AllocationInfo, error) {
	if allocationInfo == nil {
		return nil, fmt.Errorf("doAndCheckPutAllocationInfo got nil allocationInfo")
	}

	// need to adjust pools and putAllocationsAndAdjustAllocationEntries will set the allocationInfo after adjusted
	err := p.putAllocationsAndAdjustAllocationEntriesResizeAware([]*state.AllocationInfo{originAllocationInfo}, []*state.AllocationInfo{allocationInfo}, incrByReq, podInplaceUpdateResizing, persistCheckpoint)
	if err != nil {
		general.Errorf("pod: %s/%s, container: %s putAllocationsAndAdjustAllocationEntriesResizeAware failed with error: %v",
			allocationInfo.PodNamespace, allocationInfo.PodName, allocationInfo.ContainerName, err)
		return nil, fmt.Errorf("putallocationsandadjustallocationentries failed with error: %s", strings.ToLower(err.Error()))
	}

	checkedAllocationInfo := p.state.GetAllocationInfo(allocationInfo.PodUid, allocationInfo.ContainerName)
	if checkedAllocationInfo == nil {
		general.Errorf("pod: %s/%s, container: %s get nil allocationInfo after putAllocationsAndAdjustAllocationEntries",
			allocationInfo.PodNamespace, allocationInfo.PodName, allocationInfo.ContainerName)
		return nil, fmt.Errorf("allocationInfo missing after putAllocationsAndAdjustAllocationEntries: pod=%s/%s uid=%s container=%s ownerPool=%s specifiedPool=%s stateRevision=%d",
			allocationInfo.PodNamespace, allocationInfo.PodName, allocationInfo.PodUid, allocationInfo.ContainerName,
			allocationInfo.OwnerPoolName, allocationInfo.GetSpecifiedPoolName(), p.state.GetRevision())
	}

	return checkedAllocationInfo, nil
}

func (p *DynamicPolicy) doAndCheckPutAllocationInfo(allocationInfo *state.AllocationInfo, incrByReq, persistCheckpoint bool) (*state.AllocationInfo, error) {
	return p.doAndCheckPutAllocationInfoPodResizingAware(nil, allocationInfo, incrByReq, false, persistCheckpoint)
}

func (p *DynamicPolicy) getReclaimOverlapShareRatio(entries state.PodEntries) (map[string]float64, error) {
	if !p.state.GetAllowSharedCoresOverlapReclaimedCores() {
		return nil, nil
	}

	if entries.CheckPoolEmpty(commonstate.PoolNameReclaim) {
		return nil, fmt.Errorf("reclaim pool misses in current entries")
	}

	reclaimOverlapShareRatio := make(map[string]float64)

	curReclaimCPUSet := entries[commonstate.PoolNameReclaim][commonstate.FakedContainerName].AllocationResult

	// Iterate through all pools to calculate overlap ratios
	for poolName, subEntries := range entries {
		if !subEntries.IsPoolEntry() {
			continue
		}

		allocationInfo := subEntries.GetPoolEntry()

		if allocationInfo != nil && commonstate.GetPoolType(poolName) == commonstate.PoolNameShare {
			if allocationInfo.AllocationResult.IsEmpty() {
				continue
			}

			shareTypePoolSize := allocationInfo.AllocationResult.Size()
			overlapSize := allocationInfo.AllocationResult.Intersection(curReclaimCPUSet).Size()

			if overlapSize == 0 {
				continue
			}

			reclaimOverlapShareRatio[poolName] = float64(overlapSize) / float64(shareTypePoolSize)
		}
	}

	// If no overlap was found, calculate non-overlap ratios
	if len(reclaimOverlapShareRatio) == 0 {
		reclaimNonOverlapShareRatio := make(map[string]float64)

		// Iterate over all sub-entries to compute non-overlap ratios
		for _, subEntries := range entries {
			if subEntries.IsPoolEntry() {
				continue
			}

			for _, allocationInfo := range subEntries {
				if allocationInfo == nil || allocationInfo.AllocationResult.IsEmpty() {
					continue
				}

				// Only process shared pools
				poolName := allocationInfo.GetPoolName()
				if commonstate.GetPoolType(poolName) == commonstate.PoolNameShare {
					requestQuantity := allocationInfo.RequestQuantity
					if requestQuantity > 0 {
						reclaimNonOverlapShareRatio[poolName] += requestQuantity / float64(allocationInfo.AllocationResult.Size())
					}
				}
			}
		}

		// Convert non-overlap ratios to overlap ratios
		for poolName, ratio := range reclaimNonOverlapShareRatio {
			reclaimOverlapShareRatio[poolName] = 1.0 - ratio
		}
	}
	return reclaimOverlapShareRatio, nil
}

func (p *DynamicPolicy) systemCoresHintHandler(_ context.Context, request *pluginapi.ResourceRequest) (*pluginapi.ResourceHintsResponse, error) {
	return util.PackResourceHintsResponse(request, string(v1.ResourceCPU),
		map[string]*pluginapi.ListOfTopologyHints{
			string(v1.ResourceCPU): nil, // indicates that there is no numa preference
		})
}

func (p *DynamicPolicy) systemCoresAllocationHandler(ctx context.Context, req *pluginapi.ResourceRequest, persistCheckpoint bool) (*pluginapi.ResourceAllocationResponse, error) {
	if req.ContainerType == pluginapi.ContainerType_SIDECAR {
		return p.allocationSidecarHandler(ctx, req, apiconsts.PodAnnotationQoSLevelSystemCores, persistCheckpoint)
	}

	allocationInfo := &state.AllocationInfo{
		AllocationMeta: commonstate.GenerateGenericContainerAllocationMeta(req,
			commonstate.EmptyOwnerPoolName, apiconsts.PodAnnotationQoSLevelSystemCores),
		InitTimestamp: time.Now().Format(util.QRMTimeFormat),
	}
	poolCPUSet, topologyAwareAssignments, err := p.getSystemPoolCPUSetAndNumaAwareAssignments(p.state.GetPodEntries(), allocationInfo)
	if err != nil {
		general.ErrorS(err, "unable to get system pool cpuset and topologyAwareAssignments",
			"podNamespace", req.PodNamespace,
			"podName", req.PodName,
			"containerName", req.ContainerName)
		return nil, err
	}

	systemPoolName, err := allocationInfo.GetSpecifiedSystemPoolName()
	if err != nil {
		return nil, err
	}

	general.InfoS("allocate system pool cpuset successfully",
		"podNamespace", req.PodNamespace,
		"podName", req.PodName,
		"containerName", req.ContainerName,
		"poolName", systemPoolName,
		"result", poolCPUSet.String(),
		"topologyAwareAssignments", topologyAwareAssignments)

	allocationInfo.OwnerPoolName = systemPoolName
	allocationInfo.AllocationResult = poolCPUSet
	allocationInfo.OriginalAllocationResult = poolCPUSet.Clone()
	allocationInfo.TopologyAwareAssignments = topologyAwareAssignments
	allocationInfo.OriginalTopologyAwareAssignments = machine.DeepcopyCPUAssignment(topologyAwareAssignments)

	if err := p.updateAllocationInfo(allocationInfo.PodUid, allocationInfo.ContainerName, nil, allocationInfo, persistCheckpoint); err != nil {
		return nil, err
	}
	podEntries := p.state.GetPodEntries()

	updatedMachineState, err := generateMachineStateFromPodEntries(p.machineInfo.CPUTopology, podEntries, p.state.GetMachineState())
	if err != nil {
		general.Errorf("pod: %s/%s, container: %s generateMachineStateFromPodEntries failed with error: %v",
			req.PodNamespace, req.PodName, req.ContainerName, err)
		return nil, fmt.Errorf("generateMachineStateFromPodEntries failed with error: %v", err)
	}
	p.state.SetMachineState(updatedMachineState, persistCheckpoint)

	resp, err := cpuutil.PackAllocationResponse(allocationInfo, string(v1.ResourceCPU), util.OCIPropertyNameCPUSetCPUs, false, true, req, allocationInfo.Annotations)
	if err != nil {
		general.Errorf("pod: %s/%s, container: %s PackResourceAllocationResponseByAllocationInfo failed with error: %v",
			req.PodNamespace, req.PodName, req.ContainerName, err)
		return nil, fmt.Errorf("PackResourceAllocationResponseByAllocationInfo failed with error: %v", err)
	}
	p.clearCPUSetInAllocationResponseIfNeeded(resp, allocationInfo)
	return resp, nil
}

// getSystemPoolCPUSetAndNumaAwareAssignments gets the system pool cpuset and topologyAwareAssignments for the allocationInfo.
// For system shared pools, use the specified pool name, and these pools must exist in the podEntries.
// For system exclusive pool, use the system pool name, and also these pools must exist in the podEntries.
func (p *DynamicPolicy) getSystemPoolCPUSetAndNumaAwareAssignments(podEntries state.PodEntries,
	allocationInfo *state.AllocationInfo,
) (machine.CPUSet, map[int]machine.CPUSet, error) {
	if allocationInfo == nil {
		return machine.CPUSet{}, nil, fmt.Errorf("allocationInfo is nil")
	}

	poolCPUSet := machine.NewCPUSet()
	specifiedPoolName := allocationInfo.GetSpecifiedPoolName()
	if specifiedPoolName != commonstate.EmptyOwnerPoolName {
		poolName := specifiedPoolName
		if _, ok := p.dynamicConfig.GetDynamicConfiguration().SystemExclusivePool[specifiedPoolName]; ok {
			poolName = commonstate.GetSystemPoolName(specifiedPoolName)
		}

		for pool, entries := range podEntries {
			if !entries.IsPoolEntry() {
				continue
			}

			if pool == poolName || strings.HasPrefix(pool, poolName) {
				poolCPUSet = poolCPUSet.Union(entries.GetPoolEntry().AllocationResult)
				general.Infof("pod: %s/%s, container: %s get system pool cpuset from pool: %s, cpuset: %s", allocationInfo.PodNamespace, allocationInfo.PodName,
					allocationInfo.ContainerName, pool, entries.GetPoolEntry().AllocationResult.String())
			}
		}
	}

	// if pool set is empty, try to get default cpuset
	if poolCPUSet.IsEmpty() {
		// if the pod is numa binding, get the default cpuset from machine state
		if allocationInfo.CheckNUMABinding() {
			poolCPUSet = p.state.GetMachineState().GetAvailableCPUSet(p.reservedCPUs)
		}

		// if the default cpuset is empty or no numa binding, use all cpuset as default cpuset
		if poolCPUSet.IsEmpty() {
			poolCPUSet = p.machineInfo.CPUDetails.CPUs()
		}
		general.Infof("pod: %s/%s, container: %s get system pool cpuset from default cpuset: %s", allocationInfo.PodNamespace, allocationInfo.PodName,
			allocationInfo.ContainerName, poolCPUSet.String())
	}

	if poolCPUSet.IsEmpty() {
		return machine.CPUSet{}, nil, fmt.Errorf("no system pool cpuset for pool %s", specifiedPoolName)
	}

	topologyAwareAssignments, err := machine.GetNumaAwareAssignments(p.machineInfo.CPUTopology, poolCPUSet)
	if err != nil {
		return machine.CPUSet{}, nil, fmt.Errorf("unable to get numa aware assignments: %v", err)
	}

	return poolCPUSet, topologyAwareAssignments, nil
}

func (p *DynamicPolicy) getAllocationPoolEntry(allocationInfo *state.AllocationInfo, ownerPoolName string, entries state.PodEntries) (*state.AllocationInfo, error) {
	poolEntry := entries[ownerPoolName][commonstate.FakedContainerName]
	if poolEntry != nil {
		return poolEntry, nil
	}

	errMsg := fmt.Sprintf("cpu advisor doesn't return entry for pool: %s and it's referred by pod: %s/%s, container: %s, qosLevel: %s",
		ownerPoolName, allocationInfo.PodNamespace, allocationInfo.PodName, allocationInfo.ContainerName, allocationInfo.QoSLevel)

	general.Errorf(errMsg)

	_ = p.emitter.StoreInt64(util.MetricNameOrphanContainer, 1, metrics.MetricTypeNameCount,
		metrics.MetricTag{Key: "podNamespace", Val: allocationInfo.PodNamespace},
		metrics.MetricTag{Key: "podName", Val: allocationInfo.PodName},
		metrics.MetricTag{Key: "containerName", Val: allocationInfo.ContainerName},
		metrics.MetricTag{Key: "poolName", Val: ownerPoolName})
	return nil, fmt.Errorf(errMsg)
}

func (p *DynamicPolicy) updateReclaimAllocationResultByPoolEntry(allocationInfo *state.AllocationInfo,
	poolEntry *state.AllocationInfo, nonReclaimActualBindingNUMAs machine.CPUSet,
) error {
	numaID, err := allocationInfo.GetSpecifiedNUMABindingNUMAID()
	if err != nil {
		return err
	}

	getActualNUMABindingResult := func(topologyAwareAssignments map[int]machine.CPUSet) (machine.CPUSet, map[int]machine.CPUSet, error) {
		var (
			actualTopologyAwareAssignments map[int]machine.CPUSet
			actualAllocationResult         machine.CPUSet
		)
		if numaID != commonstate.FakedNUMAID {
			cpuSet, ok := topologyAwareAssignments[numaID]
			if !ok {
				return machine.CPUSet{}, nil, fmt.Errorf("pod: %s/%s container: %s is reclaimed_cores with numa_binding specified numa: %d not found in topologyAwareAssignments: %v",
					allocationInfo.PodNamespace, allocationInfo.PodName, allocationInfo.ContainerName, numaID, topologyAwareAssignments)
			}
			actualAllocationResult = cpuSet.Clone()
			actualTopologyAwareAssignments = map[int]machine.CPUSet{
				numaID: cpuSet.Clone(),
			}
		} else {
			// set non-RNB numa allocation info
			numaSet := machine.NewCPUSet()
			newTopologyAwareAssignments := make(map[int]machine.CPUSet)
			for numaNode, cpuSet := range topologyAwareAssignments {
				if !nonReclaimActualBindingNUMAs.Contains(numaNode) {
					continue
				}

				if cpuSet.Size() > 0 {
					numaSet = numaSet.Union(cpuSet)
				}

				newTopologyAwareAssignments[numaNode] = cpuSet.Clone()
			}
			actualAllocationResult = numaSet
			actualTopologyAwareAssignments = newTopologyAwareAssignments
		}
		return actualAllocationResult, actualTopologyAwareAssignments, nil
	}

	actualAllocationResult, actualTopologyAwareAssignments, err := getActualNUMABindingResult(machine.DeepcopyCPUAssignment(poolEntry.TopologyAwareAssignments))
	if err != nil {
		return fmt.Errorf("get actual NUMA binding result: %v", err)
	}

	actualOriginalAllocationResult, actualOriginalTopologyAwareAssignments, err := getActualNUMABindingResult(machine.DeepcopyCPUAssignment(poolEntry.OriginalTopologyAwareAssignments))
	if err != nil {
		return fmt.Errorf("get original actual NUMA binding result: %v", err)
	}

	general.Infof("put pod: %s/%s container: %s to pool: %s, set its allocation result from %s to %s",
		allocationInfo.PodNamespace, allocationInfo.PodName, allocationInfo.ContainerName, poolEntry.OwnerPoolName, allocationInfo.AllocationResult.String(), poolEntry.AllocationResult.String())

	allocationInfo.OwnerPoolName = poolEntry.OwnerPoolName
	allocationInfo.AllocationResult = actualAllocationResult
	allocationInfo.OriginalAllocationResult = actualOriginalAllocationResult
	allocationInfo.TopologyAwareAssignments = actualTopologyAwareAssignments
	allocationInfo.OriginalTopologyAwareAssignments = actualOriginalTopologyAwareAssignments
	return nil
}

// isRampUpReclaimHardPartitionEnabled reports whether the ramp-up reclaim hard
// partition feature is enabled by the current dynamic configuration.
func (p *DynamicPolicy) isRampUpReclaimHardPartitionEnabled() bool {
	if p.dynamicConfig == nil {
		return false
	}
	dyn := p.dynamicConfig.GetDynamicConfiguration()
	return dyn != nil && dyn.EnableRampUpReclaimHardPartition
}

// isReclaimEnabled reports the node-level reclaim switch from dynamic config.
func (p *DynamicPolicy) isReclaimEnabled() bool {
	if p.dynamicConfig == nil {
		return false
	}
	dyn := p.dynamicConfig.GetDynamicConfiguration()
	return dyn != nil && dyn.EnableReclaim
}

// deriveRampUpReclaimFloor derives one node-level hard reclaim floor shared by
// every ramp-up QoS path. enteringRampUp is true while admitting a new ramp-up
// allocation; otherwise at least one checkpointed RampUp allocation must
// exist. The floor covers all machine NUMAs rather than the current Pod's
// topology hint and keeps at least two CPUs on each NUMA. Configured reclaim
// CPUs are preserved, and CPUs already owned by the live reclaim pool are
// preferred to keep the result deterministic across recalculations.
func (p *DynamicPolicy) deriveRampUpReclaimFloor(machineState state.NUMANodeMap, enteringRampUp bool) (machine.CPUSet, error) {
	floor := machine.NewCPUSet()
	if !p.isRampUpReclaimHardPartitionEnabled() || p.machineInfo == nil {
		return floor, nil
	}
	if !enteringRampUp {
		hasActiveRampUp := false
		for _, containerEntries := range p.state.GetPodEntries() {
			if containerEntries.IsPoolEntry() {
				continue
			}
			for _, allocation := range containerEntries {
				if allocation != nil && allocation.RampUp {
					hasActiveRampUp = true
					break
				}
			}
			if hasActiveRampUp {
				break
			}
		}
		if !hasActiveRampUp {
			return floor, nil
		}
	}

	currentReclaim := machine.NewCPUSet()
	if reclaimInfo := p.state.GetAllocationInfo(commonstate.PoolNameReclaim, commonstate.FakedContainerName); reclaimInfo != nil {
		currentReclaim = reclaimInfo.AllocationResult
	}

	ratio := p.getInitialRampUpReclaimCPUSetRatio()
	numaIDs := p.machineInfo.CPUDetails.NUMANodes().ToSliceInt()
	eligibleByNUMA := make(map[int]machine.CPUSet, len(numaIDs))
	availableByNUMA := make(map[int]int, len(numaIDs))
	reservedFloorByNUMA := make(map[int]machine.CPUSet, len(numaIDs))
	totalEligible := 0
	for _, numaID := range numaIDs {
		numaState := machineState[numaID]
		if numaState == nil {
			return machine.NewCPUSet(), fmt.Errorf("derive ramp-up reclaim floor: missing machine state for NUMA %d", numaID)
		}
		eligible := numaState.GetAvailableCPUSet(p.reservedCPUs)
		reservedFloor := p.reservedReclaimedCPUSet.Intersection(eligible)
		eligibleByNUMA[numaID] = eligible
		availableByNUMA[numaID] = eligible.Size()
		reservedFloorByNUMA[numaID] = reservedFloor
		totalEligible += eligible.Size()
	}

	configuredFloor := p.reservedReclaimedCPUsSize
	if p.conf != nil {
		if dynamicConf := p.conf.GetDynamicConfiguration(); dynamicConf != nil {
			if quantity, ok := dynamicConf.MinReclaimedResourceForAllocate[v1.ResourceCPU]; ok {
				configuredFloor = int(quantity.Value())
			}
		}
	}
	minimum := len(numaIDs) * 2
	if configuredFloor > minimum {
		minimum = configuredFloor
	}
	globalTarget := machine.CalculateGlobalRampUpReclaimTarget(totalEligible, ratio, minimum)
	targetByNUMA, err := machine.DistributeNUMATarget(availableByNUMA, globalTarget, 2)
	if err != nil {
		return machine.NewCPUSet(), fmt.Errorf("derive ramp-up reclaim floor failed: %w", err)
	}

	for _, numaID := range numaIDs {
		eligible := eligibleByNUMA[numaID]
		reservedFloor := reservedFloorByNUMA[numaID]
		target := targetByNUMA[numaID]
		// reservedReclaimedCPUSet is identity-bearing configuration, not merely
		// a target count. Preserve those exact CPUs first; a positive ratio may
		// add CPUs while preferring the live reclaim set.
		if reservedFloor.Size() > target {
			return machine.NewCPUSet(), fmt.Errorf(
				"derive ramp-up reclaim floor for NUMA %d: reserved floor %d exceeds balanced target %d",
				numaID, reservedFloor.Size(), target)
		}
		floorInNUMA := reservedFloor
		additional := target - floorInNUMA.Size()
		if additional > 0 {
			additionalEligible := eligible.Difference(floorInNUMA)
			preferred := currentReclaim.Intersection(additionalEligible)
			supplement, err := p.takeByTopologyPreferring(additionalEligible, preferred, additional)
			if err != nil {
				return machine.NewCPUSet(), fmt.Errorf("select ramp-up reclaim floor for NUMA %d failed: %w", numaID, err)
			}
			floorInNUMA = floorInNUMA.Union(supplement)
		}
		floor = floor.Union(floorInNUMA)
	}
	return floor, nil
}

// numaBindingPartitionEligibility applies the same resource-package owner rules
// as the advisor block planner to each hinted NUMA's currently available CPUs.
func (p *DynamicPolicy) numaBindingPartitionEligibility(
	machineState state.NUMANodeMap,
	resourcePackageName string,
	hintNodes []uint64,
) (map[int]machine.CPUSet, map[int]machine.CPUSet, error) {
	rpPinnedCPUSet := make(map[string]machine.CPUSet)
	for _, numaState := range machineState {
		if numaState == nil {
			continue
		}
		for resourcePackage, rpState := range numaState.ResourcePackageStates {
			if rpState != nil && !rpState.PinnedCPUSet.IsEmpty() {
				rpPinnedCPUSet[resourcePackage] = rpPinnedCPUSet[resourcePackage].Union(rpState.PinnedCPUSet)
			}
		}
	}
	allPinnedCPUs := machine.NewCPUSet()
	for _, pinnedCPUs := range rpPinnedCPUSet {
		allPinnedCPUs = allPinnedCPUs.Union(pinnedCPUs)
	}

	selectorText := p.dynamicConfig.GetDynamicConfiguration().DisableReclaimPinnedCPUSetResourcePackageSelector
	disableReclaimSelector, err := general.ParseSelector(selectorText)
	if err != nil {
		return nil, nil, fmt.Errorf("parse disable-reclaim resource package selector: %w", err)
	}
	nonNilMachineState := make(state.NUMANodeMap, len(machineState))
	for numaID, numaState := range machineState {
		if numaState != nil {
			nonNilMachineState[numaID] = numaState
		}
	}
	nonReclaimableCPUSet := cpuutil.GetAggResourcePackagePinnedCPUSet(disableReclaimSelector, nonNilMachineState)

	dedicatedEligiblePerNUMA := make(map[int]machine.CPUSet, len(hintNodes))
	reclaimEligiblePerNUMA := make(map[int]machine.CPUSet, len(hintNodes))
	for _, hintedNUMAID := range hintNodes {
		numaID := int(hintedNUMAID)
		numaState := machineState[numaID]
		if numaState == nil {
			return nil, nil, fmt.Errorf("missing machine state for hinted NUMA %d", numaID)
		}
		scope := numaState.GetAvailableCPUSet(p.reservedCPUs)
		dedicatedEligiblePerNUMA[numaID] = advisorBlockOwnerEligible(
			commonstate.PoolNameDedicated,
			resourcePackageName,
			scope,
			allPinnedCPUs,
			rpPinnedCPUSet,
			nonReclaimableCPUSet,
		)
		reclaimEligiblePerNUMA[numaID] = advisorBlockOwnerEligible(
			commonstate.PoolNameReclaim,
			"",
			scope,
			allPinnedCPUs,
			rpPinnedCPUSet,
			nonReclaimableCPUSet,
		)
	}
	return dedicatedEligiblePerNUMA, reclaimEligiblePerNUMA, nil
}

// selectNumaBindingReclaimPartition preserves the mandatory reserve even when
// Pod EnableReclaim is false, while optional ratio capacity is retained only
// for reclaim-enabled Pods. Selection first consumes reclaim-only eligibility
// (G-D), then the dedicated/reclaim intersection, and never leaves the planner's
// reclaim eligibility (which excludes non-reclaimable pinned packages).
func (p *DynamicPolicy) selectNumaBindingReclaimPartition(
	derivedFloor machine.CPUSet,
	dedicatedEligiblePerNUMA, reclaimEligiblePerNUMA map[int]machine.CPUSet,
	hintNodes []uint64,
	podReclaimEnabled, coverExclusivePartition bool,
) (machine.CPUSet, error) {
	if !p.isRampUpReclaimHardPartitionEnabled() || !coverExclusivePartition {
		return derivedFloor, nil
	}

	hintedCPUs := machine.NewCPUSet()
	for _, numaID := range hintNodes {
		hintedCPUs = hintedCPUs.Union(p.machineInfo.CPUDetails.CPUsInNUMANodes(int(numaID)))
	}
	selected := derivedFloor.Difference(hintedCPUs)
	for numaID, reclaimEligible := range reclaimEligiblePerNUMA {
		dedicatedEligible := dedicatedEligiblePerNUMA[numaID]
		reclaimOnly := reclaimEligible.Difference(dedicatedEligible)
		base := reclaimOnly

		derivedInNUMA := derivedFloor.Intersection(
			p.machineInfo.CPUDetails.CPUsInNUMANodes(numaID),
		)
		reserveTarget := p.reservedReclaimedCPUSet.Intersection(derivedInNUMA).Size()
		target := reserveTarget
		if podReclaimEnabled {
			target = general.Max(target, derivedInNUMA.Size())
		}
		target = general.Max(target, base.Size())
		// Legacy checkpoints/tests may not carry an identity-bearing reserve.
		// Exclusive disjoint admission still needs a non-empty reclaim side;
		// retain the previously derived floor in that compatibility case.
		if target == 0 && !derivedInNUMA.IsEmpty() {
			target = derivedInNUMA.Size()
		}
		if target > reclaimEligible.Size() {
			return machine.NewCPUSet(), fmt.Errorf(
				"NUMA %d reclaim eligibility %d is smaller than required reserve %d",
				numaID, reclaimEligible.Size(), target)
		}

		selectedInNUMA := base
		remaining := target - selectedInNUMA.Size()
		if remaining > 0 {
			intersection := reclaimEligible.Intersection(dedicatedEligible).Difference(selectedInNUMA)
			preferred := derivedFloor.Union(p.reservedReclaimedCPUSet).Intersection(intersection)
			supplement, err := p.takeByTopologyPreferring(intersection, preferred, remaining)
			if err != nil {
				return machine.NewCPUSet(), fmt.Errorf(
					"select NUMA %d reclaim reserve from shared eligibility: %w", numaID, err)
			}
			selectedInNUMA = selectedInNUMA.Union(supplement)
		}
		selected = selected.Union(selectedInNUMA)
	}
	return selected, nil
}

// podEnableReclaim resolves the effective reclaim switch for a single pod,
// falling back to the node-level switch when the pod carries no override.
func (p *DynamicPolicy) podEnableReclaim(ctx context.Context, podUID string) (bool, error) {
	return resourcehelper.PodEnableReclaim(ctx, p.metaServer, podUID, p.isReclaimEnabled())
}

// podEnableReclaimOrFallback is the error-swallowing convenience wrapper used on
// allocation paths where a false fallback is the safe (non-hard-partition) choice.
func (p *DynamicPolicy) podEnableReclaimOrFallback(ctx context.Context, podUID, operation string) bool {
	podReclaimEnabled, err := p.podEnableReclaim(ctx, podUID)
	if err != nil {
		general.Warningf("%s: failed to check pod enable reclaim for pod %s, fallback podReclaimEnabled=false: %v",
			operation, podUID, err)
		return false
	}
	return podReclaimEnabled
}

// getInitialRampUpReclaimCPUSetRatio returns the configured initial ramp-up
// reclaim cpuset ratio ([0,1]); 0 means "reserve-only".
func (p *DynamicPolicy) getInitialRampUpReclaimCPUSetRatio() float64 {
	if p.dynamicConfig == nil {
		return 0
	}
	dyn := p.dynamicConfig.GetDynamicConfiguration()
	if dyn == nil {
		return 0
	}
	return dyn.InitialRampUpReclaimCPUSetRatio
}
