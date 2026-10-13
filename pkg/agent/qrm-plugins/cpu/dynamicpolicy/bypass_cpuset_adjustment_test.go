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

package dynamicpolicy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"

	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	coreconsts "github.com/kubewharf/katalyst-core/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

// makeCapabilityInfo builds an allocationInfo for the given QoS. When value is
// non-nil the public capability annotation is set to that exact string; when nil
// the key is absent (a legacy checkpoint).
func makeCapabilityInfo(qosLevel string, value *string) *state.AllocationInfo {
	req := &pluginapi.ResourceRequest{
		PodUid:        "pod-" + qosLevel,
		PodNamespace:  "default",
		PodName:       "pod-" + qosLevel,
		ContainerName: "main",
		Annotations:   map[string]string{apiconsts.PodAnnotationQoSLevelKey: qosLevel},
	}
	meta := commonstate.GenerateGenericContainerAllocationMeta(req, commonstate.EmptyOwnerPoolName, qosLevel)
	if value != nil {
		if meta.Annotations == nil {
			meta.Annotations = map[string]string{}
		}
		meta.Annotations[coreconsts.AllowBypassCPUSetAdjustmentAnnotationKey] = *value
	}
	return &state.AllocationInfo{AllocationMeta: meta}
}

func strptr(s string) *string { return &s }

// --- capability generation: recordBypassCapability labels QoS, never pool/fake ---

func TestRecordBypassCapability(t *testing.T) {
	t.Parallel()

	cases := []struct {
		qos  string
		want string
	}{
		{apiconsts.PodAnnotationQoSLevelSharedCores, bypassCapabilityTrue},
		{apiconsts.PodAnnotationQoSLevelReclaimedCores, bypassCapabilityTrue},
		{apiconsts.PodAnnotationQoSLevelSystemCores, bypassCapabilityTrue},
		{apiconsts.PodAnnotationQoSLevelDedicatedCores, bypassCapabilityFalse},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.qos, func(t *testing.T) {
			t.Parallel()
			info := makeCapabilityInfo(tc.qos, nil)
			recordBypassCapability(info)
			assert.Equal(t, tc.want, info.Annotations[coreconsts.AllowBypassCPUSetAdjustmentAnnotationKey])
		})
	}
}

// --- explicit values are preserved; invalid is normalized to false ---

func TestRecordBypassCapability_PreservesExplicitValue(t *testing.T) {
	t.Parallel()

	// A shared container that explicitly declared "false" must keep "false"
	// after normalization — record must NOT overwrite it with the QoS-derived "true".
	sharedFalse := makeCapabilityInfo(apiconsts.PodAnnotationQoSLevelSharedCores, strptr(bypassCapabilityFalse))
	recordBypassCapability(sharedFalse)
	assert.Equal(t, bypassCapabilityFalse, sharedFalse.Annotations[coreconsts.AllowBypassCPUSetAdjustmentAnnotationKey],
		"explicit false must survive normalization")

	// A shared container that explicitly declared "true" keeps "true".
	sharedTrue := makeCapabilityInfo(apiconsts.PodAnnotationQoSLevelSharedCores, strptr(bypassCapabilityTrue))
	recordBypassCapability(sharedTrue)
	assert.Equal(t, bypassCapabilityTrue, sharedTrue.Annotations[coreconsts.AllowBypassCPUSetAdjustmentAnnotationKey])

	// An invalid (non-strict) value is normalized to "false".
	sharedGarbage := makeCapabilityInfo(apiconsts.PodAnnotationQoSLevelSharedCores, strptr("maybe"))
	recordBypassCapability(sharedGarbage)
	assert.Equal(t, bypassCapabilityFalse, sharedGarbage.Annotations[coreconsts.AllowBypassCPUSetAdjustmentAnnotationKey],
		"invalid value must normalize to false")

	// Dedicated is always recorded as "false" even if a user set "true".
	dedicatedTrue := makeCapabilityInfo(apiconsts.PodAnnotationQoSLevelDedicatedCores, strptr(bypassCapabilityTrue))
	recordBypassCapability(dedicatedTrue)
	assert.Equal(t, bypassCapabilityFalse, dedicatedTrue.Annotations[coreconsts.AllowBypassCPUSetAdjustmentAnnotationKey],
		"dedicated must always be recorded as false")

	// Dedicated that already declared "false" stays "false".
	dedicatedFalse := makeCapabilityInfo(apiconsts.PodAnnotationQoSLevelDedicatedCores, strptr(bypassCapabilityFalse))
	recordBypassCapability(dedicatedFalse)
	assert.Equal(t, bypassCapabilityFalse, dedicatedFalse.Annotations[coreconsts.AllowBypassCPUSetAdjustmentAnnotationKey],
		"dedicated explicit false stays false")
}

// --- strict reads: invalid/missing fail closed to not-allowed ---

func TestBypassCapabilityAllow_StrictReads(t *testing.T) {
	t.Parallel()

	// Strict true -> allowed.
	assert.True(t, bypassCapabilityAllow(makeCapabilityInfo(apiconsts.PodAnnotationQoSLevelSharedCores, strptr(bypassCapabilityTrue))))

	// Strict false -> not allowed.
	assert.False(t, bypassCapabilityAllow(makeCapabilityInfo(apiconsts.PodAnnotationQoSLevelSharedCores, strptr(bypassCapabilityFalse))))

	// Missing key (legacy checkpoint) derives from QoS: shared is capable.
	assert.True(t, bypassCapabilityAllow(makeCapabilityInfo(apiconsts.PodAnnotationQoSLevelSharedCores, nil)))
	assert.False(t, bypassCapabilityAllow(makeCapabilityInfo(apiconsts.PodAnnotationQoSLevelDedicatedCores, nil)))

	// Invalid (non-strict) value fails closed.
	assert.False(t, bypassCapabilityAllow(makeCapabilityInfo(apiconsts.PodAnnotationQoSLevelSharedCores, strptr("maybe"))))

	// Dedicated with explicit "true" annotation still reports not-allowed,
	// because dedicated is never bypassable at the decision layer.
	assert.False(t, bypassCapabilityAllow(makeCapabilityInfo(apiconsts.PodAnnotationQoSLevelDedicatedCores, strptr(bypassCapabilityTrue))))
}

// --- nil safety: nil info / nil annotations must not panic ---

func TestBypassCapability_NilSafe(t *testing.T) {
	t.Parallel()

	assert.NotPanics(t, func() { recordBypassCapability(nil) })
	assert.False(t, bypassCapabilityAllow(nil), "nil info must not be allowed")

	// A shared info with a nil annotation map: record must allocate the map and
	// label it; allow must read it back without panicking.
	info := makeCapabilityInfo(apiconsts.PodAnnotationQoSLevelSharedCores, nil)
	info.Annotations = nil
	require.NotPanics(t, func() { recordBypassCapability(info) })
	require.NotNil(t, info.Annotations)
	assert.Equal(t, bypassCapabilityTrue, info.Annotations[coreconsts.AllowBypassCPUSetAdjustmentAnnotationKey])
	assert.True(t, bypassCapabilityAllow(info))
}

// --- effective decision = live switch AND recorded capability ---

func TestShouldBypassCPUSetAdjustmentForAllocation_LiveSwitch(t *testing.T) {
	t.Parallel()

	// capability=true (shared, labeled), live on -> bypass; live off -> no bypass.
	on := newPolicyForBypassTest(true)
	off := newPolicyForBypassTest(false)
	shared := makeCapabilityInfo(apiconsts.PodAnnotationQoSLevelSharedCores, strptr(bypassCapabilityTrue))

	assert.True(t, on.shouldBypassCPUSetAdjustmentForAllocation(shared), "live on + capable must bypass")
	assert.False(t, off.shouldBypassCPUSetAdjustmentForAllocation(shared), "live off must not bypass regardless of capability")

	// explicit false overrides live true: a shared entry that declared "false"
	// stays non-bypassing even when the live switch is on.
	nonCapable := makeCapabilityInfo(apiconsts.PodAnnotationQoSLevelSharedCores, strptr(bypassCapabilityFalse))
	assert.False(t, on.shouldBypassCPUSetAdjustmentForAllocation(nonCapable), "explicit false must override live true")
}

// --- sidecar inherits the capability from its main container ---

func TestBypassCapability_SidecarInheritsMain(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)

	main := &state.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{
			PodUid:        "sidecar-pod",
			PodName:       "sidecar-pod",
			ContainerName: "main",
			ContainerType: pluginapi.ContainerType_MAIN.String(),
			QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
		},
		AllocationResult:         machine.NewCPUSet(0),
		TopologyAwareAssignments: map[int]machine.CPUSet{0: machine.NewCPUSet(0)},
	}
	sidecar := &state.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{
			PodUid:        "sidecar-pod",
			PodName:       "sidecar-pod",
			ContainerName: "sidecar",
			ContainerType: pluginapi.ContainerType_SIDECAR.String(),
			QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
		},
		AllocationResult:         machine.NewCPUSet(0),
		TopologyAwareAssignments: map[int]machine.CPUSet{0: machine.NewCPUSet(0)},
	}
	recordBypassCapability(main)
	entries := state.PodEntries{"sidecar-pod": state.ContainerEntries{"main": main, "sidecar": sidecar}}

	p.syncCandidateSidecarsFromMain(entries)

	assert.Equal(t, main.Annotations[coreconsts.AllowBypassCPUSetAdjustmentAnnotationKey], sidecar.Annotations[coreconsts.AllowBypassCPUSetAdjustmentAnnotationKey],
		"sidecar must inherit the capability annotation from its main")
}

// --- response behavior: effective bypass clears X; live off keeps a non-empty X ---

func TestAllocateResponseKeepsCPUSetWhenLiveSwitchOff(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	// capability is recorded by allocation; live switch OFF must keep X.
	p.config.dynamicConfig.GetDynamicConfiguration().EnableBypassCPUSetAdjustment = false

	req := &pluginapi.ResourceRequest{
		PodUid:         "shared-pod",
		PodNamespace:   "default",
		PodName:        "shared-pod",
		ContainerName:  "main",
		ContainerType:  pluginapi.ContainerType_MAIN,
		ContainerIndex: 0,
		ResourceName:   string(v1.ResourceCPU),
		ResourceRequests: map[string]float64{
			string(v1.ResourceCPU): 2,
		},
		Labels: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores,
		},
		Annotations: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores,
		},
	}

	resp, err := p.Allocate(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	cpuInfo := resp.AllocationResult.ResourceAllocation[string(v1.ResourceCPU)]
	require.NotNil(t, cpuInfo)
	assert.NotEmpty(t, cpuInfo.AllocationResult, "live off must keep the non-empty response cpuset (X)")
}

// --- end-to-end: explicit allow=false on a shared container must survive a real
// Allocate/commit and override the live EnableBypassCPUSetAdjustment=true switch. ---

func TestAllocateResponse_ExplicitFalseShared_OverridesLiveTrue(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	// Live switch ON.
	p.config.dynamicConfig.GetDynamicConfiguration().EnableBypassCPUSetAdjustment = true

	req := &pluginapi.ResourceRequest{
		PodUid:         "shared-pod-explicit-false",
		PodNamespace:   "default",
		PodName:        "shared-pod-explicit-false",
		ContainerName:  "main",
		ContainerType:  pluginapi.ContainerType_MAIN,
		ContainerIndex: 0,
		ResourceName:   string(v1.ResourceCPU),
		ResourceRequests: map[string]float64{
			string(v1.ResourceCPU): 2,
		},
		Labels: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores,
		},
		Annotations: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey:                  apiconsts.PodAnnotationQoSLevelSharedCores,
			coreconsts.AllowBypassCPUSetAdjustmentAnnotationKey: bypassCapabilityFalse,
		},
	}

	resp, err := p.Allocate(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	cpuInfo := resp.AllocationResult.ResourceAllocation[string(v1.ResourceCPU)]
	require.NotNil(t, cpuInfo)
	// Explicit false means no bypass: the response cpuset (X) must remain non-empty.
	assert.NotEmpty(t, cpuInfo.AllocationResult, "explicit false must keep X non-empty even with live=true")

	// The persisted state must carry the explicit false (not overwritten to true).
	ai := p.state.GetAllocationInfo("shared-pod-explicit-false", "main")
	require.NotNil(t, ai)
	assert.Equal(t, bypassCapabilityFalse, ai.Annotations[coreconsts.AllowBypassCPUSetAdjustmentAnnotationKey],
		"persisted state must preserve explicit false")

	// GetResourcesAllocation must also respect it: X stays non-empty.
	gra, err := p.GetResourcesAllocation(context.Background(), &pluginapi.GetResourcesAllocationRequest{})
	require.NoError(t, err)
	graCPU := gra.PodResources["shared-pod-explicit-false"].ContainerResources["main"].ResourceAllocation[string(v1.ResourceCPU)]
	assert.NotEmpty(t, graCPU.AllocationResult, "GetResourcesAllocation must keep X non-empty for explicit false")
}

// --- end-to-end: a shared container WITHOUT explicit annotation, live=true, must
// bypass (X cleared) after a real Allocate. ---

func TestAllocateResponse_DefaultShared_BypassesWhenLiveTrue(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.config.dynamicConfig.GetDynamicConfiguration().EnableBypassCPUSetAdjustment = true

	req := &pluginapi.ResourceRequest{
		PodUid:         "shared-pod-default",
		PodNamespace:   "default",
		PodName:        "shared-pod-default",
		ContainerName:  "main",
		ContainerType:  pluginapi.ContainerType_MAIN,
		ContainerIndex: 0,
		ResourceName:   string(v1.ResourceCPU),
		ResourceRequests: map[string]float64{
			string(v1.ResourceCPU): 2,
		},
		Labels: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores,
		},
		Annotations: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores,
		},
	}

	resp, err := p.Allocate(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	cpuInfo := resp.AllocationResult.ResourceAllocation[string(v1.ResourceCPU)]
	require.NotNil(t, cpuInfo)
	assert.Empty(t, cpuInfo.AllocationResult, "default shared + live=true must bypass (X cleared)")
}

// --- end-to-end: an invalid (non-strict) annotation value fails closed: even with
// live=true the response cpuset (X) must stay non-empty. ---

func TestAllocateResponse_InvalidValue_FailsClosed(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.config.dynamicConfig.GetDynamicConfiguration().EnableBypassCPUSetAdjustment = true

	req := &pluginapi.ResourceRequest{
		PodUid:         "shared-pod-invalid",
		PodNamespace:   "default",
		PodName:        "shared-pod-invalid",
		ContainerName:  "main",
		ContainerType:  pluginapi.ContainerType_MAIN,
		ContainerIndex: 0,
		ResourceName:   string(v1.ResourceCPU),
		ResourceRequests: map[string]float64{
			string(v1.ResourceCPU): 2,
		},
		Labels: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores,
		},
		Annotations: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey:                  apiconsts.PodAnnotationQoSLevelSharedCores,
			coreconsts.AllowBypassCPUSetAdjustmentAnnotationKey: "maybe",
		},
	}

	resp, err := p.Allocate(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	cpuInfo := resp.AllocationResult.ResourceAllocation[string(v1.ResourceCPU)]
	require.NotNil(t, cpuInfo)
	assert.NotEmpty(t, cpuInfo.AllocationResult, "invalid value must fail closed (X non-empty) even with live=true")

	ai := p.state.GetAllocationInfo("shared-pod-invalid", "main")
	require.NotNil(t, ai)
	assert.Equal(t, bypassCapabilityFalse, ai.Annotations[coreconsts.AllowBypassCPUSetAdjustmentAnnotationKey],
		"invalid value must be normalized to false in persisted state")
}

// TestGetResourcesAllocationKeepsCPUSetWhenLiveSwitchOff is the unique direct
// SetAllocationInfo → GetResourcesAllocation path: it bypasses Allocate and seeds
// state directly, verifying that with live=false the response cpuset (X) is not
// cleared for an already-persisted entry.
func TestGetResourcesAllocationKeepsCPUSetWhenLiveSwitchOff(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.config.dynamicConfig.GetDynamicConfiguration().EnableBypassCPUSetAdjustment = false

	cpus := machine.NewCPUSet(2, 3)
	info := &state.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{
			PodUid:        "shared-pod",
			PodName:       "shared-pod",
			ContainerName: "main",
			QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
		},
		AllocationResult:         cpus,
		OriginalAllocationResult: cpus.Clone(),
		TopologyAwareAssignments: map[int]machine.CPUSet{0: cpus},
		RequestQuantity:          float64(cpus.Size()),
	}
	p.state.SetAllocationInfo("shared-pod", "main", info, false)

	resp, err := p.GetResourcesAllocation(context.Background(), &pluginapi.GetResourcesAllocationRequest{})
	require.NoError(t, err)
	cpuInfo := resp.PodResources["shared-pod"].ContainerResources["main"].ResourceAllocation[string(v1.ResourceCPU)]
	assert.NotEmpty(t, cpuInfo.AllocationResult, "live off must not clear a non-empty X")
}

// TestGetResourcesAllocation_LiveFlipSameEntry proves the gating is a live decision
// layered on top of an unchanged persisted allocation: flipping the live switch
// false->true clears X on the next response, and flipping it true->false restores X,
// without rewriting the persisted entry either way.
func TestGetResourcesAllocation_LiveFlipSameEntry(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)

	cpus := machine.NewCPUSet(2, 3)
	info := &state.AllocationInfo{
		AllocationMeta: commonstate.AllocationMeta{
			PodUid:        "flip-pod",
			PodName:       "flip-pod",
			ContainerName: "main",
			QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
		},
		AllocationResult:         cpus,
		OriginalAllocationResult: cpus.Clone(),
		TopologyAwareAssignments: map[int]machine.CPUSet{0: cpus},
		RequestQuantity:          float64(cpus.Size()),
	}
	// Record the capability up front so the flip is purely about the live switch.
	recordBypassCapability(info)
	p.state.SetAllocationInfo("flip-pod", "main", info, false)

	getX := func() string {
		t.Helper()
		resp, err := p.GetResourcesAllocation(context.Background(), &pluginapi.GetResourcesAllocationRequest{})
		require.NoError(t, err)
		return resp.PodResources["flip-pod"].ContainerResources["main"].ResourceAllocation[string(v1.ResourceCPU)].AllocationResult
	}

	// live off -> X kept
	p.config.dynamicConfig.GetDynamicConfiguration().EnableBypassCPUSetAdjustment = false
	assert.NotEmpty(t, getX(), "live off must keep X non-empty")

	// live on -> X cleared (same persisted entry)
	p.config.dynamicConfig.GetDynamicConfiguration().EnableBypassCPUSetAdjustment = true
	assert.Empty(t, getX(), "live on must clear X for a capable entry")

	// live off again -> X restored
	p.config.dynamicConfig.GetDynamicConfiguration().EnableBypassCPUSetAdjustment = false
	assert.NotEmpty(t, getX(), "live off again must restore X")

	// The response gating only rewrites the response view: the recorded capability
	// annotation on the persisted entry is untouched by the live flips.
	persisted := p.state.GetAllocationInfo("flip-pod", "main")
	require.NotNil(t, persisted)
	assert.Equal(t, bypassCapabilityTrue, persisted.Annotations[coreconsts.AllowBypassCPUSetAdjustmentAnnotationKey],
		"recorded capability must survive live flips")
}

// TestAllocate_DefaultKeepsAllowKey_FiltersRandomAnnotation pins the admission-time
// annotation filter: with the default kept-annotation baseline, the public allow
// annotation survives into the persisted allocation, while a random, non-kept
// annotation is still stripped. This proves the behavior moved from a per-call-site
// helper into the options default without regressing the filter.
func TestAllocate_DefaultKeepsAllowKey_FiltersRandomAnnotation(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(16, 2, 4)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithInitialization(topology, t.TempDir())
	require.NoError(t, err)
	// Live switch off so the response cpuset behavior is irrelevant to this assertion.
	p.config.dynamicConfig.GetDynamicConfiguration().EnableBypassCPUSetAdjustment = false

	const randomAnnoKey = "random.example.com/not-a-kept-key"

	req := &pluginapi.ResourceRequest{
		PodUid:         "shared-pod-filter",
		PodNamespace:   "default",
		PodName:        "shared-pod-filter",
		ContainerName:  "main",
		ContainerType:  pluginapi.ContainerType_MAIN,
		ContainerIndex: 0,
		ResourceName:   string(v1.ResourceCPU),
		ResourceRequests: map[string]float64{
			string(v1.ResourceCPU): 2,
		},
		Labels: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey: apiconsts.PodAnnotationQoSLevelSharedCores,
		},
		Annotations: map[string]string{
			apiconsts.PodAnnotationQoSLevelKey:                  apiconsts.PodAnnotationQoSLevelSharedCores,
			coreconsts.AllowBypassCPUSetAdjustmentAnnotationKey: bypassCapabilityTrue,
			randomAnnoKey: "should-be-stripped",
		},
	}

	resp, err := p.Allocate(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	ai := p.state.GetAllocationInfo("shared-pod-filter", "main")
	require.NotNil(t, ai)
	require.NotNil(t, ai.Annotations)

	// The public allow key is kept by the default baseline (and normalized to true).
	assert.Contains(t, ai.Annotations, coreconsts.AllowBypassCPUSetAdjustmentAnnotationKey,
		"default baseline must keep the public allow annotation")
	assert.Equal(t, bypassCapabilityTrue, ai.Annotations[coreconsts.AllowBypassCPUSetAdjustmentAnnotationKey])

	// A random, non-kept annotation must be filtered out at admission.
	assert.NotContains(t, ai.Annotations, randomAnnoKey,
		"random non-kept annotation must not survive the admission filter")
}
