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

package region

import (
	"context"
	"errors"
	"math"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"

	configapi "github.com/kubewharf/katalyst-api/pkg/apis/config/v1alpha1"
	workloadapis "github.com/kubewharf/katalyst-api/pkg/apis/workload/v1alpha1"
	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	katalyst_base "github.com/kubewharf/katalyst-core/cmd/base"
	"github.com/kubewharf/katalyst-core/cmd/katalyst-agent/app/options"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/metacache"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/types"
	"github.com/kubewharf/katalyst-core/pkg/config"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	"github.com/kubewharf/katalyst-core/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent"
	agentmetric "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/spd"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	metricspool "github.com/kubewharf/katalyst-core/pkg/metrics/metrics-pool"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	utilmetric "github.com/kubewharf/katalyst-core/pkg/util/metric"
	"github.com/kubewharf/katalyst-core/pkg/util/reclaim"
)

type dedicatedIndicatorFixture struct {
	conf      *config.Configuration
	cache     metacache.MetaCache
	fetcher   *agentmetric.FakeMetricsFetcher
	region    *QoSRegionDedicated
	container *types.ContainerInfo
}

func newDedicatedIndicatorFixture(
	t *testing.T,
	numaID int,
	assignments types.TopologyAwareAssignment,
) *dedicatedIndicatorFixture {
	t.Helper()

	conf, err := options.NewOptions().Config()
	require.NoError(t, err)
	conf.CPUAdvisorConfiguration.ProvisionPolicies = nil
	conf.CPUAdvisorConfiguration.HeadroomPolicies = nil

	cache := metacache.NewDummyMetaCacheImp()
	fetcher := agentmetric.NewFakeMetricsFetcher(metrics.DummyMetrics{}).(*agentmetric.FakeMetricsFetcher)
	metaServer := &metaserver.MetaServer{
		MetaAgent: &agent.MetaAgent{
			MetricsFetcher: fetcher,
			PodFetcher: &pod.PodFetcherStub{PodList: []*v1.Pod{{
				ObjectMeta: metav1.ObjectMeta{UID: "pod"},
			}}},
		},
		ServiceProfilingManager: &spd.DummyServiceProfilingManager{},
	}
	container := &types.ContainerInfo{
		PodUID:                   "pod",
		ContainerName:            "main",
		OwnerPoolName:            "dedicated-pool",
		OriginOwnerPoolName:      "dedicated-pool",
		QoSLevel:                 apiconsts.PodAnnotationQoSLevelDedicatedCores,
		TopologyAwareAssignments: assignments,
	}
	require.NoError(t, cache.SetContainerInfo(container.PodUID, container.ContainerName, container))

	r := NewQoSRegionDedicated(
		container,
		conf,
		numaID,
		nil,
		cache,
		metaServer,
		metrics.DummyMetrics{},
	).(*QoSRegionDedicated)
	require.NoError(t, r.AddContainer(container))

	return &dedicatedIndicatorFixture{
		conf:      conf,
		cache:     cache,
		fetcher:   fetcher,
		region:    r,
		container: container,
	}
}

func TestNewQoSRegionDedicatedRegistersSchedWaitGetter(t *testing.T) {
	t.Parallel()

	f := newDedicatedIndicatorFixture(t, 0, types.TopologyAwareAssignment{
		0: machine.NewCPUSet(0, 1),
	})

	require.Contains(t, f.region.indicatorCurrentGetters,
		string(workloadapis.ServiceSystemIndicatorNameCPUSchedWait))
}

func TestDefaultDedicatedIndicatorsDoNotEnableSchedWait(t *testing.T) {
	t.Parallel()

	conf, err := options.NewOptions().Config()
	require.NoError(t, err)

	for _, indicator := range conf.GetDynamicConfiguration().
		RegionIndicatorTargetConfiguration[configapi.QoSRegionTypeDedicated] {
		require.NotEqual(t, workloadapis.ServiceSystemIndicatorNameCPUSchedWait, indicator.Name)
	}
}

func TestQoSRegionDedicatedAssignedCPUSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		numaID      int
		assignments types.TopologyAwareAssignment
		want        machine.CPUSet
	}{
		{
			name:   "numa binding uses only current sibling",
			numaID: 0,
			assignments: types.TopologyAwareAssignment{
				0: machine.NewCPUSet(0, 1),
				1: machine.NewCPUSet(2, 3),
			},
			want: machine.NewCPUSet(0, 1),
		},
		{
			name:   "non binding uses all assigned cpus",
			numaID: commonstate.FakedNUMAID,
			assignments: types.TopologyAwareAssignment{
				0: machine.NewCPUSet(0, 1),
				1: machine.NewCPUSet(2, 3),
			},
			want: machine.NewCPUSet(0, 1, 2, 3),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newDedicatedIndicatorFixture(t, tt.numaID, tt.assignments)

			got, err := f.region.getAssignedCPUSet()

			require.NoError(t, err)
			require.True(t, tt.want.Equals(got), "want %s, got %s", tt.want.String(), got.String())
		})
	}
}

func TestQoSRegionDedicatedAssignedCPUSetUnavailable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(t *testing.T, f *dedicatedIndicatorFixture)
	}{
		{
			name: "missing container info",
			setup: func(t *testing.T, f *dedicatedIndicatorFixture) {
				sidecar := &types.ContainerInfo{
					PodUID:              f.container.PodUID,
					ContainerName:       "sidecar",
					OwnerPoolName:       f.container.OwnerPoolName,
					OriginOwnerPoolName: f.container.OriginOwnerPoolName,
					QoSLevel:            f.container.QoSLevel,
					TopologyAwareAssignments: types.TopologyAwareAssignment{
						0: machine.NewCPUSet(1),
					},
				}
				require.NoError(t, f.cache.SetContainerInfo(sidecar.PodUID, sidecar.ContainerName, sidecar))
				require.NoError(t, f.region.AddContainer(sidecar))
				require.NoError(t, f.cache.DeleteContainer(f.container.PodUID, f.container.ContainerName))
			},
		},
		{
			name: "binding numa has no assigned cpus",
			setup: func(_ *testing.T, f *dedicatedIndicatorFixture) {
				f.region.bindingNumas = machine.NewCPUSet(1)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newDedicatedIndicatorFixture(t, 0, types.TopologyAwareAssignment{
				0: machine.NewCPUSet(0),
			})
			tt.setup(t, f)

			_, err := f.region.getAssignedCPUSet()
			require.ErrorIs(t, err, errIndicatorUnavailable)

			dynamicConf := f.conf.GetDynamicConfiguration()
			dynamicConf.RegionIndicatorTargetConfiguration = map[configapi.QoSRegionType][]configapi.IndicatorTargetConfiguration{
				configapi.QoSRegionTypeDedicated: {
					{Name: workloadapis.ServiceSystemIndicatorNameCPUSchedWait, Target: 460},
					{Name: workloadapis.ServiceSystemIndicatorNameCPUUsageRatio, Target: 0.55},
				},
			}
			f.region.SetEssentials(types.ResourceEssentials{DynamicConfiguration: dynamicConf})
			f.region.indicatorCurrentGetters[string(workloadapis.ServiceSystemIndicatorNameCPUUsageRatio)] = func() (float64, error) { return 0.5, nil }

			indicators, err := f.region.getIndicators()
			require.NoError(t, err)
			require.NotContains(t, indicators, string(workloadapis.ServiceSystemIndicatorNameCPUSchedWait))
			require.Equal(t, types.IndicatorValue{Current: 0.5, Target: 0.55},
				indicators[string(workloadapis.ServiceSystemIndicatorNameCPUUsageRatio)])
		})
	}
}

func TestQoSRegionDedicatedAssignedCPUSetUsesLatestNonBindingAssignments(t *testing.T) {
	t.Parallel()

	f := newDedicatedIndicatorFixture(t, commonstate.FakedNUMAID, types.TopologyAwareAssignment{
		0: machine.NewCPUSet(0),
	})
	container, ok := f.cache.GetContainerInfo(f.container.PodUID, f.container.ContainerName)
	require.True(t, ok)
	container.TopologyAwareAssignments = types.TopologyAwareAssignment{
		1: machine.NewCPUSet(2, 3),
	}
	require.NoError(t, f.cache.SetContainerInfo(container.PodUID, container.ContainerName, container))

	got, err := f.region.getAssignedCPUSet()

	require.NoError(t, err)
	require.True(t, machine.NewCPUSet(2, 3).Equals(got), "got %s", got.String())
}

func TestQoSRegionDedicatedSchedWaitUsesSiblingCPUSet(t *testing.T) {
	t.Parallel()

	assignments := types.TopologyAwareAssignment{
		0: machine.NewCPUSet(0, 1),
		1: machine.NewCPUSet(2, 3),
	}
	numa0 := newDedicatedIndicatorFixture(t, 0, assignments)
	numa1 := newDedicatedIndicatorFixture(t, 1, assignments)
	now := time.Now()
	for cpuID, value := range map[int]float64{
		0: 10,
		1: 30,
		2: 50,
		3: 70,
	} {
		numa0.fetcher.SetCPUMetric(
			cpuID,
			consts.MetricCPUSchedwait,
			utilmetric.MetricData{Value: value, Time: &now},
		)
		numa1.fetcher.SetCPUMetric(
			cpuID,
			consts.MetricCPUSchedwait,
			utilmetric.MetricData{Value: value, Time: &now},
		)
	}

	numa0Value, err := numa0.region.getCPUSchedWait()
	require.NoError(t, err)
	numa1Value, err := numa1.region.getCPUSchedWait()
	require.NoError(t, err)

	require.Equal(t, 20.0, numa0Value)
	require.Equal(t, 60.0, numa1Value)
}

func TestQoSRegionDedicatedCPUUsageUsesAssignedCPUSet(t *testing.T) {
	t.Parallel()

	assignments := types.TopologyAwareAssignment{
		0: machine.NewCPUSet(0, 1),
		1: machine.NewCPUSet(2, 3),
	}
	numa0 := newDedicatedIndicatorFixture(t, 0, assignments)
	nonBinding := newDedicatedIndicatorFixture(t, commonstate.FakedNUMAID, assignments)
	now := time.Now()
	for cpuID, value := range map[int]float64{
		0: 0.1,
		1: 0.3,
		2: 0.5,
		3: 0.7,
	} {
		numa0.fetcher.SetCPUMetric(
			cpuID,
			consts.MetricCPUUsageRatio,
			utilmetric.MetricData{Value: value, Time: &now},
		)
		nonBinding.fetcher.SetCPUMetric(
			cpuID,
			consts.MetricCPUUsageRatio,
			utilmetric.MetricData{Value: value, Time: &now},
		)
	}

	numa0Value, err := numa0.region.getCPUUsageRatio()
	require.NoError(t, err)
	nonBindingValue, err := nonBinding.region.getCPUUsageRatio()
	require.NoError(t, err)

	require.InDelta(t, 0.2, numa0Value, 1e-9)
	require.InDelta(t, 0.4, nonBindingValue, 1e-9)
}

func TestQoSRegionDedicatedSchedWaitDeduplicatesContainerCPUAssignments(t *testing.T) {
	t.Parallel()

	assignments := types.TopologyAwareAssignment{
		0: machine.NewCPUSet(0, 1),
	}
	f := newDedicatedIndicatorFixture(t, 0, assignments)
	sidecar := &types.ContainerInfo{
		PodUID:              f.container.PodUID,
		ContainerName:       "sidecar",
		OwnerPoolName:       f.container.OwnerPoolName,
		OriginOwnerPoolName: f.container.OriginOwnerPoolName,
		QoSLevel:            f.container.QoSLevel,
		TopologyAwareAssignments: types.TopologyAwareAssignment{
			0: machine.NewCPUSet(1, 2),
		},
	}
	require.NoError(t, f.cache.SetContainerInfo(sidecar.PodUID, sidecar.ContainerName, sidecar))
	require.NoError(t, f.region.AddContainer(sidecar))

	now := time.Now()
	f.fetcher.SetCPUMetric(0, consts.MetricCPUSchedwait, utilmetric.MetricData{Value: 10, Time: &now})
	f.fetcher.SetCPUMetric(1, consts.MetricCPUSchedwait, utilmetric.MetricData{Value: 30, Time: &now})
	f.fetcher.SetCPUMetric(2, consts.MetricCPUSchedwait, utilmetric.MetricData{Value: 80, Time: &now})

	got, err := f.region.getCPUSchedWait()

	require.NoError(t, err)
	require.Equal(t, 40.0, got)
}

func TestQoSRegionDedicatedGetIndicatorsRejectsAllUnavailable(t *testing.T) {
	t.Parallel()

	f := newDedicatedIndicatorFixture(t, 0, types.TopologyAwareAssignment{
		0: machine.NewCPUSet(0),
	})
	dynamicConf := f.conf.GetDynamicConfiguration()
	dynamicConf.RegionIndicatorTargetConfiguration = map[configapi.QoSRegionType][]configapi.IndicatorTargetConfiguration{
		configapi.QoSRegionTypeDedicated: {
			{Name: workloadapis.ServiceSystemIndicatorNameCPUSchedWait, Target: 460},
		},
	}
	f.region.SetEssentials(types.ResourceEssentials{DynamicConfiguration: dynamicConf})

	indicators, err := f.region.getIndicators()

	require.ErrorIs(t, err, errIndicatorUnavailable)
	require.Nil(t, indicators)
}

func TestQoSRegionDedicatedGetIndicatorsIsolatesUnavailableSchedWait(t *testing.T) {
	t.Parallel()

	f := newDedicatedIndicatorFixture(t, 0, types.TopologyAwareAssignment{
		0: machine.NewCPUSet(0),
	})
	dynamicConf := f.conf.GetDynamicConfiguration()
	dynamicConf.RegionIndicatorTargetConfiguration = map[configapi.QoSRegionType][]configapi.IndicatorTargetConfiguration{
		configapi.QoSRegionTypeDedicated: {
			{Name: workloadapis.ServiceSystemIndicatorNameCPUSchedWait, Target: 460},
			{Name: workloadapis.ServiceSystemIndicatorNameCPUUsageRatio, Target: 0.55},
		},
	}
	dynamicConf.IndicatorTargetDefaultGetter = "test-default"
	f.region.SetEssentials(types.ResourceEssentials{DynamicConfiguration: dynamicConf})
	f.region.indicatorTargetGetters["test-default"] = func(
		_ workloadapis.ServiceSystemIndicatorName,
		target float64,
	) float64 {
		return target
	}
	f.region.indicatorCurrentGetters[string(workloadapis.ServiceSystemIndicatorNameCPUUsageRatio)] = func() (float64, error) { return 0.5, nil }

	indicators, err := f.region.getIndicators()
	require.NoError(t, err)
	require.NotContains(t, indicators, string(workloadapis.ServiceSystemIndicatorNameCPUSchedWait))
	require.Equal(t, types.IndicatorValue{Current: 0.5, Target: 0.55},
		indicators[string(workloadapis.ServiceSystemIndicatorNameCPUUsageRatio)])

	now := time.Now()
	f.fetcher.SetCPUMetric(0, consts.MetricCPUSchedwait, utilmetric.MetricData{
		Value: 0,
		Time:  &now,
	})

	indicators, err = f.region.getIndicators()
	require.NoError(t, err)
	require.Equal(t, types.IndicatorValue{Current: 0, Target: 460},
		indicators[string(workloadapis.ServiceSystemIndicatorNameCPUSchedWait)])
}

func TestQoSRegionDedicatedGetIndicatorsRejectsInvalidSchedWait(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		current float64
	}{
		{name: "negative", current: -1},
		{name: "nan", current: math.NaN()},
		{name: "positive infinity", current: math.Inf(1)},
		{name: "negative infinity", current: math.Inf(-1)},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newDedicatedIndicatorFixture(t, 0, types.TopologyAwareAssignment{
				0: machine.NewCPUSet(0),
			})
			dynamicConf := f.conf.GetDynamicConfiguration()
			dynamicConf.RegionIndicatorTargetConfiguration = map[configapi.QoSRegionType][]configapi.IndicatorTargetConfiguration{
				configapi.QoSRegionTypeDedicated: {
					{Name: workloadapis.ServiceSystemIndicatorNameCPUSchedWait, Target: 460},
				},
			}
			dynamicConf.IndicatorTargetDefaultGetter = "test-default"
			f.region.SetEssentials(types.ResourceEssentials{DynamicConfiguration: dynamicConf})
			f.region.indicatorTargetGetters["test-default"] = func(
				_ workloadapis.ServiceSystemIndicatorName,
				target float64,
			) float64 {
				return target
			}
			f.region.indicatorCurrentGetters[string(workloadapis.ServiceSystemIndicatorNameCPUSchedWait)] = func() (float64, error) { return tt.current, nil }

			indicators, err := f.region.getIndicators()

			require.NoError(t, err)
			require.NotContains(t, indicators, string(workloadapis.ServiceSystemIndicatorNameCPUSchedWait))
		})
	}
}

func TestQoSRegionDedicatedGetIndicatorsPreservesUnexpectedErrors(t *testing.T) {
	t.Parallel()

	f := newDedicatedIndicatorFixture(t, 0, types.TopologyAwareAssignment{
		0: machine.NewCPUSet(0),
	})
	dynamicConf := f.conf.GetDynamicConfiguration()
	dynamicConf.RegionIndicatorTargetConfiguration = map[configapi.QoSRegionType][]configapi.IndicatorTargetConfiguration{
		configapi.QoSRegionTypeDedicated: {
			{Name: workloadapis.ServiceSystemIndicatorNameCPUSchedWait, Target: 460},
		},
	}
	f.region.SetEssentials(types.ResourceEssentials{DynamicConfiguration: dynamicConf})

	wantErr := errors.New("unexpected getter failure")
	f.region.indicatorCurrentGetters[string(workloadapis.ServiceSystemIndicatorNameCPUSchedWait)] = func() (float64, error) { return 0, wantErr }

	indicators, err := f.region.getIndicators()

	require.ErrorIs(t, err, wantErr)
	require.Nil(t, indicators)
}

func TestQoSRegionBaseGetIndicatorsPreservesShareSchedWaitZeroBehavior(t *testing.T) {
	t.Parallel()

	f := newDedicatedIndicatorFixture(t, 0, types.TopologyAwareAssignment{
		0: machine.NewCPUSet(0),
	})
	f.region.regionType = configapi.QoSRegionTypeShare
	dynamicConf := f.conf.GetDynamicConfiguration()
	dynamicConf.RegionIndicatorTargetConfiguration = map[configapi.QoSRegionType][]configapi.IndicatorTargetConfiguration{
		configapi.QoSRegionTypeShare: {
			{Name: workloadapis.ServiceSystemIndicatorNameCPUSchedWait, Target: 460},
		},
	}
	dynamicConf.IndicatorTargetDefaultGetter = "test-default"
	f.region.SetEssentials(types.ResourceEssentials{DynamicConfiguration: dynamicConf})
	f.region.indicatorTargetGetters["test-default"] = func(
		_ workloadapis.ServiceSystemIndicatorName,
		target float64,
	) float64 {
		return target
	}
	f.region.indicatorCurrentGetters[string(workloadapis.ServiceSystemIndicatorNameCPUSchedWait)] = func() (float64, error) { return 0, nil }

	indicators, err := f.region.getIndicators()

	require.NoError(t, err)
	require.NotContains(t, indicators, string(workloadapis.ServiceSystemIndicatorNameCPUSchedWait))
}

// newDedicatedRestrictFixture builds a QoSRegionDedicated wired like production
// so that the exact -> bounded ordering can be exercised end-to-end.
func newDedicatedRestrictFixture(t *testing.T) *QoSRegionDedicated {
	conf, err := options.NewOptions().Config()
	require.NoError(t, err)
	stateFileDir := "stateFileDir" + uuid.New().String()
	checkpointDir := "checkpointDir" + uuid.New().String()
	conf.GenericSysAdvisorConfiguration.StateFileDirectory = stateFileDir
	conf.MetaServerConfiguration.CheckpointManagerDir = checkpointDir
	t.Cleanup(func() { os.RemoveAll(stateFileDir); os.RemoveAll(checkpointDir) })
	conf.ExactRestrictRefPolicy = map[types.CPUProvisionPolicyName]types.CPUProvisionPolicyName{
		types.CPUProvisionPolicyRama: types.CPUProvisionPolicyDynamicQuota,
	}

	genericCtx, err := katalyst_base.GenerateFakeGenericContext([]runtime.Object{})
	require.NoError(t, err)
	reclaim.UnregisterConsumer(reclaim.GenericConsumerName)
	t.Cleanup(func() { reclaim.UnregisterConsumer(reclaim.GenericConsumerName) })
	metaServer, err := metaserver.NewMetaServer(genericCtx.Client, metrics.DummyMetrics{}, conf)
	require.NoError(t, err)
	metaCache, err := metacache.NewMetaCacheImp(conf, metricspool.DummyMetricsEmitterPool{}, agentmetric.NewFakeMetricsFetcher(metrics.DummyMetrics{}))
	require.NoError(t, err)
	ci := types.ContainerInfo{QoSLevel: apiconsts.PodAnnotationQoSLevelDedicatedCores, RegionNames: sets.NewString("dedicated")}
	return NewQoSRegionDedicated(&ci, conf, commonstate.FakedNUMAID, nil, metaCache, metaServer, metrics.DummyMetrics{}).(*QoSRegionDedicated)
}

func f64ptrBool(v bool) *bool { return &v }

// fakeProvisionPolicy is a ProvisionPolicy stand-in that returns a fixed control
// knob map. It lets TryUpdateProvision run end-to-end without real policy logic.
type fakeProvisionPolicy struct {
	name types.CPUProvisionPolicyName
	knob types.ControlKnob
}

func (f *fakeProvisionPolicy) SetEssentials(types.ResourceEssentials, types.ControlEssentials) {}
func (f *fakeProvisionPolicy) SetPodSet(types.PodSet)                                          {}
func (f *fakeProvisionPolicy) SetBindingNumas(machine.CPUSet, bool)                            {}
func (f *fakeProvisionPolicy) Update() error                                                   { return nil }
func (f *fakeProvisionPolicy) GetMetaInfo() string                                             { return string(f.name) }
func (f *fakeProvisionPolicy) GetControlKnobAdjusted() (types.ControlKnob, error) {
	return f.knob.Clone(), nil
}

func injectFakeProvisionPolicies(t *testing.T, r *QoSRegionDedicated, knobs map[types.CPUProvisionPolicyName]types.ControlKnob) {
	t.Helper()
	pols := make([]*internalProvisionPolicy, 0, len(knobs))
	for name, knob := range knobs {
		pols = append(pols, &internalProvisionPolicy{
			name:                name,
			policy:              &fakeProvisionPolicy{name: name, knob: knob},
			internalPolicyState: internalPolicyState{updateStatus: types.PolicyUpdateSucceeded},
		})
	}
	r.provisionPolicies = pols
}

// TestDedicatedTryUpdateProvisionExactBoundedOrder calls the production
// QoSRegionDedicated.TryUpdateProvision and asserts the exact -> bounded order
// end-to-end through provisionPolicyResults. Uses a custom knob (dummy
// regulator) for the bounded side so regulator behavior does not mask the clamp.
func TestDedicatedTryUpdateProvisionExactBoundedOrder(t *testing.T) {
	const (
		rama      = types.CPUProvisionPolicyRama
		canon     = types.CPUProvisionPolicyCanonical
		dynP      = types.CPUProvisionPolicyDynamicQuota
		quotaKnob = configapi.ControlKnobReclaimedCoresCPUQuota
		testKnob  = configapi.ControlKnobName("test-bounded-knob")
	)

	r := newDedicatedRestrictFixture(t)
	// Inject fake provision policies returning fixed knobs.
	injectFakeProvisionPolicies(t, r, map[types.CPUProvisionPolicyName]types.ControlKnob{
		rama:  {testKnob: ramaRestrictKnob(30), quotaKnob: ramaRestrictKnob(20)},
		canon: {testKnob: ramaRestrictKnob(10)},
		dynP:  {quotaKnob: ramaRestrictKnob(12)},
	})

	// No bounded constraint: exact path clamps quota 20->12; testKnob stays raw.
	r.DynamicConfiguration = dynamic.NewConfiguration()
	r.TryUpdateProvision()
	res := r.provisionPolicyResults[rama].getControlKnob()
	assert.Equal(t, 12.0, res[quotaKnob].Value)
	assert.Equal(t, 30.0, res[testKnob].Value)

	// Enforce bounded rule on non-exact-owned testKnob: 30->12; quota still 12.
	dyn := dynamic.NewConfiguration()
	dyn.CPUProvisionConfiguration.ControlKnobConstraints = []configapi.ControlKnobConstraints{
		restrictRule(string(rama), string(canon), testKnob, true, 2, 0),
	}
	r.DynamicConfiguration = dyn
	r.TryUpdateProvision()
	res = r.provisionPolicyResults[rama].getControlKnob()
	assert.Equal(t, 12.0, res[quotaKnob].Value)
	assert.Equal(t, 12.0, res[testKnob].Value)
}

// TestDedicatedTryUpdateProvisionExactOwnedInvalid proves a fully-explicit
// bounded constraint on the exact-owned quota knob is invalid-config and does
// not alter the exact quota clamp through production TryUpdateProvision.
func TestDedicatedTryUpdateProvisionExactOwnedInvalid(t *testing.T) {
	const (
		rama      = types.CPUProvisionPolicyRama
		dynP      = types.CPUProvisionPolicyDynamicQuota
		quotaKnob = configapi.ControlKnobReclaimedCoresCPUQuota
	)

	r := newDedicatedRestrictFixture(t)
	injectFakeProvisionPolicies(t, r, map[types.CPUProvisionPolicyName]types.ControlKnob{
		rama: {quotaKnob: ramaRestrictKnob(20)},
		dynP: {quotaKnob: ramaRestrictKnob(12)},
	})

	dyn := dynamic.NewConfiguration()
	dyn.CPUProvisionConfiguration.ControlKnobConstraints = []configapi.ControlKnobConstraints{
		{Name: quotaKnob, TargetPolicy: configapi.CPUProvisionPolicyName(rama), ReferencePolicy: configapi.CPUProvisionPolicyName(dynP), Enforce: f64ptrBool(true)},
	}
	r.DynamicConfiguration = dyn
	r.TryUpdateProvision()

	res := r.provisionPolicyResults[rama].getControlKnob()
	assert.Equal(t, 12.0, res[quotaKnob].Value)
}

// lowerRestrictRule builds an AQC ControlKnobConstraints entry with lower-gap
// fields set (upper disabled).
func lowerRestrictRule(target, ref string, knob configapi.ControlKnobName, enforce bool, gap, ratio float64) configapi.ControlKnobConstraints {
	e := enforce
	c := configapi.ControlKnobConstraints{
		Name:            knob,
		TargetPolicy:    configapi.CPUProvisionPolicyName(target),
		ReferencePolicy: configapi.CPUProvisionPolicyName(ref),
		Enforce:         &e,
	}
	if gap != 0 {
		g := gap
		c.MaxLowerGap = &g
	}
	if ratio != 0 {
		rr := ratio
		c.MaxLowerGapRatio = &rr
	}
	return c
}

// recordingMetrics records StoreFloat64 calls for assertions.
type recordingMetrics struct {
	entries []recordingEntry
}
type recordingEntry struct {
	key  string
	val  float64
	tags []metrics.MetricTag
}

func (m *recordingMetrics) StoreFloat64(key string, val float64, _ metrics.MetricTypeName, tags ...metrics.MetricTag) error {
	m.entries = append(m.entries, recordingEntry{key: key, val: val, tags: tags})
	return nil
}
func (m *recordingMetrics) StoreInt(_ string, _ int64, _ metrics.MetricTypeName, _ ...metrics.MetricTag) error {
	return nil
}
func (m *recordingMetrics) StoreBool(_ string, _ bool, _ metrics.MetricTypeName, _ ...metrics.MetricTag) error {
	return nil
}
func (m *recordingMetrics) StoreFloat64TimeSeries(_ string, _ float64, _ metrics.MetricTypeName, _ ...metrics.MetricTag) error {
	return nil
}
func (m *recordingMetrics) StoreIntTimeSeries(_ string, _ int64, _ metrics.MetricTypeName, _ ...metrics.MetricTag) error {
	return nil
}
func (m *recordingMetrics) StoreWithContext(_ string, _ float64, _ metrics.MetricTypeName, _ ...metrics.MetricTag) error {
	return nil
}
func (m *recordingMetrics) GetMetricsEmitter(_ string) metrics.MetricEmitter { return m }
func (m *recordingMetrics) Run(_ context.Context)                            {}
func (m *recordingMetrics) Stop()                                            {}
func (m *recordingMetrics) StoreInt64(_ string, _ int64, _ metrics.MetricTypeName, _ ...metrics.MetricTag) error {
	return nil
}
func (m *recordingMetrics) WithTags(_ string, _ ...metrics.MetricTag) metrics.MetricEmitter { return m }

// TestDedicatedTryUpdateProvisionBoundedCases runs table-driven bounded cases
// through the real QoSRegionDedicated.TryUpdateProvision.
func TestDedicatedTryUpdateProvisionBoundedCases(t *testing.T) {
	const (
		rama      = types.CPUProvisionPolicyRama
		canon     = types.CPUProvisionPolicyCanonical
		dynP      = types.CPUProvisionPolicyDynamicQuota
		quotaKnob = configapi.ControlKnobReclaimedCoresCPUQuota
		testKnob  = configapi.ControlKnobName("test-bounded-knob")
	)

	// ref=10: upper band = 10+2=12; lower band = max(0,10-2)=8.
	mkBase := func() *QoSRegionDedicated {
		r := newDedicatedRestrictFixture(t)
		injectFakeProvisionPolicies(t, r, map[types.CPUProvisionPolicyName]types.ControlKnob{
			rama:  {testKnob: ramaRestrictKnob(20), quotaKnob: ramaRestrictKnob(20)},
			canon: {testKnob: ramaRestrictKnob(10)},
			dynP:  {quotaKnob: ramaRestrictKnob(12)},
		})
		return r
	}

	t.Run("shadow upper observes no clamp", func(t *testing.T) {
		r := mkBase()
		rec := &recordingMetrics{}
		r.emitter = rec
		dyn := dynamic.NewConfiguration()
		dyn.CPUProvisionConfiguration.ControlKnobConstraints = []configapi.ControlKnobConstraints{
			restrictRule(string(rama), string(canon), testKnob, false, 2, 0),
		}
		r.DynamicConfiguration = dyn
		r.TryUpdateProvision()
		res := r.provisionPolicyResults[rama].getControlKnob()
		assert.Equal(t, 20.0, res[testKnob].Value) // raw unchanged
		// quota exact still clamps 20->12
		assert.Equal(t, 12.0, res[quotaKnob].Value)
		// upper metric emitted with expected tags.
		var foundEntry *recordingEntry
		for i := range rec.entries {
			if rec.entries[i].key == metricControlKnobRestrictUpperBound {
				foundEntry = &rec.entries[i]
				break
			}
		}
		require.NotNil(t, foundEntry, "upper metric emitted")
		tags := map[string]string{}
		for _, tg := range foundEntry.tags {
			tags[tg.Key] = tg.Val
		}
		assert.Equal(t, "dedicated", tags[metricTagKeyRegionType])
		assert.Equal(t, string(rama), tags["target_policy"])
		assert.Equal(t, string(canon), tags["reference_policy"])
		assert.Equal(t, string(testKnob), tags["control_knob"])
		assert.Equal(t, restrictModeShadow, tags["mode"])
		assert.Equal(t, "observed", tags["action"])
	})

	t.Run("enforce lower raises raw below lower", func(t *testing.T) {
		r := mkBase()
		injectFakeProvisionPolicies(t, r, map[types.CPUProvisionPolicyName]types.ControlKnob{
			rama:  {testKnob: ramaRestrictKnob(5), quotaKnob: ramaRestrictKnob(20)},
			canon: {testKnob: ramaRestrictKnob(10)},
			dynP:  {quotaKnob: ramaRestrictKnob(12)},
		})
		rec := &recordingMetrics{}
		r.emitter = rec
		dyn := dynamic.NewConfiguration()
		dyn.CPUProvisionConfiguration.ControlKnobConstraints = []configapi.ControlKnobConstraints{
			lowerRestrictRule(string(rama), string(canon), testKnob, true, 2, 0),
		}
		r.DynamicConfiguration = dyn
		r.TryUpdateProvision()
		res := r.provisionPolicyResults[rama].getControlKnob()
		// lower band = max(0,10-2)=8; raw 5 raised to 8.
		assert.Equal(t, 8.0, res[testKnob].Value)
		// lower metric emitted with expected tags and value 8.
		var foundEntry *recordingEntry
		for i := range rec.entries {
			if rec.entries[i].key == metricControlKnobRestrictLowerBound {
				foundEntry = &rec.entries[i]
				break
			}
		}
		require.NotNil(t, foundEntry, "lower metric emitted")
		assert.Equal(t, 8.0, foundEntry.val)
		tags := map[string]string{}
		for _, tg := range foundEntry.tags {
			tags[tg.Key] = tg.Val
		}
		assert.Equal(t, "dedicated", tags[metricTagKeyRegionType])
		assert.Equal(t, restrictModeEnforce, tags["mode"])
		assert.Equal(t, "clamped", tags["action"])
	})

	t.Run("lower clamps even when below region requests", func(t *testing.T) {
		// Regression: raw=20, ref=40, lower=25 (lower gap 15). Even though the
		// delivered candidate 25 is below region requests 30, bounded path
		// raises raw to lower=25.
		r := mkBase()
		injectFakeProvisionPolicies(t, r, map[types.CPUProvisionPolicyName]types.ControlKnob{
			rama:  {testKnob: ramaRestrictKnob(20), quotaKnob: ramaRestrictKnob(20)},
			canon: {testKnob: ramaRestrictKnob(40)},
			dynP:  {quotaKnob: ramaRestrictKnob(12)},
		})
		dyn := dynamic.NewConfiguration()
		dyn.CPUProvisionConfiguration.ControlKnobConstraints = []configapi.ControlKnobConstraints{
			lowerRestrictRule(string(rama), string(canon), testKnob, true, 15, 0),
		}
		r.DynamicConfiguration = dyn
		r.TryUpdateProvision()
		res := r.provisionPolicyResults[rama].getControlKnob()
		assert.Equal(t, 25.0, res[testKnob].Value)
	})

	t.Run("upper clamps even when candidate below region requests", func(t *testing.T) {
		// Regression: raw=80, ref=10, upper=15. Even though the delivered upper
		// 15 is below region requests 30, bounded path clamps raw down to 15.
		r := mkBase()
		injectFakeProvisionPolicies(t, r, map[types.CPUProvisionPolicyName]types.ControlKnob{
			rama:  {testKnob: ramaRestrictKnob(80), quotaKnob: ramaRestrictKnob(20)},
			canon: {testKnob: ramaRestrictKnob(10)},
			dynP:  {quotaKnob: ramaRestrictKnob(12)},
		})
		dyn := dynamic.NewConfiguration()
		dyn.CPUProvisionConfiguration.ControlKnobConstraints = []configapi.ControlKnobConstraints{
			restrictRule(string(rama), string(canon), testKnob, true, 5, 0),
		}
		r.DynamicConfiguration = dyn
		r.TryUpdateProvision()
		res := r.provisionPolicyResults[rama].getControlKnob()
		assert.Equal(t, 15.0, res[testKnob].Value)
	})

	t.Run("half explicit invalid-config no clamp", func(t *testing.T) {
		r := mkBase()
		rec := &recordingMetrics{}
		r.emitter = rec
		dyn := dynamic.NewConfiguration()
		dyn.CPUProvisionConfiguration.ControlKnobConstraints = []configapi.ControlKnobConstraints{
			{Name: testKnob, TargetPolicy: configapi.CPUProvisionPolicyName(rama)},
		}
		r.DynamicConfiguration = dyn
		r.TryUpdateProvision()
		res := r.provisionPolicyResults[rama].getControlKnob()
		assert.Equal(t, 20.0, res[testKnob].Value)
		foundInvalid := false
		for _, e := range rec.entries {
			if e.key != metricControlKnobRestrictUpperBound {
				continue
			}
			tags := map[string]string{}
			for _, tg := range e.tags {
				tags[tg.Key] = tg.Val
			}
			if tags[metricTagKeyRegionType] == "dedicated" &&
				tags["action"] == string(restrictActionInvalidConfig) {
				foundInvalid = true
			}
		}
		assert.True(t, foundInvalid, "invalid-config action with region_type=dedicated emitted")
	})
}
