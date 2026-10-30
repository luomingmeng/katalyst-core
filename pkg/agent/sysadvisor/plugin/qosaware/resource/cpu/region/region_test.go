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
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"

	configapi "github.com/kubewharf/katalyst-api/pkg/apis/config/v1alpha1"
	"github.com/kubewharf/katalyst-api/pkg/consts"
	katalyst_base "github.com/kubewharf/katalyst-core/cmd/base"
	"github.com/kubewharf/katalyst-core/cmd/katalyst-agent/app/options"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/metacache"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/types"
	"github.com/kubewharf/katalyst-core/pkg/config"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	metricspool "github.com/kubewharf/katalyst-core/pkg/metrics/metrics-pool"
	"github.com/kubewharf/katalyst-core/pkg/util/cgroup/common"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	"github.com/kubewharf/katalyst-core/pkg/util/reclaim"
)

func TestGetRegionNameFromMetaCache(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		container  *types.ContainerInfo
		numaID     int
		region     *types.RegionInfo
		regionName string
	}{
		{
			name: "shared-region",
			container: &types.ContainerInfo{
				QoSLevel:    consts.PodAnnotationQoSLevelSharedCores,
				RegionNames: sets.NewString("share-t"),
			},
			region: &types.RegionInfo{
				RegionName: "share-t",
				RegionType: configapi.QoSRegionTypeShare,
			},
			regionName: "share-t",
		},
		{
			name: "dedicated-region",
			container: &types.ContainerInfo{
				QoSLevel: consts.PodAnnotationQoSLevelDedicatedCores,
				Annotations: map[string]string{
					consts.PodAnnotationMemoryEnhancementNumaBinding: consts.PodAnnotationMemoryEnhancementNumaBindingEnable,
				},
				RegionNames: sets.NewString("dedicated-t"),
			},
			region: &types.RegionInfo{
				RegionName:   "dedicated-t",
				RegionType:   configapi.QoSRegionTypeDedicated,
				BindingNumas: machine.NewCPUSet(1),
			},
			numaID:     1,
			regionName: "dedicated-t",
		},
		{
			name: "dedicated-region-numa-miss",
			container: &types.ContainerInfo{
				QoSLevel: consts.PodAnnotationQoSLevelDedicatedCores,
				Annotations: map[string]string{
					consts.PodAnnotationMemoryEnhancementNumaBinding: consts.PodAnnotationMemoryEnhancementNumaBindingEnable,
				},
				RegionNames: sets.NewString("dedicated-t"),
			},
			region: &types.RegionInfo{
				RegionName:   "dedicated-t",
				RegionType:   configapi.QoSRegionTypeDedicated,
				BindingNumas: machine.NewCPUSet(1),
			},
			numaID:     2,
			regionName: "",
		},
		{
			name: "isolation-region",
			container: &types.ContainerInfo{
				QoSLevel:    consts.PodAnnotationQoSLevelSharedCores,
				RegionNames: sets.NewString("isolation-t"),
				Isolated:    true,
			},
			region: &types.RegionInfo{
				RegionName: "isolation-t",
				RegionType: configapi.QoSRegionTypeIsolation,
			},
			regionName: "isolation-t",
		},
		{
			name: "isolation-region-empty",
			container: &types.ContainerInfo{
				QoSLevel:    consts.PodAnnotationQoSLevelSharedCores,
				RegionNames: sets.NewString("isolation-t"),
				Isolated:    true,
			},
			region: &types.RegionInfo{
				RegionName: "isolation-t",
				RegionType: configapi.QoSRegionTypeShare,
			},
			regionName: "",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			conf, err := options.NewOptions().Config()
			assert.NoError(t, err)
			conf.GenericSysAdvisorConfiguration.StateFileDirectory = os.TempDir()
			cache, err := metacache.NewMetaCacheImp(conf, metricspool.DummyMetricsEmitterPool{}, nil)
			assert.NoError(t, err)
			err = cache.SetRegionInfo(tt.region.RegionName, tt.region)
			assert.NoError(t, err)
			err = cache.SetContainerInfo(tt.container.PodUID, tt.container.ContainerName, tt.container)
			assert.NoError(t, err)

			assert.Equal(t, tt.regionName, getRegionNameFromMetaCache(tt.container, tt.numaID, cache))
		})
	}
}

// isolatedReclaimConsumer re-points the test's MetaServer bootstrap at a
// private, uniquely-named reclaim consumer instead of the process-wide
// GenericConsumerName, and backs the state/checkpoint directories with
// t.TempDir().
//
// NewMetaServer -> NewMetaAgent -> reclaim.SetupConsumers otherwise registers
// the shared "generic" consumer into the process-global registry. Cleaning that
// up afterwards via reclaim.UnregisterConsumer(GenericConsumerName) tears down
// an entry that a parallel sibling test still owns, which is exactly why the
// tests had to drop t.Parallel. Giving each test its own consumer name (and an
// empty reclaim cgroup path so the reverse index stays empty) keeps the
// bootstrap side-effect fully local, so the tests can run concurrently without
// a mutex.
func isolatedReclaimConsumer(t *testing.T, conf *config.Configuration) {
	t.Helper()

	// t.Name() is unique per top-level test and per subtest, so the consumer
	// never collides with a parallel sibling's entry.
	consumerName := "region-test/" + t.Name()
	reclaim.RegisterFactory(consumerName, reclaim.NewGenericConsumer)
	conf.ReclaimConsumers = []string{consumerName}
	// No reclaim cgroup subtree to publish: keep the reverse index empty so
	// parallel consumers never share a path entry.
	conf.BaseConfiguration.ReclaimRelativeRootCgroupPath = ""

	stateDir := t.TempDir()
	conf.GenericSysAdvisorConfiguration.StateFileDirectory = stateDir
	conf.MetaServerConfiguration.CheckpointManagerDir = filepath.Join(stateDir, "checkpoint")

	t.Cleanup(func() { reclaim.UnregisterConsumer(consumerName) })
}

func TestIsNumaBinding(t *testing.T) {
	t.Parallel()
	conf, err := options.NewOptions().Config()
	require.NoError(t, err)
	require.NotNil(t, conf)

	conf.ExactRestrictRefPolicy = nil
	isolatedReclaimConsumer(t, conf)

	genericCtx, err := katalyst_base.GenerateFakeGenericContext([]runtime.Object{})
	require.NoError(t, err)

	metaServer, err := metaserver.NewMetaServer(genericCtx.Client, metrics.DummyMetrics{}, conf)
	require.NoError(t, err)

	metaCache, err := metacache.NewMetaCacheImp(conf, metricspool.DummyMetricsEmitterPool{}, metric.NewFakeMetricsFetcher(metrics.DummyMetrics{}))
	require.NoError(t, err)
	ci := types.ContainerInfo{
		QoSLevel:    consts.PodAnnotationQoSLevelSharedCores,
		RegionNames: sets.NewString("share-NUMA1"),
	}
	share := NewQoSRegionShare(&ci, conf, nil, 1, metaCache, metaServer, metrics.DummyMetrics{})
	require.True(t, share.IsNumaBinding(), "test IsNumaBinding failed")

	ci2 := types.ContainerInfo{
		QoSLevel:    consts.PodAnnotationQoSLevelSharedCores,
		RegionNames: sets.NewString("share"),
	}
	share2 := NewQoSRegionShare(&ci2, conf, nil, commonstate.FakedNUMAID, metaCache, metaServer, metrics.DummyMetrics{})
	require.False(t, share2.IsNumaBinding(), "test IsNumaBinding failed")

	ci3 := types.ContainerInfo{
		QoSLevel:    consts.PodAnnotationQoSLevelSharedCores,
		RegionNames: sets.NewString("isolation-NUMA1-1"),
		Isolated:    true,
	}
	isolation1 := NewQoSRegionIsolation(&ci3, "isolation-1", conf, nil, 1, metaCache, metaServer, metrics.DummyMetrics{})
	require.True(t, isolation1.IsNumaBinding(), "test IsNumaBinding failed")

	ci4 := types.ContainerInfo{
		QoSLevel:    consts.PodAnnotationQoSLevelSharedCores,
		RegionNames: sets.NewString("isolation-1"),
		Isolated:    true,
	}
	isolation2 := NewQoSRegionIsolation(&ci4, "isolation-1", conf, nil, commonstate.FakedNUMAID, metaCache, metaServer, metrics.DummyMetrics{})
	require.False(t, isolation2.IsNumaBinding(), "test IsNumaBinding failed")
}

func TestRestrictControlKnobByExactPolicy(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name              string
		originControlKnob map[types.CPUProvisionPolicyName]types.ControlKnob
		wantControlKnob   map[types.CPUProvisionPolicyName]types.ControlKnob
	}{
		{
			name:              "no restriction",
			originControlKnob: map[types.CPUProvisionPolicyName]types.ControlKnob{"p1": {"c1": types.ControlKnobItem{Value: 8}}, "p2": {"c1": types.ControlKnobItem{Value: 10}}},
			wantControlKnob:   map[types.CPUProvisionPolicyName]types.ControlKnob{"p1": {"c1": types.ControlKnobItem{Value: 8}}, "p2": {"c1": types.ControlKnobItem{Value: 10}}},
		},
		{
			name:              "restricted by p2",
			originControlKnob: map[types.CPUProvisionPolicyName]types.ControlKnob{"p1": {configapi.ControlKnobReclaimedCoresCPUQuota: types.ControlKnobItem{Value: 16}}, "p2": {configapi.ControlKnobReclaimedCoresCPUQuota: types.ControlKnobItem{Value: 10}}},
			wantControlKnob:   map[types.CPUProvisionPolicyName]types.ControlKnob{"p1": {configapi.ControlKnobReclaimedCoresCPUQuota: types.ControlKnobItem{Value: 10}}, "p2": {configapi.ControlKnobReclaimedCoresCPUQuota: types.ControlKnobItem{Value: 10}}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			conf, err := options.NewOptions().Config()
			require.NoError(t, err)
			require.NotNil(t, conf)

			conf.ExactRestrictRefPolicy = map[types.CPUProvisionPolicyName]types.CPUProvisionPolicyName{"p1": "p2"}
			isolatedReclaimConsumer(t, conf)

			genericCtx, err := katalyst_base.GenerateFakeGenericContext([]runtime.Object{})
			require.NoError(t, err)

			metaServer, err := metaserver.NewMetaServer(genericCtx.Client, metrics.DummyMetrics{}, conf)
			require.NoError(t, err)

			metaCache, err := metacache.NewMetaCacheImp(conf, metricspool.DummyMetricsEmitterPool{}, metric.NewFakeMetricsFetcher(metrics.DummyMetrics{}))
			require.NoError(t, err)
			ci := types.ContainerInfo{
				QoSLevel:    consts.PodAnnotationQoSLevelSharedCores,
				RegionNames: sets.NewString("share"),
			}
			share := NewQoSRegionShare(&ci, conf, nil, commonstate.FakedNUMAID, metaCache, metaServer, metrics.DummyMetrics{}).(*QoSRegionShare)
			dyn := dynamic.NewConfiguration()
			if tt.name == "restricted by p2" {
				// Name-only (exact-path candidate) constraint for the quota knob.
				dyn.CPUProvisionConfiguration.ControlKnobConstraints = []configapi.ControlKnobConstraints{
					{Name: configapi.ControlKnobReclaimedCoresCPUQuota},
				}
			}
			share.DynamicConfiguration = dyn
			restrictedControlKnobs := share.restrictControlKnobByExactPolicy(tt.originControlKnob)
			assert.Equal(t, tt.wantControlKnob, restrictedControlKnobs)
		})
	}
}

// ramaRestrictKnob is a tiny helper to build a ControlKnobItem with Action=none.
func ramaRestrictKnob(v float64) types.ControlKnobItem {
	return types.ControlKnobItem{Value: v, Action: types.ControlKnobActionNone}
}

func f64ptr(v float64) *float64 { return &v }

// restrictRule builds an AQC ControlKnobConstraints entry for the generic executor.
func restrictRule(target, ref string, knob configapi.ControlKnobName, enforce bool, gap, ratio float64) configapi.ControlKnobConstraints {
	e := enforce
	c := configapi.ControlKnobConstraints{
		Name:            knob,
		TargetPolicy:    configapi.CPUProvisionPolicyName(target),
		ReferencePolicy: configapi.CPUProvisionPolicyName(ref),
		Enforce:         &e,
	}
	if gap != 0 {
		g := gap
		c.MaxUpperGap = &g
	}
	if ratio != 0 {
		rr := ratio
		c.MaxUpperGapRatio = &rr
	}
	return c
}

// newRestrictRegion builds a bare QoSRegionBase with AQC restrict constraints.
func newRestrictRegion(regionType configapi.QoSRegionType, constraints []configapi.ControlKnobConstraints) *QoSRegionBase {
	dyn := dynamic.NewConfiguration()
	dyn.CPUProvisionConfiguration.ControlKnobConstraints = constraints
	r := &QoSRegionBase{
		conf:       config.NewConfiguration(),
		regionType: regionType,
		name:       "restrict-rule-test",
		emitter:    metrics.DummyMetrics{},
	}
	r.DynamicConfiguration = dyn
	return r
}

// TestEvaluateUpperBoundPure covers the knob-agnostic pure computation.
func TestEvaluateUpperBoundPure(t *testing.T) {
	cases := []struct {
		name       string
		raw, ref   float64
		gap, ratio float64
		wantErr    string
		wantOver   bool
	}{
		{"ref NaN", 20, math.NaN(), 2, 0, restrictActionRefUnavailable, false},
		{"ref Inf", 20, math.Inf(1), 2, 0, restrictActionRefUnavailable, false},
		{"ref zero", 20, 0, 2, 0, restrictActionRefUnavailable, false},
		{"ref negative", 20, -3, 2, 0, restrictActionRefUnavailable, false},
		{"no positive tolerance", 20, 10, 0, 0, restrictActionNoPositiveTolerance, false},
		{"within band", 11, 10, 2, 0, "", false},
		{"overshoot gap", 20, 10, 2, 0, "", true},
		{"overshoot ratio", 20, 10, 0, 0.5, "", true},
	}
	for _, c := range cases {
		_, _, _, _, upperOver, _, errAction := evaluateBounds(c.raw, c.ref, c.gap, c.ratio, 0, 0)
		assert.Equal(t, c.wantErr, errAction, c.name)
		assert.Equal(t, c.wantOver, upperOver, c.name)
	}
	up, _, upperOn, _, _, _, errAction := evaluateBounds(20, 10, 2, 0.1, 0, 0)
	assert.True(t, upperOn)
	assert.Empty(t, errAction)
	assert.Equal(t, 11.0, up)
}

// TestApplyBoundedControlKnobConstraints covers the generic executor table-driven:
// one-sided upper bound, fail-open paths, composition and validators.
func TestApplyBoundedControlKnobConstraints(t *testing.T) {
	const (
		rama   = types.CPUProvisionPolicyRama
		canon  = types.CPUProvisionPolicyCanonical
		nrKnob = configapi.ControlKnobNonReclaimedCPURequirement
	)
	strRama, strCanon := string(rama), string(canon)

	mk := func(refV, ramaV float64) map[types.CPUProvisionPolicyName]types.ControlKnob {
		return map[types.CPUProvisionPolicyName]types.ControlKnob{
			canon: {nrKnob: ramaRestrictKnob(refV)},
			rama:  {nrKnob: ramaRestrictKnob(ramaV)},
		}
	}

	tests := []struct {
		name   string
		rules  []configapi.ControlKnobConstraints
		origin map[types.CPUProvisionPolicyName]types.ControlKnob
		want   map[types.CPUProvisionPolicyName]types.ControlKnob
	}{
		{name: "no rules disabled passthrough", origin: mk(10, 20), want: mk(10, 20)},
		{name: "enforce gap clamps", rules: []configapi.ControlKnobConstraints{
			restrictRule(strRama, strCanon, nrKnob, true, 2, 0)}, origin: mk(10, 20), want: mk(10, 12)},
		{name: "shadow observes no clamp", rules: []configapi.ControlKnobConstraints{
			restrictRule(strRama, strCanon, nrKnob, false, 2, 0)}, origin: mk(10, 20), want: mk(10, 20)},
		{name: "raw within band passthrough", rules: []configapi.ControlKnobConstraints{
			restrictRule(strRama, strCanon, nrKnob, true, 2, 0)}, origin: mk(10, 11), want: mk(10, 11)},
		{name: "ref absent fail-open", rules: []configapi.ControlKnobConstraints{
			restrictRule(strRama, strCanon, nrKnob, true, 2, 0)},
			origin: map[types.CPUProvisionPolicyName]types.ControlKnob{rama: {nrKnob: ramaRestrictKnob(20)}},
			want:   map[types.CPUProvisionPolicyName]types.ControlKnob{rama: {nrKnob: ramaRestrictKnob(20)}}},
		{name: "unknown target policy skip no panic", rules: []configapi.ControlKnobConstraints{
			restrictRule("no-such-policy", strCanon, nrKnob, true, 2, 0)}, origin: mk(10, 20), want: mk(10, 20)},
		{name: "unknown reference policy fail-open no panic", rules: []configapi.ControlKnobConstraints{
			restrictRule(strRama, "ghost", nrKnob, true, 2, 0)}, origin: mk(10, 20), want: mk(10, 20)},
		{name: "no positive tolerance observe", rules: []configapi.ControlKnobConstraints{
			restrictRule(strRama, strCanon, nrKnob, true, 0, 0)}, origin: mk(10, 20), want: mk(10, 20)},
		{name: "upper clamps even when below region requests", rules: []configapi.ControlKnobConstraints{
			restrictRule(strRama, strCanon, nrKnob, true, 2, 0)}, origin: mk(10, 30), want: mk(10, 12)},
		{name: "two enforce rules take min upper", rules: []configapi.ControlKnobConstraints{
			restrictRule(strRama, strCanon, nrKnob, true, 5, 0),
			restrictRule(strRama, strCanon, nrKnob, true, 1, 0),
		}, origin: mk(10, 30), want: mk(10, 11)},
		{name: "shadow tighter than enforce does not lower delivery", rules: []configapi.ControlKnobConstraints{
			restrictRule(strRama, strCanon, nrKnob, false, 0.5, 0),
			restrictRule(strRama, strCanon, nrKnob, true, 5, 0),
		}, origin: mk(10, 30), want: mk(10, 15)},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			r := newRestrictRegion(configapi.QoSRegionTypeShare, tt.rules)
			got := r.applyBoundedControlKnobConstraints(tt.origin)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestApplyBoundedConstraintsDecoupledFromExactRefPolicy proves bounded Constraints are fully
// decoupled from the exact-path ExactRestrictRefPolicy map: both policies must be explicit, a
// missing target or reference is invalid-config (raw stands), and changing the
// ExactRestrictRefPolicy map has zero effect on an explicit bounded rule.
func TestApplyBoundedConstraintsDecoupledFromExactRefPolicy(t *testing.T) {
	const (
		dynP   = types.CPUProvisionPolicyName("dynamic-quota")
		canon  = types.CPUProvisionPolicyCanonical
		nrKnob = configapi.ControlKnobNonReclaimedCPURequirement
	)
	origin := map[types.CPUProvisionPolicyName]types.ControlKnob{
		canon: {nrKnob: ramaRestrictKnob(10)},
		dynP:  {nrKnob: ramaRestrictKnob(20)},
	}

	t.Run("omitted reference is invalid-config, raw stands", func(t *testing.T) {
		r := newRestrictRegion(configapi.QoSRegionTypeShare,
			[]configapi.ControlKnobConstraints{restrictRule(string(dynP), "", nrKnob, true, 2, 0)})
		// Even with a ExactRestrictRefPolicy entry, bounded path never reads it.
		r.conf.ExactRestrictRefPolicy = map[types.CPUProvisionPolicyName]types.CPUProvisionPolicyName{dynP: canon}
		got := r.applyBoundedControlKnobConstraints(cloneControlKnobs(origin))
		assert.Equal(t, 20.0, got[dynP][nrKnob].Value)
	})

	t.Run("omitted target is invalid-config, raw stands", func(t *testing.T) {
		r := newRestrictRegion(configapi.QoSRegionTypeShare,
			[]configapi.ControlKnobConstraints{restrictRule("", string(canon), nrKnob, true, 2, 0)})
		got := r.applyBoundedControlKnobConstraints(cloneControlKnobs(origin))
		assert.Equal(t, 20.0, got[dynP][nrKnob].Value)
	})

	t.Run("explicit bounded result is unaffected by ExactRestrictRefPolicy", func(t *testing.T) {
		rule := []configapi.ControlKnobConstraints{restrictRule(string(dynP), string(canon), nrKnob, true, 2, 0)}
		withMap := newRestrictRegion(configapi.QoSRegionTypeShare, rule)
		withMap.conf.ExactRestrictRefPolicy = map[types.CPUProvisionPolicyName]types.CPUProvisionPolicyName{dynP: canon}
		gotWith := withMap.applyBoundedControlKnobConstraints(cloneControlKnobs(origin))

		emptyMap := newRestrictRegion(configapi.QoSRegionTypeShare, rule)
		emptyMap.conf.ExactRestrictRefPolicy = map[types.CPUProvisionPolicyName]types.CPUProvisionPolicyName{}
		gotEmpty := emptyMap.applyBoundedControlKnobConstraints(cloneControlKnobs(origin))

		assert.Equal(t, gotWith[dynP][nrKnob].Value, gotEmpty[dynP][nrKnob].Value)
		assert.Equal(t, 12.0, gotWith[dynP][nrKnob].Value)
	})
}

// TestApplyBoundedConstraintsDifferentReferenceCoexist: two rules on same target/knob
// against different references each read their own ref; delivered = min(raw, valid
// enforce uppers), no feed-forward.
func TestApplyBoundedConstraintsDifferentReferenceCoexist(t *testing.T) {
	const (
		rama   = types.CPUProvisionPolicyRama
		canon  = types.CPUProvisionPolicyCanonical
		nrKnob = configapi.ControlKnobNonReclaimedCPURequirement
	)
	strRama, strCanon := string(rama), string(canon)
	origin := map[types.CPUProvisionPolicyName]types.ControlKnob{
		canon:                                {nrKnob: ramaRestrictKnob(10)},
		types.CPUProvisionPolicyDynamicQuota: {nrKnob: ramaRestrictKnob(20)},
		rama:                                 {nrKnob: ramaRestrictKnob(30)},
	}
	rules := []configapi.ControlKnobConstraints{
		restrictRule(strRama, strCanon, nrKnob, true, 5, 0),
		restrictRule(strRama, string(types.CPUProvisionPolicyDynamicQuota), nrKnob, true, 1, 0),
	}
	r := newRestrictRegion(configapi.QoSRegionTypeShare, rules)
	got := r.applyBoundedControlKnobConstraints(origin)
	assert.Equal(t, 15.0, got[rama][nrKnob].Value)
	assert.Equal(t, 10.0, got[canon][nrKnob].Value)
	assert.Equal(t, 20.0, got[types.CPUProvisionPolicyDynamicQuota][nrKnob].Value)
}

// TestApplyBoundedConstraintsMultiKnobClamp: two rules on different non-exact-set knobs
// of the same policy clamp independently; neither feeds into the other.
func TestApplyBoundedConstraintsMultiKnobClamp(t *testing.T) {
	const (
		rama       = types.CPUProvisionPolicyRama
		canon      = types.CPUProvisionPolicyCanonical
		nrKnob     = configapi.ControlKnobNonReclaimedCPURequirement
		customKnob = configapi.ControlKnobName("custom-test-knob")
	)
	strRama, strCanon := string(rama), string(canon)
	origin := map[types.CPUProvisionPolicyName]types.ControlKnob{
		canon: {nrKnob: ramaRestrictKnob(10), customKnob: ramaRestrictKnob(20)},
		rama:  {nrKnob: ramaRestrictKnob(30), customKnob: ramaRestrictKnob(40)},
	}
	rules := []configapi.ControlKnobConstraints{
		restrictRule(strRama, strCanon, nrKnob, true, 2, 0),     // 10+2=12
		restrictRule(strRama, strCanon, customKnob, true, 5, 0), // 20+5=25
	}
	r := newRestrictRegion(configapi.QoSRegionTypeShare, rules)
	got := r.applyBoundedControlKnobConstraints(origin)
	assert.Equal(t, 12.0, got[rama][nrKnob].Value)
	assert.Equal(t, 25.0, got[rama][customKnob].Value)
	// reference untouched
	assert.Equal(t, 10.0, got[canon][nrKnob].Value)
	assert.Equal(t, 20.0, got[canon][customKnob].Value)
}

// recordingEmitter captures the tags of every StoreFloat64 call.
type recordingEmitter struct {
	metrics.MetricEmitter
	records [][]metrics.MetricTag
}

func cloneControlKnobs(in map[types.CPUProvisionPolicyName]types.ControlKnob) map[types.CPUProvisionPolicyName]types.ControlKnob {
	out := make(map[types.CPUProvisionPolicyName]types.ControlKnob, len(in))
	for k, v := range in {
		out[k] = v.Clone()
	}
	return out
}

func (e *recordingEmitter) StoreFloat64(key string, val float64, _ metrics.MetricTypeName, tags ...metrics.MetricTag) error {
	e.records = append(e.records, tags)
	return nil
}
func (e *recordingEmitter) StoreInt64(_ string, _ int64, _ metrics.MetricTypeName, _ ...metrics.MetricTag) error {
	return nil
}
func (e *recordingEmitter) WithTags(_ string, _ ...metrics.MetricTag) metrics.MetricEmitter { return e }

// TestApplyBoundedConstraintsUnknownPolicyLabels asserts target/reference labels are
// "unknown" for a typo policy, the configured string never leaks into labels, and
// delivery stays fail-open. The control_knob tag follows the same contract: it is
// "unknown" unless the target policy and its raw knob both exist; known policies
// with the raw knob present keep the real knob label.
func TestApplyBoundedConstraintsUnknownPolicyLabels(t *testing.T) {
	const (
		rama   = types.CPUProvisionPolicyRama
		canon  = types.CPUProvisionPolicyCanonical
		nrKnob = configapi.ControlKnobNonReclaimedCPURequirement
	)
	tagVal := func(tags []metrics.MetricTag, key string) string {
		for _, tg := range tags {
			if tg.Key == key {
				return tg.Val
			}
		}
		return ""
	}
	origin := map[types.CPUProvisionPolicyName]types.ControlKnob{
		canon: {nrKnob: ramaRestrictKnob(10)},
		rama:  {nrKnob: ramaRestrictKnob(30)},
	}

	// typo target, known reference: target missing => control_knob unknown.
	{
		r := newRestrictRegion(configapi.QoSRegionTypeShare,
			[]configapi.ControlKnobConstraints{restrictRule("typo-target", string(canon), nrKnob, true, 2, 0)})
		em := &recordingEmitter{}
		r.emitter = em
		got := r.applyBoundedControlKnobConstraints(cloneControlKnobs(origin))
		assert.Equal(t, 30.0, got[rama][nrKnob].Value) // fail-open
		require.Len(t, em.records, 1)
		assert.Equal(t, "unknown", tagVal(em.records[0], "target_policy"))
		assert.Equal(t, string(canon), tagVal(em.records[0], "reference_policy"))
		assert.Equal(t, "unknown", tagVal(em.records[0], "control_knob"))
	}
	// known target, typo reference: target and raw knob exist => real knob label.
	{
		r := newRestrictRegion(configapi.QoSRegionTypeShare,
			[]configapi.ControlKnobConstraints{restrictRule(string(rama), "typo-ref", nrKnob, true, 2, 0)})
		em := &recordingEmitter{}
		r.emitter = em
		got := r.applyBoundedControlKnobConstraints(cloneControlKnobs(origin))
		assert.Equal(t, 30.0, got[rama][nrKnob].Value)
		require.Len(t, em.records, 1)
		assert.Equal(t, string(rama), tagVal(em.records[0], "target_policy"))
		assert.Equal(t, "unknown", tagVal(em.records[0], "reference_policy"))
		assert.Equal(t, string(nrKnob), tagVal(em.records[0], "control_knob"))
	}
	// known target and reference, but raw knob absent from the target map: target
	// exists yet rawOK=false => control_knob unknown (no configured knob string leaks).
	{
		missingKnob := configapi.ControlKnobName("absent-knob")
		r := newRestrictRegion(configapi.QoSRegionTypeShare,
			[]configapi.ControlKnobConstraints{restrictRule(string(rama), string(canon), missingKnob, true, 2, 0)})
		em := &recordingEmitter{}
		r.emitter = em
		r.applyBoundedControlKnobConstraints(cloneControlKnobs(origin))
		require.Len(t, em.records, 1)
		assert.Equal(t, string(rama), tagVal(em.records[0], "target_policy"))
		assert.Equal(t, string(canon), tagVal(em.records[0], "reference_policy"))
		assert.Equal(t, "unknown", tagVal(em.records[0], "control_knob"))
	}
	// known target and reference with raw knob present keep real labels.
	{
		r := newRestrictRegion(configapi.QoSRegionTypeShare,
			[]configapi.ControlKnobConstraints{restrictRule(string(rama), string(canon), nrKnob, true, 2, 0)})
		em := &recordingEmitter{}
		r.emitter = em
		r.applyBoundedControlKnobConstraints(cloneControlKnobs(origin))
		require.Len(t, em.records, 1)
		assert.Equal(t, string(rama), tagVal(em.records[0], "target_policy"))
		assert.Equal(t, string(canon), tagVal(em.records[0], "reference_policy"))
		assert.Equal(t, string(nrKnob), tagVal(em.records[0], "control_knob"))
	}
}

// TestApplyBoundedBothSidesEmitsTwoGauges proves that when a constraint has
// both upper and lower bands enabled, the executor emits both the upper and
// lower gauges (each with its own bound value and per-side action), and that
// error paths emit exactly one upper gauge with raw.
func TestApplyBoundedBothSidesEmitsTwoGauges(t *testing.T) {
	const (
		rama   = types.CPUProvisionPolicyRama
		canon  = types.CPUProvisionPolicyCanonical
		nrKnob = configapi.ControlKnobNonReclaimedCPURequirement
	)
	tagVal := func(tags []metrics.MetricTag, key string) string {
		for _, tg := range tags {
			if tg.Key == key {
				return tg.Val
			}
		}
		return ""
	}
	// ref=10, upper=12 (gap 2), lower=8 (gap 2). raw=20 -> upper overshoot,
	// lower passthrough. Enforce: upper clamps to 12, lower gauge still emits 8.
	origin := map[types.CPUProvisionPolicyName]types.ControlKnob{
		canon: {nrKnob: ramaRestrictKnob(10)},
		rama:  {nrKnob: ramaRestrictKnob(20)},
	}
	both := configapi.ControlKnobConstraints{
		Name:            nrKnob,
		TargetPolicy:    configapi.CPUProvisionPolicyName(rama),
		ReferencePolicy: configapi.CPUProvisionPolicyName(canon),
		Enforce:         func() *bool { v := true; return &v }(),
	}
	upperGap := 2.0
	both.MaxUpperGap = &upperGap
	lowerGap := 2.0
	both.MaxLowerGap = &lowerGap

	r := newRestrictRegion(configapi.QoSRegionTypeShare, []configapi.ControlKnobConstraints{both})
	rec := &recordingMetrics{}
	r.emitter = rec
	got := r.applyBoundedControlKnobConstraints(cloneControlKnobs(origin))
	assert.Equal(t, 12.0, got[rama][nrKnob].Value)

	var upperEntry, lowerEntry *recordingEntry
	for i := range rec.entries {
		switch rec.entries[i].key {
		case metricControlKnobRestrictUpperBound:
			upperEntry = &rec.entries[i]
		case metricControlKnobRestrictLowerBound:
			lowerEntry = &rec.entries[i]
		}
	}
	require.NotNil(t, upperEntry, "upper gauge emitted")
	require.NotNil(t, lowerEntry, "lower gauge emitted")
	assert.Equal(t, 12.0, upperEntry.val)
	assert.Equal(t, 8.0, lowerEntry.val)
	assert.Equal(t, "clamped", tagVal(upperEntry.tags, "action"))
	// raw 20 is not below lower 8, so lower side is passthrough.
	assert.Equal(t, "passthrough", tagVal(lowerEntry.tags, "action"))
	assert.Equal(t, restrictModeEnforce, tagVal(upperEntry.tags, "mode"))
	assert.Equal(t, restrictModeEnforce, tagVal(lowerEntry.tags, "mode"))
	assert.Equal(t, string(rama), tagVal(upperEntry.tags, "target_policy"))
	assert.Equal(t, string(canon), tagVal(upperEntry.tags, "reference_policy"))

	// Error path (no positive tolerance): exactly one upper gauge with raw.
	rec.entries = nil
	noneRule := restrictRule(string(rama), string(canon), nrKnob, true, 0, 0)
	r2 := newRestrictRegion(configapi.QoSRegionTypeShare, []configapi.ControlKnobConstraints{noneRule})
	r2.emitter = rec
	r2.applyBoundedControlKnobConstraints(cloneControlKnobs(origin))
	require.Len(t, rec.entries, 1)
	assert.Equal(t, metricControlKnobRestrictUpperBound, rec.entries[0].key)
	assert.Equal(t, 20.0, rec.entries[0].val)
	assert.Equal(t, restrictActionNoPositiveTolerance, tagVal(rec.entries[0].tags, "action"))

	// Error path (half-explicit invalid-config: target set, ref empty): exactly
	// one upper gauge with raw, no lower gauge.
	rec.entries = nil
	halfInvalid := configapi.ControlKnobConstraints{
		Name:         nrKnob,
		TargetPolicy: configapi.CPUProvisionPolicyName(rama),
		Enforce:      func() *bool { v := true; return &v }(),
	}
	r3 := newRestrictRegion(configapi.QoSRegionTypeShare, []configapi.ControlKnobConstraints{halfInvalid})
	r3.emitter = rec
	r3.applyBoundedControlKnobConstraints(cloneControlKnobs(origin))
	require.Len(t, rec.entries, 1, "half-explicit invalid-config emits one observation")
	assert.Equal(t, metricControlKnobRestrictUpperBound, rec.entries[0].key)
	assert.Equal(t, 20.0, rec.entries[0].val)
	assert.Equal(t, restrictActionInvalidConfig, tagVal(rec.entries[0].tags, "action"))
}

// TestApplyBoundedConflictEmitsOneConflictingBounds proves that when composed
// enforce bounds conflict (maxLower > minUpper) the delivery stays raw and
// exactly one upper gauge is emitted with action=conflicting-bounds.
func TestApplyBoundedConflictEmitsOneConflictingBounds(t *testing.T) {
	const (
		rama   = types.CPUProvisionPolicyRama
		canon  = types.CPUProvisionPolicyCanonical
		high   = types.CPUProvisionPolicyName("high-ref")
		nrKnob = configapi.ControlKnobNonReclaimedCPURequirement
	)
	tagVal := func(tags []metrics.MetricTag, key string) string {
		for _, tg := range tags {
			if tg.Key == key {
				return tg.Val
			}
		}
		return ""
	}
	// canon=10, high=100, raw=50. Upper rule against canon clamps raw down to 11
	// (gap 1). Lower rule against high raises raw up to 90 (gap 10). The composed
	// minUpper=11, maxLower=90 => maxLower > minUpper => conflict fail-open.
	origin := map[types.CPUProvisionPolicyName]types.ControlKnob{
		canon: {nrKnob: ramaRestrictKnob(10)},
		high:  {nrKnob: ramaRestrictKnob(100)},
		rama:  {nrKnob: ramaRestrictKnob(50)},
	}
	upperRule := restrictRule(string(rama), string(canon), nrKnob, true, 1, 0)
	lowerRule := lowerRestrictRule(string(rama), string(high), nrKnob, true, 10, 0)
	r := newRestrictRegion(configapi.QoSRegionTypeShare,
		[]configapi.ControlKnobConstraints{upperRule, lowerRule})
	rec := &recordingMetrics{}
	r.emitter = rec
	got := r.applyBoundedControlKnobConstraints(cloneControlKnobs(origin))
	// Delivery stays raw (conflict fail-open).
	assert.Equal(t, 50.0, got[rama][nrKnob].Value)

	var conflictCount int
	for i := range rec.entries {
		if rec.entries[i].key == metricControlKnobRestrictUpperBound &&
			tagVal(rec.entries[i].tags, "action") == restrictActionConflictingBounds {
			conflictCount++
			assert.Equal(t, 50.0, rec.entries[i].val)
		}
	}
	assert.Equal(t, 1, conflictCount, "exactly one conflicting-bounds observation")
}

// TestApplyBoundedAdversarialUpperLowerInversion proves the aggregation rule:
// every enabled bound of a successful enforce result is composed regardless of
// whether raw currently overshoots it. Here an upper rule anchored on ref=30
// (upper bound 40) and a lower rule anchored on ref=120 (lower bound 60) invert
// across references: 60 > 40. Whether raw sits below both (30) or above both
// (70), only one side would overshoot under the old overshoot-gated aggregation
// and clamp; composing all enabled bounds surfaces the conflict either way and
// fails open (raw stands, one conflicting-bounds observation).
func TestApplyBoundedAdversarialUpperLowerInversion(t *testing.T) {
	const (
		rama   = types.CPUProvisionPolicyRama
		canon  = types.CPUProvisionPolicyCanonical
		high   = types.CPUProvisionPolicyName("high-ref")
		nrKnob = configapi.ControlKnobNonReclaimedCPURequirement
	)
	tagVal := func(tags []metrics.MetricTag, key string) string {
		for _, tg := range tags {
			if tg.Key == key {
				return tg.Val
			}
		}
		return ""
	}
	for _, raw := range []float64{30, 70} {
		origin := map[types.CPUProvisionPolicyName]types.ControlKnob{
			canon: {nrKnob: ramaRestrictKnob(30)},
			high:  {nrKnob: ramaRestrictKnob(120)},
			rama:  {nrKnob: ramaRestrictKnob(raw)},
		}
		upperRule := restrictRule(string(rama), string(canon), nrKnob, true, 10, 0)     // upper=40
		lowerRule := lowerRestrictRule(string(rama), string(high), nrKnob, true, 60, 0) // lower=60
		r := newRestrictRegion(configapi.QoSRegionTypeShare,
			[]configapi.ControlKnobConstraints{upperRule, lowerRule})
		rec := &recordingMetrics{}
		r.emitter = rec
		got := r.applyBoundedControlKnobConstraints(cloneControlKnobs(origin))
		assert.Equal(t, raw, got[rama][nrKnob].Value, "raw=%.0f must fail open on conflict", raw)

		conflictCount := 0
		for i := range rec.entries {
			if rec.entries[i].key == metricControlKnobRestrictUpperBound &&
				tagVal(rec.entries[i].tags, "action") == restrictActionConflictingBounds {
				conflictCount++
				assert.Equal(t, raw, rec.entries[i].val)
			}
		}
		assert.Equal(t, 1, conflictCount, "raw=%.0f: exactly one conflicting-bounds observation", raw)
	}
}

// TestApplyBoundedAggregateEqualityNoConflict proves maxLower == minUpper is NOT a
// conflict (the aggregator uses strict >). An upper rule anchored on ref=10
// (upper bound 15) and a lower rule anchored on ref=25 (lower bound 15) meet at
// exactly 15: delivery clamps to the shared bound 15 and no conflicting-bounds
// observation is emitted.
func TestApplyBoundedAggregateEqualityNoConflict(t *testing.T) {
	const (
		rama   = types.CPUProvisionPolicyRama
		nrKnob = configapi.ControlKnobNonReclaimedCPURequirement
	)
	tagVal := func(tags []metrics.MetricTag, key string) string {
		for _, tg := range tags {
			if tg.Key == key {
				return tg.Val
			}
		}
		return ""
	}
	origin := map[types.CPUProvisionPolicyName]types.ControlKnob{
		types.CPUProvisionPolicyName("ref-a"): {nrKnob: ramaRestrictKnob(10)},
		types.CPUProvisionPolicyName("ref-b"): {nrKnob: ramaRestrictKnob(25)},
		rama:                                  {nrKnob: ramaRestrictKnob(20)},
	}
	upperRule := restrictRule(string(rama), "ref-a", nrKnob, true, 5, 0)       // upper = 10+5 = 15
	lowerRule := lowerRestrictRule(string(rama), "ref-b", nrKnob, true, 10, 0) // lower = 25-10 = 15
	r := newRestrictRegion(configapi.QoSRegionTypeShare,
		[]configapi.ControlKnobConstraints{upperRule, lowerRule})
	rec := &recordingMetrics{}
	r.emitter = rec
	got := r.applyBoundedControlKnobConstraints(cloneControlKnobs(origin))
	assert.Equal(t, 15.0, got[rama][nrKnob].Value, "clamp to shared bound 15")

	for i := range rec.entries {
		assert.NotEqual(t, restrictActionConflictingBounds,
			tagVal(rec.entries[i].tags, "action"),
			"no conflicting-bounds observation when maxLower==minUpper")
	}
}

// TestApplyBoundedCompositionAllBoundsComposed proves an in-band upper bound still
// participates in composition (overshoot only selects the metric action, not the
// aggregate). Three enforce upper rules anchored on distinct references produce
// bounds 12, 15, and 20 on the same target/knob; raw=14 overshoots only the
// tightest 12, yet min(12,15,20)=12 clamps and the within-band rules do not loosen it.
func TestApplyBoundedCompositionAllBoundsComposed(t *testing.T) {
	const (
		rama   = types.CPUProvisionPolicyRama
		nrKnob = configapi.ControlKnobNonReclaimedCPURequirement
	)
	origin := map[types.CPUProvisionPolicyName]types.ControlKnob{
		types.CPUProvisionPolicyName("ref-a"): {nrKnob: ramaRestrictKnob(10)},
		types.CPUProvisionPolicyName("ref-b"): {nrKnob: ramaRestrictKnob(10)},
		types.CPUProvisionPolicyName("ref-c"): {nrKnob: ramaRestrictKnob(10)},
		rama:                                  {nrKnob: ramaRestrictKnob(14)},
	}
	rules := []configapi.ControlKnobConstraints{
		restrictRule(string(rama), "ref-a", nrKnob, true, 2, 0),  // upper=12, overshoots raw=14
		restrictRule(string(rama), "ref-b", nrKnob, true, 5, 0),  // upper=15, within band
		restrictRule(string(rama), "ref-c", nrKnob, true, 10, 0), // upper=20, within band
	}
	r := newRestrictRegion(configapi.QoSRegionTypeShare, rules)
	got := r.applyBoundedControlKnobConstraints(origin)
	assert.Equal(t, 12.0, got[rama][nrKnob].Value)
}

// TestRestrictDynamicHotSwitch verifies AQC disabled -> shadow -> enforce -> delete.
func TestRestrictDynamicHotSwitch(t *testing.T) {
	const (
		rama   = types.CPUProvisionPolicyRama
		canon  = types.CPUProvisionPolicyCanonical
		nrKnob = configapi.ControlKnobNonReclaimedCPURequirement
	)
	origin := func() map[types.CPUProvisionPolicyName]types.ControlKnob {
		return map[types.CPUProvisionPolicyName]types.ControlKnob{
			canon: {nrKnob: ramaRestrictKnob(10)},
			rama:  {nrKnob: ramaRestrictKnob(30)},
		}
	}
	run := func(rules []configapi.ControlKnobConstraints) float64 {
		r := newRestrictRegion(configapi.QoSRegionTypeShare, rules)
		return r.applyBoundedControlKnobConstraints(origin())[rama][nrKnob].Value
	}
	assert.Equal(t, 30.0, run(nil))
	assert.Equal(t, 30.0, run([]configapi.ControlKnobConstraints{
		restrictRule(string(rama), string(canon), nrKnob, false, 2, 0)}))
	assert.Equal(t, 12.0, run([]configapi.ControlKnobConstraints{
		restrictRule(string(rama), string(canon), nrKnob, true, 2, 0)}))
	assert.Equal(t, 30.0, run(nil))
}

// TestRestrictShareProductionWiring verifies share order exact-path then bounded rules and
// exact-path quota->dynamic-quota untouched; dedicated/isolation do not wire generic rules.
func TestRestrictShareProductionWiring(t *testing.T) {
	const (
		rama      = types.CPUProvisionPolicyRama
		canon     = types.CPUProvisionPolicyCanonical
		dynP      = types.CPUProvisionPolicyDynamicQuota
		nrKnob    = configapi.ControlKnobNonReclaimedCPURequirement
		quotaKnob = configapi.ControlKnobReclaimedCoresCPUQuota
	)

	conf, err := options.NewOptions().Config()
	require.NoError(t, err)
	stateFileDir := "stateFileDir" + uuid.New().String()
	checkpointDir := "checkpointDir" + uuid.New().String()
	conf.GenericSysAdvisorConfiguration.StateFileDirectory = stateFileDir
	conf.MetaServerConfiguration.CheckpointManagerDir = checkpointDir
	t.Cleanup(func() { os.RemoveAll(stateFileDir); os.RemoveAll(checkpointDir) })
	conf.ExactRestrictRefPolicy = map[types.CPUProvisionPolicyName]types.CPUProvisionPolicyName{
		rama: dynP,
	}

	genericCtx, err := katalyst_base.GenerateFakeGenericContext([]runtime.Object{})
	require.NoError(t, err)
	reclaim.UnregisterConsumer(reclaim.GenericConsumerName)
	t.Cleanup(func() { reclaim.UnregisterConsumer(reclaim.GenericConsumerName) })
	metaServer, err := metaserver.NewMetaServer(genericCtx.Client, metrics.DummyMetrics{}, conf)
	require.NoError(t, err)
	metaCache, err := metacache.NewMetaCacheImp(conf, metricspool.DummyMetricsEmitterPool{}, metric.NewFakeMetricsFetcher(metrics.DummyMetrics{}))
	require.NoError(t, err)
	ci := types.ContainerInfo{QoSLevel: consts.PodAnnotationQoSLevelSharedCores, RegionNames: sets.NewString("share")}
	share := NewQoSRegionShare(&ci, conf, nil, commonstate.FakedNUMAID, metaCache, metaServer, metrics.DummyMetrics{}).(*QoSRegionShare)

	raw := map[types.CPUProvisionPolicyName]types.ControlKnob{
		rama:  {nrKnob: ramaRestrictKnob(30), quotaKnob: ramaRestrictKnob(20)},
		canon: {nrKnob: ramaRestrictKnob(10)},
		dynP:  {quotaKnob: ramaRestrictKnob(12)},
	}
	// nameOnlyQuota is the exact-path candidate constraint (neither policy set) for quota.
	nameOnlyQuota := configapi.ControlKnobConstraints{Name: quotaKnob}
	run := func(rules []configapi.ControlKnobConstraints) map[types.CPUProvisionPolicyName]types.ControlKnob {
		dyn := dynamic.NewConfiguration()
		// Always include the exact-path name-only quota constraint so the exact path has a knob to clamp.
		dyn.CPUProvisionConfiguration.ControlKnobConstraints = append(
			append([]configapi.ControlKnobConstraints{}, nameOnlyQuota), rules...)
		share.DynamicConfiguration = dyn
		out := share.restrictControlKnobByExactPolicy(raw)
		return share.applyBoundedControlKnobConstraints(out)
	}

	// exact path clamps quota (name-only); bounded path has no explicit rule, so nrKnob stays raw.
	got := run(nil)
	assert.Equal(t, 12.0, got[rama][quotaKnob].Value)
	assert.Equal(t, 30.0, got[rama][nrKnob].Value)
	// enforce: exact path clamps quota 20->12, bounded explicit rule clamps non-reclaimed 30->12.
	got = run([]configapi.ControlKnobConstraints{
		restrictRule(string(rama), string(canon), nrKnob, true, 2, 0),
	})
	assert.Equal(t, 12.0, got[rama][quotaKnob].Value)
	assert.Equal(t, 12.0, got[rama][nrKnob].Value)
	assert.Equal(t, 10.0, got[canon][nrKnob].Value)
	assert.Equal(t, 12.0, got[dynP][quotaKnob].Value)
}

// TestApplyBoundedConstraintsKnobAgnostic proves the executor clamps an arbitrary
// non-exact-set knob on an arbitrary target/reference policy. A custom knob with no
// requests-floor validator clamps purely on the reference band.
func TestApplyBoundedConstraintsKnobAgnostic(t *testing.T) {
	const (
		canon      = types.CPUProvisionPolicyCanonical
		dynP       = types.CPUProvisionPolicyDynamicQuota
		customKnob = configapi.ControlKnobName("custom-test-knob")
	)
	origin := map[types.CPUProvisionPolicyName]types.ControlKnob{
		canon: {customKnob: ramaRestrictKnob(10)},
		dynP:  {customKnob: ramaRestrictKnob(30)},
	}
	// Clamp dynamic-quota's custom knob against canonical+gap=3: upper=13, raw=30 -> 13.
	rules := []configapi.ControlKnobConstraints{
		restrictRule(string(dynP), string(canon), customKnob, true, 3, 0),
	}
	r := newRestrictRegion(configapi.QoSRegionTypeShare, rules)
	got := r.applyBoundedControlKnobConstraints(origin)
	assert.Equal(t, 13.0, got[dynP][customKnob].Value)
	assert.Equal(t, 10.0, got[canon][customKnob].Value)
}

func TestGetEffectiveReclaimResource(t *testing.T) {
	t.Parallel()
	consumerNames := []string{
		reclaim.GenericConsumerName,
		"region-test-a",
		"region-test-b",
	}
	for _, name := range consumerNames {
		reclaim.UnregisterConsumer(name)
	}
	t.Cleanup(func() {
		for _, name := range consumerNames {
			reclaim.UnregisterConsumer(name)
		}
	})

	machineInfo := &machine.KatalystMachineInfo{
		CPUTopology: &machine.CPUTopology{
			CPUDetails: machine.CPUDetails{
				0: machine.CPUTopoInfo{NUMANodeID: 1},
			},
		},
	}
	reg := func(name, path string, pct float64) {
		c := config.NewConfiguration()
		c.BaseConfiguration.ReclaimRelativeRootCgroupPath = path
		c.BaseConfiguration.GenericReclaimedResourcePercentage = pct
		require.NoError(t, reclaim.RegisterNamedGenericConsumer(name, c, machineInfo))
	}
	reg("region-test-a", "/kubepods/besteffort", 0)
	reg("region-test-b", "/kubesandbox", 0)

	period := uint64(100000)
	stats := func(cores float64) *common.CPUStats {
		return &common.CPUStats{CpuPeriod: period, CpuQuota: int64(cores * float64(period))}
	}
	unlimited := &common.CPUStats{CpuPeriod: period, CpuQuota: common.CPUQuotaUnlimit}
	maxInt := &common.CPUStats{CpuPeriod: period, CpuQuota: math.MaxInt}

	tests := []struct {
		name          string
		isNumaBinding bool
		bindingNumas  machine.CPUSet
		gateEnabled   bool
		// cpuStats maps a reclaim cgroup path to its stats; a missing key makes
		// the mocked GetCPUWithRelativePath return an error for that path.
		cpuStats     map[string]*common.CPUStats
		reclaimPool  *types.PoolInfo
		expectQuota  float64
		expectCpuset int
	}{
		{
			name:         "non-binding sums finite quotas across consumers",
			gateEnabled:  true,
			cpuStats:     map[string]*common.CPUStats{"/kubepods/besteffort": stats(1.0), "/kubesandbox": stats(2.0)},
			expectQuota:  3.0,
			expectCpuset: 0,
		},
		{
			name:         "non-binding any unlimited path makes whole scope unlimited",
			gateEnabled:  true,
			cpuStats:     map[string]*common.CPUStats{"/kubepods/besteffort": stats(1.0), "/kubesandbox": unlimited},
			expectQuota:  common.CPUQuotaUnlimit,
			expectCpuset: 0,
		},
		{
			name:         "non-binding math.MaxInt is treated as unlimited",
			gateEnabled:  true,
			cpuStats:     map[string]*common.CPUStats{"/kubepods/besteffort": maxInt, "/kubesandbox": stats(2.0)},
			expectQuota:  common.CPUQuotaUnlimit,
			expectCpuset: 0,
		},
		{
			name:         "gate disabled yields unlimited regardless of cgroup values",
			gateEnabled:  false,
			cpuStats:     map[string]*common.CPUStats{"/kubepods/besteffort": stats(1.0), "/kubesandbox": stats(2.0)},
			expectQuota:  common.CPUQuotaUnlimit,
			expectCpuset: 0,
		},
		{
			name:         "all paths missing yields unlimited",
			gateEnabled:  true,
			cpuStats:     map[string]*common.CPUStats{},
			expectQuota:  common.CPUQuotaUnlimit,
			expectCpuset: 0,
		},
		{
			name:         "missing path is skipped and the remainder is summed",
			gateEnabled:  true,
			cpuStats:     map[string]*common.CPUStats{"/kubesandbox": stats(2.0)},
			expectQuota:  2.0,
			expectCpuset: 0,
		},
		{
			name:          "numa-binding resolves per-numa paths, sums quota and reads cpuset",
			isNumaBinding: true,
			bindingNumas:  machine.NewCPUSet(1),
			gateEnabled:   true,
			cpuStats:      map[string]*common.CPUStats{"/kubepods/besteffort-1": stats(1.0), "/kubesandbox-1": stats(1.5)},
			reclaimPool: &types.PoolInfo{
				PoolName: commonstate.PoolNameReclaim,
				TopologyAwareAssignments: types.TopologyAwareAssignment{
					1: machine.NewCPUSet(10, 11, 12),
				},
			},
			expectQuota:  2.5,
			expectCpuset: 3,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			metaCache := metacache.NewDummyMetaCacheImp()
			if tt.reclaimPool != nil {
				require.NoError(t, metaCache.SetPoolInfo(commonstate.PoolNameReclaim, tt.reclaimPool))
			}

			r := &QoSRegionBase{
				isNumaBinding: tt.isNumaBinding,
				bindingNumas:  tt.bindingNumas,
				metaReader:    metaCache,
				isQuotaCtrlKnobEnabled: func(metacache.MetaReader) (bool, error) {
					return tt.gateEnabled, nil
				},
				getCPUWithRelativePath: func(path string) (*common.CPUStats, error) {
					if s, ok := tt.cpuStats[path]; ok {
						return s, nil
					}
					return nil, fmt.Errorf("cgroup not found: %s", path)
				},
			}

			quota, cpusetSize, err := r.getEffectiveReclaimResource()
			require.NoError(t, err)
			assert.InDelta(t, tt.expectQuota, quota, 1e-9)
			assert.Equal(t, tt.expectCpuset, cpusetSize)
		})
	}
}

func TestQoSRegionShareBootstrapEffectiveControlKnobWhenDefaultShareBackfillEnabled(t *testing.T) {
	t.Parallel()

	conf, err := options.NewOptions().Config()
	require.NoError(t, err)
	dynamicConf := conf.GetDynamicConfiguration()
	dynamicConf.EnableReclaim = true
	dynamicConf.FillDefaultSharePoolWithNonReclaimCPUs = true
	dynamicConf.AllowSharedCoresOverlapReclaimedCores = false
	dynamicConf.DisableDedicatedCoresOverlapReclaimedCores = true

	metaCache := metacache.NewDummyMetaCacheImp()
	require.NoError(t, metaCache.SetPoolInfo(commonstate.PoolNameShare, &types.PoolInfo{
		PoolName: commonstate.PoolNameShare,
		TopologyAwareAssignments: types.TopologyAwareAssignment{
			0: machine.NewCPUSet(),
		},
	}))
	require.NoError(t, metaCache.SetPoolInfo(commonstate.PoolNameReserve, &types.PoolInfo{
		PoolName: commonstate.PoolNameReserve,
		TopologyAwareAssignments: types.TopologyAwareAssignment{
			0: machine.NewCPUSet(0),
		},
	}))
	require.NoError(t, metaCache.SetPoolInfo(commonstate.PoolNameReclaim, &types.PoolInfo{
		PoolName: commonstate.PoolNameReclaim,
		TopologyAwareAssignments: types.TopologyAwareAssignment{
			0: machine.NewCPUSet(1, 2),
		},
	}))

	share := &QoSRegionShare{
		QoSRegionBase: &QoSRegionBase{
			ownerPoolName: commonstate.PoolNameShare,
			conf:          conf,
			metaReader:    metaCache,
			isQuotaCtrlKnobEnabled: func(metacache.MetaReader) (bool, error) {
				return false, nil
			},
			ResourceEssentials: types.ResourceEssentials{
				DynamicConfiguration: dynamicConf,
				ResourceUpperBound:   5,
			},
		},
	}

	controlKnobs := share.getEffectiveControlKnobs()
	require.NotNil(t, controlKnobs)
	require.Contains(t, controlKnobs, configapi.ControlKnobNonReclaimedCPURequirement)
	require.Equal(t, float64(5), controlKnobs[configapi.ControlKnobNonReclaimedCPURequirement].Value)
}

func TestQoSRegionShareBootstrapUsesStaticReclaimReserveWhenReclaimDisabled(t *testing.T) {
	t.Parallel()

	conf, err := options.NewOptions().Config()
	require.NoError(t, err)
	dynamicConf := conf.GetDynamicConfiguration()
	dynamicConf.EnableReclaim = false
	dynamicConf.FillDefaultSharePoolWithNonReclaimCPUs = true
	dynamicConf.AllowSharedCoresOverlapReclaimedCores = false
	dynamicConf.DisableDedicatedCoresOverlapReclaimedCores = true

	metaCache := metacache.NewDummyMetaCacheImp()
	require.NoError(t, metaCache.SetPoolInfo(commonstate.PoolNameShare, &types.PoolInfo{
		PoolName: commonstate.PoolNameShare,
		TopologyAwareAssignments: types.TopologyAwareAssignment{
			0: machine.NewCPUSet(),
		},
	}))
	require.NoError(t, metaCache.SetPoolInfo(commonstate.PoolNameReserve, &types.PoolInfo{
		PoolName: commonstate.PoolNameReserve,
		TopologyAwareAssignments: types.TopologyAwareAssignment{
			0: machine.NewCPUSet(0),
		},
	}))
	require.NoError(t, metaCache.SetPoolInfo(commonstate.PoolNameReclaim, &types.PoolInfo{
		PoolName: commonstate.PoolNameReclaim,
		TopologyAwareAssignments: types.TopologyAwareAssignment{
			0: machine.NewCPUSet(1, 2, 3, 4, 5, 6),
		},
	}))

	share := &QoSRegionShare{
		QoSRegionBase: &QoSRegionBase{
			ownerPoolName: commonstate.PoolNameShare,
			conf:          conf,
			metaReader:    metaCache,
			isQuotaCtrlKnobEnabled: func(metacache.MetaReader) (bool, error) {
				return false, nil
			},
			ResourceEssentials: types.ResourceEssentials{
				DynamicConfiguration: dynamicConf,
				ResourceUpperBound:   5,
				ReservedForReclaim:   2,
			},
		},
	}

	controlKnobs := share.getEffectiveControlKnobs()
	require.NotNil(t, controlKnobs)
	require.Contains(t, controlKnobs, configapi.ControlKnobNonReclaimedCPURequirement)
	require.Equal(t, float64(5), controlKnobs[configapi.ControlKnobNonReclaimedCPURequirement].Value)
}

func TestQoSRegionShareDoesNotBootstrapNonDefaultSharePools(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		ownerPoolName string
		isNumaBinding bool
	}{
		{
			name:          "custom share",
			ownerPoolName: "custom-share",
		},
		{
			name:          "share numa binding",
			ownerPoolName: commonstate.PoolNameShare + commonstate.NUMAPoolInfix + "0",
			isNumaBinding: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			conf, err := options.NewOptions().Config()
			require.NoError(t, err)
			dynamicConf := conf.GetDynamicConfiguration()
			dynamicConf.FillDefaultSharePoolWithNonReclaimCPUs = true
			dynamicConf.AllowSharedCoresOverlapReclaimedCores = false

			metaCache := metacache.NewDummyMetaCacheImp()
			require.NoError(t, metaCache.SetPoolInfo(tt.ownerPoolName, &types.PoolInfo{
				PoolName: tt.ownerPoolName,
				TopologyAwareAssignments: types.TopologyAwareAssignment{
					0: machine.NewCPUSet(),
				},
			}))

			share := &QoSRegionShare{
				QoSRegionBase: &QoSRegionBase{
					ownerPoolName: tt.ownerPoolName,
					conf:          conf,
					metaReader:    metaCache,
					isNumaBinding: tt.isNumaBinding,
					bindingNumas:  machine.NewCPUSet(0),
					isQuotaCtrlKnobEnabled: func(metacache.MetaReader) (bool, error) {
						return false, nil
					},
					ResourceEssentials: types.ResourceEssentials{
						DynamicConfiguration: dynamicConf,
						ResourceUpperBound:   5,
					},
				},
			}

			require.Nil(t, share.getEffectiveControlKnobs())
		})
	}
}

// boundedExpectedTagKeys is the exact seven-key schema every bounded restrict
// observation carries, on both upper and lower gauges.
var boundedExpectedTagKeys = []string{
	metricTagKeyRegionType,
	metricTagKeyRegionName,
	"target_policy",
	"reference_policy",
	"control_knob",
	"mode",
	"action",
}

func tagKeySet(tags []metrics.MetricTag) map[string]string {
	out := make(map[string]string, len(tags))
	for _, tg := range tags {
		out[tg.Key] = tg.Val
	}
	return out
}

// TestApplyBoundedSchemaIsUnifiedAndRegionNameDistinguishesSameTypeRegions proves:
//   - every observation carries exactly the seven fixed keys;
//   - upper and lower gauges share the identical key set;
//   - two regions of the same regionType but different name emit distinct
//     region_name values (otherwise same-type regions on one node would alias);
//   - a target knob that is absent from the target policy's map emits
//     action=target-unavailable (no passthrough fallback).
func TestApplyBoundedSchemaIsUnifiedAndRegionNameDistinguishesSameTypeRegions(t *testing.T) {
	const (
		rama   = types.CPUProvisionPolicyRama
		canon  = types.CPUProvisionPolicyCanonical
		nrKnob = configapi.ControlKnobNonReclaimedCPURequirement
	)
	origin := map[types.CPUProvisionPolicyName]types.ControlKnob{
		canon: {nrKnob: ramaRestrictKnob(10)},
		rama:  {nrKnob: ramaRestrictKnob(20)},
	}
	// Both upper and lower bands on, enforce, raw=20 overshoots upper (upper=12)
	// but not lower (lower=8).
	upperGap := 2.0
	lowerGap := 2.0
	rule := configapi.ControlKnobConstraints{
		Name:            nrKnob,
		TargetPolicy:    configapi.CPUProvisionPolicyName(rama),
		ReferencePolicy: configapi.CPUProvisionPolicyName(canon),
		RestrictConstraints: configapi.RestrictConstraints{
			MaxUpperGap: &upperGap,
			MaxLowerGap: &lowerGap,
		},
	}
	enforce := true
	rule.Enforce = &enforce

	runRegion := func(name string) *recordingEmitter {
		r := newRestrictRegion(configapi.QoSRegionTypeShare,
			[]configapi.ControlKnobConstraints{rule})
		r.name = name
		em := &recordingEmitter{}
		r.emitter = em
		r.applyBoundedControlKnobConstraints(cloneControlKnobs(origin))
		return em
	}

	regionA := runRegion("region-a")
	regionB := runRegion("region-b")
	require.Len(t, regionA.records, 2, "one upper + one lower observation")
	require.Len(t, regionB.records, 2)

	for i, rec := range regionA.records {
		keys := tagKeySet(rec)
		// Full seven-key set present.
		for _, want := range boundedExpectedTagKeys {
			_, ok := keys[want]
			require.True(t, ok, "record %d missing expected key %q; got %v", i, want, keys)
		}
		// No extra keys (exactly seven).
		require.Len(t, keys, len(boundedExpectedTagKeys), "record %d has unexpected extra keys: %v", i, keys)
	}

	// Upper and lower share the identical key set (only metric name, value, and
	// per-side action/reason differ).
	upperKeysA := tagKeySet(regionA.records[0])
	lowerKeysA := tagKeySet(regionA.records[1])
	require.Len(t, upperKeysA, len(boundedExpectedTagKeys))
	require.Len(t, lowerKeysA, len(boundedExpectedTagKeys))
	for k := range upperKeysA {
		_, ok := lowerKeysA[k]
		require.True(t, ok, "upper key %q missing on lower gauge", k)
	}

	// region_name distinguishes the two same-type regions.
	keysA0 := tagKeySet(regionA.records[0])
	keysB0 := tagKeySet(regionB.records[0])
	require.Equal(t, "region-a", keysA0[metricTagKeyRegionName])
	require.Equal(t, "region-b", keysB0[metricTagKeyRegionName])
	require.Equal(t, string(configapi.QoSRegionTypeShare), keysA0[metricTagKeyRegionType])
	require.Equal(t, string(configapi.QoSRegionTypeShare), keysB0[metricTagKeyRegionType])
	require.NotEqual(t, keysA0[metricTagKeyRegionName], keysB0[metricTagKeyRegionName],
		"region_name must differ for two same-type regions")

	// Target knob missing from the target policy's map: fail-open with
	// action=target-unavailable (not passthrough, not observed).
	missingKnob := configapi.ControlKnobName("absent-knob")
	missingRule := configapi.ControlKnobConstraints{
		Name:            missingKnob,
		TargetPolicy:    configapi.CPUProvisionPolicyName(rama),
		ReferencePolicy: configapi.CPUProvisionPolicyName(canon),
		RestrictConstraints: configapi.RestrictConstraints{
			MaxUpperGap: &upperGap,
		},
	}
	rMissing := newRestrictRegion(configapi.QoSRegionTypeShare,
		[]configapi.ControlKnobConstraints{missingRule})
	emMissing := &recordingEmitter{}
	rMissing.emitter = emMissing
	rMissing.applyBoundedControlKnobConstraints(cloneControlKnobs(origin))
	require.Len(t, emMissing.records, 1, "fail-open emits exactly one upper observation")
	missingKeys := tagKeySet(emMissing.records[0])
	require.Equal(t, restrictActionTargetUnavailable, missingKeys["action"])
	require.Equal(t, string(configapi.QoSRegionTypeShare), missingKeys[metricTagKeyRegionType])
}
