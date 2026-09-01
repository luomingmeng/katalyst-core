# Dedicated Region Sched-Wait Indicator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in, region-scoped, freshness-aware `cpu_sched_wait` current-value getter for `QoSRegionDedicated` without changing default configuration or allowing one unavailable metric to discard other valid indicators.

**Architecture:** Resolve a dedicated region's effective CPU set from current MetaCache container assignments and its binding NUMAs. Average per-CPU sched-wait through freshness-validating `GetCPUMetric` reads, represent complete sample absence with a private sentinel error, and let `getIndicators` omit only that unavailable indicator. Keep numeric validation centralized so sched-wait accepts a real zero while all existing indicators retain their current positive-value contract. Preserve that zero through Region and Rama, and let the generic PID controller own the logarithmic-domain conversion required for finite bounded output.

**Tech Stack:** Go 1.18, Katalyst sys-advisor region framework, MetaCache, MetaServer metrics reader, `machine.CPUSet`, `testify/require`.

---

## Preconditions

- Work only in the existing worktree for `feat/default-share-residual-backfill`.
- Start from design commit `852eb6c69`.
- Do not modify the default dedicated `RegionIndicatorTargetConfiguration`.
- Do not change `MetricsFetcher`, Malachite, `katalyst-api`, or `katalyst-adapter`.
- Keep production code and tests in separate commits.
- Preserve parallel-safe tests; do not use package-level mutable mocks or monkey patching.

## File Map

- Create: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/indicator.go`
  - Own the private unavailable-indicator error, freshness-aware per-CPU average helper, and indicator-specific numeric validation.
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/region_base.go`
  - Isolate unavailable getter failures and delegate current/target validation to the new helper.
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/region_dedicated.go`
  - Register sched-wait, centralize dedicated CPU-set resolution, implement the getter, and reuse CPU-set resolution for CPU usage.
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/resource/helper/pid.go`
  - Apply a bounded negative adjustment for an exact zero and rebuild the derivative baseline on the next positive sample, without adding Region-specific branches to Rama.
- Create: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/indicator_test.go`
  - Cover aggregation freshness, partial availability, zero semantics, and per-indicator failure isolation.
- Create: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/region_dedicated_test.go`
  - Cover constructor registration and dedicated NUMA/non-NUMA CPU scope.

## Task Summary

| Priority | Task | Estimate | Evidence |
|---|---|---:|---|
| P0 | Add focused failing tests | 1.5 h | New tests fail for missing symbols/behavior |
| P0 | Implement aggregation and isolation | 1.5 h | Focused tests pass |
| P0 | Implement Dedicated getter and CPU scope | 1.0 h | Dedicated tests pass |
| P1 | Split commits and run regression suite | 1.0 h | Clean diff, race test and CPU advisor tests pass |

Estimated total: 5 hours, excluding CI queue time and optional node-level verification.

### Task 1: Add Failing Indicator Tests

**Files:**
- Create: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/indicator_test.go`
- Create: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/region_dedicated_test.go`

- [ ] **Step 1: Add a reusable test fixture**

Create the following helper in `region_dedicated_test.go`. It uses dependency
injection through MetaCache and `FakeMetricsFetcher`, so every subtest owns its
state and can run in parallel.

```go
package region

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	configapi "github.com/kubewharf/katalyst-api/pkg/apis/config/v1alpha1"
	workloadapis "github.com/kubewharf/katalyst-api/pkg/apis/workload/v1alpha1"
	"github.com/kubewharf/katalyst-core/cmd/katalyst-agent/app/options"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/metacache"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/types"
	"github.com/kubewharf/katalyst-core/pkg/config"
	"github.com/kubewharf/katalyst-core/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent"
	agentmetric "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	utilmetric "github.com/kubewharf/katalyst-core/pkg/util/metric"
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
		MetaAgent: &agent.MetaAgent{MetricsFetcher: fetcher},
	}
	container := &types.ContainerInfo{
		PodUID:                  "pod",
		ContainerName:           "main",
		OwnerPoolName:           "dedicated-pool",
		OriginOwnerPoolName:     "dedicated-pool",
		QoSLevel:                consts.PodAnnotationQoSLevelDedicatedCores,
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
```

Do not add shared package state.

- [ ] **Step 2: Test constructor registration**

Add:

```go
func TestNewQoSRegionDedicatedRegistersSchedWaitGetter(t *testing.T) {
	t.Parallel()

	f := newDedicatedIndicatorFixture(t, 0, types.TopologyAwareAssignment{
		0: machine.NewCPUSet(0, 1),
	})

	require.Contains(t, f.region.indicatorCurrentGetters,
		string(workloadapis.ServiceSystemIndicatorNameCPUSchedWait))
}
```

- [ ] **Step 3: Lock down the opt-in default**

Add:

```go
func TestDefaultDedicatedIndicatorsDoNotEnableSchedWait(t *testing.T) {
	t.Parallel()

	conf, err := options.NewOptions().Config()
	require.NoError(t, err)

	for _, indicator := range conf.GetDynamicConfiguration().
		RegionIndicatorTargetConfiguration[configapi.QoSRegionTypeDedicated] {
		require.NotEqual(t, workloadapis.ServiceSystemIndicatorNameCPUSchedWait, indicator.Name)
	}
}
```

- [ ] **Step 4: Test dedicated CPU scope**

Add table-driven coverage that calls the private CPU-set resolver:

```go
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
```

- [ ] **Step 5: Test per-CPU aggregation**

Start `indicator_test.go` with:

```go
package region

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	workloadapis "github.com/kubewharf/katalyst-api/pkg/apis/workload/v1alpha1"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/types"
	"github.com/kubewharf/katalyst-core/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	utilmetric "github.com/kubewharf/katalyst-core/pkg/util/metric"
)
```

Then test `getAverageCoreMetric` with independent fixtures:

```go
func TestQoSRegionBaseGetAverageCoreMetric(t *testing.T) {
	t.Parallel()

	now := time.Now()
	expired := now.Add(-2 * time.Minute)
	tests := []struct {
		name      string
		cpus      machine.CPUSet
		samples   map[int]utilmetric.MetricData
		want      float64
		wantError bool
	}{
		{
			name: "averages valid samples",
			cpus: machine.NewCPUSet(0, 1),
			samples: map[int]utilmetric.MetricData{
				0: {Value: 100, Time: &now},
				1: {Value: 300, Time: &now},
			},
			want: 200,
		},
		{
			name: "keeps a measured zero",
			cpus: machine.NewCPUSet(0),
			samples: map[int]utilmetric.MetricData{
				0: {Value: 0, Time: &now},
			},
			want: 0,
		},
		{
			name: "uses valid subset",
			cpus: machine.NewCPUSet(0, 1, 2),
			samples: map[int]utilmetric.MetricData{
				0: {Value: 100, Time: &now},
				2: {Value: 300, Time: &now},
			},
			want: 200,
		},
		{
			name: "rejects empty set",
			cpus: machine.NewCPUSet(),
			wantError: true,
		},
		{
			name: "rejects no valid samples",
			cpus: machine.NewCPUSet(0, 1),
			samples: map[int]utilmetric.MetricData{
				0: {Value: math.NaN(), Time: &now},
				1: {Value: 100, Time: &expired},
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newDedicatedIndicatorFixture(t, 0, types.TopologyAwareAssignment{
				0: tt.cpus,
			})
			for cpu, sample := range tt.samples {
				f.fetcher.SetCPUMetric(cpu, consts.MetricCPUSchedwait, sample)
			}

			got, err := f.region.getAverageCoreMetric(tt.cpus, consts.MetricCPUSchedwait)
			if tt.wantError {
				require.ErrorIs(t, err, errIndicatorUnavailable)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
```

The fake fetcher's minimum insurance period is one minute, so the two-minute
sample age deterministically exercises expiration.

- [ ] **Step 6: Test sibling isolation**

Add:

```go
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
```

- [ ] **Step 7: Test CPU-set deduplication**

Add a sidecar with the same topology assignment and verify union semantics:

```go
func TestQoSRegionDedicatedSchedWaitDeduplicatesContainerCPUAssignments(t *testing.T) {
	t.Parallel()

	assignments := types.TopologyAwareAssignment{
		0: machine.NewCPUSet(0, 1),
	}
	f := newDedicatedIndicatorFixture(t, 0, assignments)
	sidecar := &types.ContainerInfo{
		PodUID:                  f.container.PodUID,
		ContainerName:           "sidecar",
		OwnerPoolName:           f.container.OwnerPoolName,
		OriginOwnerPoolName:     f.container.OriginOwnerPoolName,
		QoSLevel:                f.container.QoSLevel,
		TopologyAwareAssignments: assignments,
	}
	require.NoError(t, f.cache.SetContainerInfo(sidecar.PodUID, sidecar.ContainerName, sidecar))
	require.NoError(t, f.region.AddContainer(sidecar))

	now := time.Now()
	f.fetcher.SetCPUMetric(0, consts.MetricCPUSchedwait, utilmetric.MetricData{Value: 10, Time: &now})
	f.fetcher.SetCPUMetric(1, consts.MetricCPUSchedwait, utilmetric.MetricData{Value: 30, Time: &now})

	got, err := f.region.getCPUSchedWait()

	require.NoError(t, err)
	require.Equal(t, 20.0, got)
}
```

- [ ] **Step 8: Test unavailable isolation and numeric validation**

In `region_dedicated_test.go`, configure sched-wait and CPU usage, then replace
the CPU usage getter with a deterministic closure:

```go
func TestQoSRegionDedicatedGetIndicatorsIsolatesUnavailableSchedWait(t *testing.T) {
	t.Parallel()

	f := newDedicatedIndicatorFixture(t, 0, types.TopologyAwareAssignment{
		0: machine.NewCPUSet(0),
	})
	dynamicConf := f.conf.GetDynamicConfiguration()
	dynamicConf.RegionIndicatorTargetConfiguration =
		map[configapi.QoSRegionType][]configapi.IndicatorTargetConfiguration{
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
	f.region.indicatorCurrentGetters[string(workloadapis.ServiceSystemIndicatorNameCPUUsageRatio)] =
		func() (float64, error) { return 0.5, nil }

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
```

In `indicator_test.go`, add direct table coverage for
`isIndicatorValueValid`:

```go
func TestIndicatorValueValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		indicator workloadapis.ServiceSystemIndicatorName
		current   float64
		target    float64
		wantValid bool
	}{
		{"sched wait zero", workloadapis.ServiceSystemIndicatorNameCPUSchedWait, 0, 460, true},
		{"sched wait negative", workloadapis.ServiceSystemIndicatorNameCPUSchedWait, -1, 460, false},
		{"sched wait nan", workloadapis.ServiceSystemIndicatorNameCPUSchedWait, math.NaN(), 460, false},
		{"sched wait infinity", workloadapis.ServiceSystemIndicatorNameCPUSchedWait, math.Inf(1), 460, false},
		{"usage zero preserves old behavior", workloadapis.ServiceSystemIndicatorNameCPUUsageRatio, 0, 0.55, false},
		{"usage positive", workloadapis.ServiceSystemIndicatorNameCPUUsageRatio, 0.5, 0.55, true},
		{"target must be positive", workloadapis.ServiceSystemIndicatorNameCPUUsageRatio, 0.5, 0, false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := isIndicatorValueValid(tt.indicator, types.IndicatorValue{
				Current: tt.current,
				Target:  tt.target,
			})
			require.Equal(t, tt.wantValid, got)
		})
	}
}
```

- [ ] **Step 9: Prove the tests fail for the intended reasons**

Run:

```bash
go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region \
  -run 'TestNewQoSRegionDedicatedRegistersSchedWaitGetter|TestDefaultDedicatedIndicatorsDoNotEnableSchedWait|TestQoSRegionDedicatedAssignedCPUSet|TestQoSRegionBaseGetAverageCoreMetric|TestQoSRegionDedicatedSchedWait|TestGetIndicators|TestIndicatorValueValid' \
  -count=1
```

Expected: compilation fails because `getAssignedCPUSet`,
`getAverageCoreMetric`, `errIndicatorUnavailable`, and
`isIndicatorValueValid` do not exist, or behavior assertions fail because
sched-wait is not registered and unavailable errors are not isolated.

Do not commit yet. Keep the failing tests in the working tree so the
implementation is developed against them.

### Task 2: Implement Common Indicator Semantics

**Files:**
- Create: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/indicator.go`
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/region_base.go`
- Test: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/indicator_test.go`

- [ ] **Step 1: Add the unavailable error and average helper**

Create `indicator.go`:

```go
package region

import (
	"errors"
	"fmt"
	"math"

	workloadapis "github.com/kubewharf/katalyst-api/pkg/apis/workload/v1alpha1"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/types"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

var errIndicatorUnavailable = errors.New("indicator unavailable")

func (r *QoSRegionBase) getAverageCoreMetric(cpuSet machine.CPUSet, metricName string) (float64, error) {
	if cpuSet.IsEmpty() {
		return 0, fmt.Errorf("%w: empty cpu set for metric %s", errIndicatorUnavailable, metricName)
	}

	var sum float64
	validSamples := 0
	for _, cpuID := range cpuSet.ToSliceInt() {
		data, err := r.metaServer.GetCPUMetric(cpuID, metricName)
		if err != nil || math.IsNaN(data.Value) || math.IsInf(data.Value, 0) {
			continue
		}
		sum += data.Value
		validSamples++
	}
	if validSamples == 0 {
		return 0, fmt.Errorf(
			"%w: no valid samples for metric %s on cpus %s",
			errIndicatorUnavailable,
			metricName,
			cpuSet.String(),
		)
	}
	return sum / float64(validSamples), nil
}

func isIndicatorValueValid(
	indicatorName workloadapis.ServiceSystemIndicatorName,
	value types.IndicatorValue,
) bool {
	if math.IsNaN(value.Current) || math.IsInf(value.Current, 0) ||
		math.IsNaN(value.Target) || math.IsInf(value.Target, 0) ||
		value.Target <= 0 {
		return false
	}
	if indicatorName == workloadapis.ServiceSystemIndicatorNameCPUSchedWait {
		return value.Current >= 0
	}
	return value.Current > 0
}
```

Keep the sentinel package-private. Do not add it to shared types or public
interfaces.

- [ ] **Step 2: Isolate unavailable indicators**

In `region_base.go`, alias the existing Kubernetes errors import and add the
standard library errors package:

```go
import (
	stderrors "errors"
	// ...
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)
```

Update the existing not-found check:

```go
if err != nil && !apierrors.IsNotFound(err) {
	return nil, err
}
```

Replace the getter error handling in `getIndicators` with:

```go
current, err := indicatorCurrentGetter()
if err != nil {
	if stderrors.Is(err, errIndicatorUnavailable) {
		general.Warningf(
			"skip unavailable indicator %s for region %s: %v",
			indicatorName,
			r.name,
			err,
		)
		continue
	}
	return nil, fmt.Errorf("get current indicator %s for region %s: %w", indicatorName, r.name, err)
}
```

Replace the current generic non-positive check with:

```go
indicatorValue := types.IndicatorValue{
	Current: current,
	Target:  target,
}
if !isIndicatorValueValid(indicatorName, indicatorValue) {
	klog.ErrorS(nil, "invalid indicator",
		"regionName", r.name,
		"indicatorName", indicatorName,
		"indicatorValue", indicatorValue,
	)
	continue
}
```

- [ ] **Step 3: Run common indicator tests**

Run:

```bash
go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region \
  -run 'TestQoSRegionBaseGetAverageCoreMetric|TestGetIndicators|TestIndicatorValueValid' \
  -count=1
```

Expected: common aggregation and validation tests pass. Dedicated registration
and getter tests may still fail until Task 3.

### Task 3: Implement Dedicated Sched-Wait

**Files:**
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/region_dedicated.go`
- Test: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/region_dedicated_test.go`

- [ ] **Step 1: Register the getter**

Extend the Dedicated getter map:

```go
r.indicatorCurrentGetters = map[string]types.IndicatorCurrentGetter{
	string(workloadapis.ServiceSystemIndicatorNameCPUSchedWait):     r.getCPUSchedWait,
	string(workloadapis.ServiceSystemIndicatorNameCPI):              r.getPodCPICurrent,
	string(workloadapis.ServiceSystemIndicatorNameCPUUsageRatio):    r.getCPUUsageRatio,
}
```

Run `gofmt` after implementation so alignment is produced by the formatter
rather than hand-maintained spacing.

- [ ] **Step 2: Extract the assigned CPU-set resolver**

Add:

```go
func (r *QoSRegionDedicated) getAssignedCPUSet() (machine.CPUSet, error) {
	cpuSet := machine.NewCPUSet()
	for podUID, containerSet := range r.podSet {
		for containerName := range containerSet {
			ci, ok := r.metaReader.GetContainerInfo(podUID, containerName)
			if !ok || ci == nil {
				return machine.NewCPUSet(), fmt.Errorf(
					"%w: container info not found for %s/%s",
					errIndicatorUnavailable,
					podUID,
					containerName,
				)
			}
			for numaID, assignedCPUs := range ci.TopologyAwareAssignments {
				if r.bindingNumas.Contains(numaID) {
					cpuSet = cpuSet.Union(assignedCPUs)
				}
			}
		}
	}
	if cpuSet.IsEmpty() {
		return machine.NewCPUSet(), fmt.Errorf(
			"%w: empty assigned cpu set for region %s with numas %s",
			errIndicatorUnavailable,
			r.name,
			r.bindingNumas.String(),
		)
	}
	return cpuSet, nil
}
```

This must read the latest MetaCache assignment rather than the cached
`containerTopologyAwareAssignment`.

- [ ] **Step 3: Add the sched-wait getter**

Add:

```go
func (r *QoSRegionDedicated) getCPUSchedWait() (float64, error) {
	cpuSet, err := r.getAssignedCPUSet()
	if err != nil {
		return 0, err
	}
	return r.getAverageCoreMetric(cpuSet, consts.MetricCPUSchedwait)
}
```

- [ ] **Step 4: Reuse CPU-set resolution for CPU usage**

Replace the duplicated traversal in `getCPUUsageRatio`:

```go
func (r *QoSRegionDedicated) getCPUUsageRatio() (float64, error) {
	cpuSet, err := r.getAssignedCPUSet()
	if err != nil {
		return 0, err
	}
	usageRatio := r.metaServer.AggregateCoreMetric(
		cpuSet,
		consts.MetricCPUUsageRatio,
		metric.AggregatorAvg,
	)
	return usageRatio.Value, nil
}
```

Do not migrate CPU usage to the new freshness-aware aggregator in this
change. That would alter an existing indicator's stale and missing-sample
behavior beyond the approved scope.

- [ ] **Step 5: Format and run all new tests**

Run:

```bash
gofmt -w \
  pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/indicator.go \
  pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/region_base.go \
  pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/region_dedicated.go \
  pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/indicator_test.go \
  pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/region_dedicated_test.go

go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region \
  -run 'TestNewQoSRegionDedicatedRegistersSchedWaitGetter|TestQoSRegionDedicatedAssignedCPUSet|TestQoSRegionBaseGetAverageCoreMetric|TestQoSRegionDedicatedSchedWait|TestGetIndicators|TestIndicatorValueValid' \
  -count=1
```

Expected: all named tests pass.

- [ ] **Step 6: Inspect the implementation diff**

Run:

```bash
git diff --check
git diff -- \
  pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/indicator.go \
  pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/region_base.go \
  pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/region_dedicated.go
```

Confirm:

- no default configuration changed;
- no Share getter changed;
- no public interface changed;
- errors are lowercase;
- no package-level mutable test dependency exists.

### Task 4: Commit Production and Tests Separately

**Files:**
- Production:
  - `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/indicator.go`
  - `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/region_base.go`
  - `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/region_dedicated.go`
- Tests:
  - `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/indicator_test.go`
  - `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/region_dedicated_test.go`

- [ ] **Step 1: Commit only production files**

The tests remain present in the working tree and have already passed against
the implementation, but stage only production files:

```bash
git add \
  pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/indicator.go \
  pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/region_base.go \
  pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/region_dedicated.go

git diff --cached --check
git commit \
  -m "feat(sysadvisor): support sched wait for dedicated regions" \
  -m "Resolve each dedicated region's current assigned CPU set and aggregate fresh per-CPU sched-wait samples without conflating missing data with a measured zero." \
  -m "Isolate temporarily unavailable indicators while preserving fail-fast handling for unexpected getter errors."
```

Expected: one production commit; both new test files remain untracked.

- [ ] **Step 2: Re-run focused tests after the production commit**

Run the same focused command while the test files remain in the working tree:

```bash
go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region \
  -run 'TestNewQoSRegionDedicatedRegistersSchedWaitGetter|TestQoSRegionDedicatedAssignedCPUSet|TestQoSRegionBaseGetAverageCoreMetric|TestQoSRegionDedicatedSchedWait|TestGetIndicators|TestIndicatorValueValid' \
  -count=1
```

Expected: PASS.

- [ ] **Step 3: Commit only tests**

```bash
git add \
  pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/indicator_test.go \
  pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/region_dedicated_test.go

git diff --cached --check
git commit \
  -m "test(sysadvisor): cover dedicated sched wait indicators" \
  -m "Cover NUMA-scoped and non-binding CPU selection, sibling isolation, deduplication, partial and expired samples, zero values, numeric rejection, and per-indicator failure isolation."
```

Expected: one test-only commit and no uncommitted files.

### Task 5: Regression and Quality Gates

**Files:**
- No new files.

- [ ] **Step 1: Run the complete region package**

```bash
go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region -count=1
```

Expected: PASS.

- [ ] **Step 2: Run the region package with the race detector**

```bash
go test -race ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region -count=1
```

Expected: PASS with no race reports.

- [ ] **Step 3: Run the CPU advisor package**

```bash
go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/... -count=1
```

Expected: PASS.

- [ ] **Step 4: Run static checks for touched packages**

```bash
go vet ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region
```

Expected: no diagnostics.

- [ ] **Step 5: Audit final history and scope**

```bash
git status --short --branch
git log --oneline -4
git diff 852eb6c69..HEAD --stat
git diff 852eb6c69..HEAD -- \
  pkg/config \
  pkg/metaserver/agent/metric \
  pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/region_share.go
```

Expected:

- clean worktree;
- one production commit followed by one test commit;
- changes limited to the three production and two test files listed above;
- the final scoped diff command prints nothing.

## Acceptance Criteria

- An explicitly configured dedicated `cpu_sched_wait` indicator reaches
  `ControlEssentials.Indicators`.
- Each NUMA-binding sibling observes only its assigned CPUs.
- A non-NUMA-binding dedicated region observes all assigned CPUs.
- Partial valid coverage produces an average over valid fresh samples.
- Complete absence omits only sched-wait.
- A measured zero is retained as valid.
- Existing indicator validation semantics remain unchanged for non-sched-wait
  indicators.
- Default configuration and Share behavior are unchanged.
- Focused, race, advisor, and vet checks pass.
- Production and test changes are separate commits.
