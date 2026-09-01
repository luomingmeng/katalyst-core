# Dedicated Region Sched-Wait Indicator Design

## Status

Discussion approved; pending written specification review.

## Context

The CPU advisor obtains current indicator values through region-specific
`indicatorCurrentGetters`. Share regions support `cpu_sched_wait`, but
dedicated regions currently expose only CPI and CPU usage ratio.

Malachite already stores sched-wait as the per-CPU metric
`cpu.schedwait.cpu`. No collection, API, or adapter change is required.

Dedicated NUMA-binding pods are represented by one sibling region per NUMA.
Each sibling must observe only the CPUs assigned to that region. A
non-NUMA-binding dedicated region must observe the pod's complete assigned
CPU set.

## Goals

- Allow `QoSRegionDedicated` to consume `cpu_sched_wait` when the indicator is
  explicitly configured for dedicated regions.
- Aggregate over the exact CPU set assigned to the current dedicated region.
- Accept a real sched-wait value of zero.
- Ignore missing or expired CPU samples when at least one valid sample exists.
- Skip only sched-wait when no valid sample exists.
- Reuse CPU-set resolution and metric aggregation logic for future dedicated
  per-CPU indicators.
- Preserve existing default behavior unless configuration explicitly enables
  dedicated sched-wait.

## Non-Goals

- Do not add sched-wait to the default dedicated indicator configuration.
- Do not change Share region indicator behavior.
- Do not change Malachite collection or metric units.
- Do not extend the public `MetricsFetcher` interface.
- Do not introduce last-value caching or a new feature gate.
- Do not redesign existing CPI aggregation.

## Existing Behavior

`QoSRegionShare.getPoolCPUSchedWait` obtains the owner pool CPU set and calls
`AggregateCoreMetric` with `AggregatorAvg`.

That path is unsuitable for the dedicated implementation:

- a dedicated region is scoped by pod assignment and, for NUMA-binding pods,
  by one sibling region's NUMA;
- `AggregateCoreMetric` silently ignores failed CPU reads;
- it does not expose the number of valid samples;
- an empty or completely missing sample set is indistinguishable from a real
  zero value;
- it reads the metric store directly, bypassing the expiration validation
  performed by `GetCPUMetric`.

`QoSRegionDedicated.getCPUUsageRatio` already derives the correct region CPU
scope by reading current container metadata, filtering assignments by
`bindingNumas`, and taking a CPU-set union. Copying that traversal into each
new per-CPU getter would create divergent ownership logic.

`QoSRegionBase.getIndicators` currently fails the complete indicator
collection when any getter returns an error. It also rejects every current
value less than or equal to zero. These behaviors would respectively enlarge
the failure domain of transient sched-wait unavailability and incorrectly
discard a valid sched-wait value of zero.

## Design

### Dedicated CPU Scope

Add a private `QoSRegionDedicated` helper that resolves the CPUs assigned to
the current region.

The helper will:

1. Iterate over the region's current `podSet`.
2. Read the latest `ContainerInfo` for each container from MetaCache.
3. Select only topology assignments whose NUMA IDs are present in
   `bindingNumas`.
4. Union selected CPU sets so shared container assignments are not counted
   more than once.
5. Return an unavailable error when container metadata is missing or the
   resulting CPU set is empty.

For a NUMA-binding dedicated pod, `bindingNumas` contains the sibling
region's single NUMA. For a non-NUMA-binding dedicated pod, construction
populates `bindingNumas` with every NUMA found in the container assignment.
The same helper therefore implements both cases without separate branches.

`getCPUUsageRatio` will reuse this helper. Its normal aggregation semantics
remain unchanged.

### Fresh Metric Aggregation

Add a private region-level helper for averaging a named per-CPU metric over a
CPU set.

The helper will call `metaServer.GetCPUMetric` for each CPU rather than
`AggregateCoreMetric`. This preserves the configured metric expiration check
and makes valid-sample accounting explicit.

Aggregation rules:

- include every finite sample returned without error;
- skip missing, expired, NaN, and infinite samples;
- divide by the number of included samples, not the requested CPU count;
- return an unavailable error when no sample is included.

Partial coverage remains usable and does not bias the average with synthetic
zeroes. Complete absence is explicit and cannot be confused with a measured
zero.

### Getter Registration

Register a dedicated getter under
`ServiceSystemIndicatorNameCPUSchedWait`. The getter resolves the dedicated
region CPU set and averages `MetricCPUSchedwait`.

The default `RegionIndicatorTargetConfiguration` remains unchanged.
Dedicated sched-wait is therefore consumed only when an AdminQoSConfiguration
explicitly supplies a dedicated-region sched-wait target.

### Unavailable Indicator Semantics

Introduce a package-private sentinel error for an indicator that is
temporarily unavailable. CPU-scope and zero-valid-sample failures will wrap
this sentinel with region, metric, and CPU-set context.

`getIndicators` will treat this error specially:

- log the unavailable indicator with region context;
- continue collecting the remaining configured indicators;
- omit only the unavailable indicator from the current result.

All other getter errors retain the existing fail-fast behavior. This avoids
silently downgrading programming errors or unrelated data-source failures.

The implementation will not retain a previous sched-wait value. Reusing stale
control input would require an explicit age contract and would hide collection
outages.

### Indicator Validation

Current and target validation will reject NaN and infinite values before they
enter a provision policy.

Targets must remain strictly positive. Current values use indicator-specific
semantics:

- sched-wait is valid when it is greater than or equal to zero;
- existing indicators retain the current strictly-positive requirement.

This is intentionally narrow. It fixes sched-wait's valid zero without
changing historical behavior for CPI, CPU usage ratio, or memory latency.
The Region layer preserves the measured zero in `IndicatorValue`. The generic
PID controller owns its logarithmic-domain boundary: an exact zero produces a
bounded negative adjustment without entering `math.Log`, and the next positive
sample re-establishes the derivative baseline. Rama does not branch on Region
type or indicator name.

### Concurrency

`TryUpdateProvision` holds the region mutex while it updates policies and
collects indicators. The new helpers read `podSet` and `bindingNumas` only
within that locked path. MetaCache and metric readers retain responsibility
for their own internal synchronization.

No goroutines, package-level mutable state, or process-wide test stubs will be
introduced.

## Error Handling

- Missing `ContainerInfo`: mark the indicator unavailable and include pod and
  container identity in the error.
- Empty resolved CPU set: mark the indicator unavailable and include region
  identity and binding NUMAs.
- Missing or expired CPU metric: skip that CPU and continue.
- No valid CPU metric: mark only sched-wait unavailable.
- Invalid finite domain: reject the indicator before policy input.
- Unexpected getter error: preserve the existing fail-fast behavior.

Logs must use lowercase error text and contain enough context to identify the
region, indicator, and affected CPU scope without logging per-cycle success
noise.

## Alternatives

### Copy the Share Getter

Registering sched-wait and calling `AggregateCoreMetric` is the smallest
change, but it cannot distinguish a measured zero from absent data, bypasses
expiration checks, and encourages duplicate CPU-scope traversal. It does not
meet the reliability requirement.

### Extend MetricsFetcher

A new aggregate result containing value, valid sample count, and errors would
centralize the behavior. It would also require changes to the public
interface, production and fake implementations, and unrelated callers. That
scope is disproportionate for one region capability and can be considered
separately if multiple subsystems need coverage-aware aggregation.

### Cache the Last Valid Value

Last-value reuse would keep controller input continuous during collection
gaps, but it introduces state, expiry policy, restart behavior, and the risk
of acting on stale pressure. The design therefore uses explicit omission.

## Verification

Add parallel-safe table-driven tests covering:

- the dedicated constructor registers the sched-wait getter;
- a NUMA-binding sibling uses only CPUs assigned on its binding NUMA;
- sibling regions of one cross-NUMA pod produce independent values;
- a non-NUMA-binding region uses all assigned CPUs;
- duplicate container CPU assignments are counted once;
- partial metric availability averages only valid samples;
- missing and expired samples are omitted;
- complete sample absence omits only sched-wait while retaining other
  indicators;
- a measured sched-wait value of zero is retained;
- negative, NaN, and infinite values do not reach provision policies;
- the extracted CPU-scope helper preserves existing dedicated CPU usage
  behavior;
- default dedicated indicator configuration remains unchanged.

Run the focused region package tests, the CPU advisor package tests, and the
repository's standard formatting and static checks for the touched packages.

## Delivery

Keep production changes and tests in separate commits:

1. `feat(sysadvisor): support sched wait for dedicated regions`
2. `test(sysadvisor): cover dedicated sched wait indicators`

The implementation must not modify default configuration or adapter
dependencies.
