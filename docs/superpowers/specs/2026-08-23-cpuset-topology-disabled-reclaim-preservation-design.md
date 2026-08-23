# Reclaim CPUSet Preservation While Topology Is Disabled

## Status

Revised after architecture review. Implementation has not started.

## Goal

Add an opt-in static configuration that keeps configured reclaim cgroups aligned
with the latest `DesiredView.ReclaimEffective` while the dynamic
`EnableBulkheadCpusetTopology` switch is false.

The mode has a deliberately narrow ownership boundary:

- configured `BulkheadReclaimRelPaths` continue to follow
  `DesiredView.ReclaimEffective`;
- existing NUMA buckets selected by `BulkheadReclaimNumaPrefixes` continue to
  follow `DesiredView.ReclaimEffectivePerNUMA`;
- dynamic descendants below those reclaim cgroups may be adjusted when required
  to preserve cgroup hierarchy constraints;
- primary, partition, configured reclaim sibling, and discovered reclaim sibling
  paths retain the current disabled reset behavior;
- later Bulkhead plugins are not stopped by the manager and decide whether to
  act using their own enablement and proof requirements.

The new behavior is disabled by default.

## Non-Goals

This design does not:

- make the mode active when `AllowSharedCoresOverlapReclaimedCores` is true;
- bypass the cgroup v2 opt-in gate;
- perform any write when the global Bulkhead switch is off;
- create missing reclaim roots or NUMA buckets;
- claim that primary, sibling, or container-leaf topology has fully converged;
- manufacture rel proofs for paths outside the reclaim-only ownership scope;
- change the enablement policy of downstream plugins;
- introduce a dynamic AQC field for this behavior.

## Configuration

Add the following field to `BulkheadConfiguration` and `BulkheadOptions`:

```go
PreserveReclaimCPUSetWhenTopologyDisabled bool
```

The default is `false`.

Expose it through:

```text
--qrm-cpu-bulkhead-preserve-reclaim-cpuset-when-topology-disabled
```

The Adapter environment mapping is:

```text
QRMCPUPluginPreserveReclaimCPUSetWhenTopologyDisabled
  -> qrm-cpu-bulkhead-preserve-reclaim-cpuset-when-topology-disabled
```

The configuration remains static. Changing it requires a process restart. The
dynamic `EnableBulkheadCpusetTopology` field continues to select between full
topology mode and disabled mode at runtime.

## Current Behavior

`Manager.Apply` builds the desired partition view, evaluates every plugin's
`Enable` method, and records the effective enablement map.

When `cpuset_topology` is disabled:

1. the manager invokes `CPUSetAdjustmentDisabledHandler` only when
   `needsDisabledReset` reports an enabled-to-disabled transition;
2. `CPUSetAdjustmentDisabledHandler` invokes `resetCPUSetTopology`;
3. `buildDisabledResetDAG` gives every controlled static node the disabled
   reset target;
4. the reset coordinator propagates that target through dynamic descendants;
5. the manager sets `topologyStopped=true`;
6. later plugins are skipped.

On cgroup v1, the reset target is the machine CPUSet. On cgroup v2, reset keeps
the existing inherited/reset semantics and is guarded by
`EnableBulkheadCpusetTopologyOnCgroupV2`.

This behavior cannot satisfy the new requirement because reclaim paths are
reset during the transition and no topology operation runs on later disabled
rounds.

## Design Principles

### Preserve enablement truth

`CPUSetTopologyPlugin.Enable` must continue to mean that full cpuset topology
management is enabled. The new mode must not make `Enable` return true while the
dynamic topology switch is false.

### Preserve typed publication

The manager must receive a typed `DAGApplyResult`. It must not publish state
through the legacy callback in the middle of a transaction.

### Publish only physical proof

Every rel included in the reclaim-only `AppliedView` must come from the fresh
final snapshot that established convergence. Desired values that were not
observed on disk must not be represented as applied.

### Keep hard gates hard

Global Bulkhead disablement and the cgroup v2 opt-in gate remain mutation
barriers. The overlap mode remains outside reclaim-only ownership.

### Reuse hierarchy-safe convergence

All reclaim CPUSet changes use `TopologyCoordinator`. No direct cgroup write
path may bypass identity checks, ordering, replan handling, convergence budgets,
or final snapshot validation.

## API Extension

Add a named optional interface in the Bulkhead API:

```go
type DisabledTopologyReconciler interface {
	ShouldReconcileWhenDisabled(
		context.Context,
		HandlerContext,
	) bool
	ReconcileDisabled(
		context.Context,
		HandlerContext,
	) (DAGApplyResult, error)
}
```

`CPUSetTopologyPlugin` implements this interface in addition to `Plugin` and
`TopologyPlugin`.

The optional interface avoids adding disabled reconciliation methods to every
plugin. It also avoids plugin-name-specific policy in the manager.

## Mode Selection

`ShouldReconcileWhenDisabled` returns true only when all of the following hold:

```text
PreserveReclaimCPUSetWhenTopologyDisabled == true
EnableBulkheadCpusetTopology == false
AllowSharedCoresOverlapReclaimedCores == false
cgroup version == v1
  OR EnableBulkheadCpusetTopologyOnCgroupV2 == true
global Bulkhead switch == true
```

The manager already enforces the global Bulkhead gate before invoking plugins.
The plugin must still enforce the cgroup version gate at each mutation entry
point.

If the topology plugin is disabled for overlap or for a non-opted-in cgroup v2
host, `ShouldReconcileWhenDisabled` returns false and the existing disabled
behavior remains authoritative.

## Manager Flow

The manager handles a disabled topology plugin in this order:

1. Check whether it implements `DisabledTopologyReconciler`.
2. Call `ShouldReconcileWhenDisabled`.
3. If false, use the existing disabled transition behavior and stop topology
   dependents as today.
4. If true and the non-reclaim disabled reset has not completed for this
   disabled epoch, call `CPUSetAdjustmentDisabledHandler` once.
5. Record successful reset completion independently from the complete
   adjustment-round result.
6. Call `ReconcileDisabled` on every adjustment round.
7. Validate the returned `DAGApplyResult` through the same typed acceptance
   rules used for normal topology apply.
8. Rebuild and compare the desired view after physical convergence.
9. Apply the generation fence.
10. Commit the reclaim-only `AppliedView`, revision, and latest verified reclaim
   CPUSet.
11. Continue iterating through later plugins without setting
   `topologyStopped`.

The manager keeps a dedicated disabled-reset completion state for plugins that
implement `DisabledTopologyReconciler`. It does not infer this state from
`lastCPUSetAdjustmentEnabled`, because a successful physical reset followed by
a failed reclaim convergence must not force the reset to run again.

The reset completion state follows these rules:

- it starts incomplete after process startup;
- it becomes complete only after the disabled reset succeeds and the generation
  fence is still current;
- it remains complete when the later reclaim-only convergence fails;
- it is cleared when the plugin is observed enabled, when reclaim-only mode
  becomes ineligible, or when the global Bulkhead hard gate clears manager
  enablement state;
- a failed or stale disabled reset remains incomplete and is retried.

The accepted result must satisfy:

```text
FullyConverged == true
FinalSnapshotCurrent == true
AppliedView != nil
AppliedView.Level == reclaim_only
```

`ParentSafe` is not accepted for reclaim-only publication. This mode has no
admission-specific deferred-leaf contract, and downstream plugins must not run
from an incomplete reclaim proof.

If reconciliation fails, the manager does not publish the candidate
`AppliedView`, does not update `latestAppliedReclaim`, and does not run later
enabled plugins in that round.

## Disabled Execution

The disabled path contains one transition operation and one steady-state
operation. Both execute under the manager mutex. Each coordinator invocation
acquires the shared topology mode gate for its own operation.

### Transition-Only Non-Reclaim Reset

Build a reset DAG that contains:

- the primary controlled path;
- configured reclaim siblings;
- discovered reclaim siblings;
- dynamic descendants reached by the reset coordinator.

Exclude:

- configured reclaim roots;
- reclaim NUMA buckets;
- dynamic descendants rooted under the excluded reclaim paths.

`BulkheadPartitionRelPaths` remains boundary metadata used by discovery and path
classification. It does not become an independent topology node.

Targets retain current disabled semantics:

- cgroup v1 uses the machine CPUSet;
- cgroup v2 uses the current reset/inherit representation;
- known expected container leaves retain the existing exact-leaf protection.

The reset phase uses `ResetModeGuardWithGate`. The reset writer receives the
normalized reclaim roots as explicit traversal boundaries. When dynamic
propagation reaches one of those roots, it skips the root and does not recurse
below it. This prevents an excluded reclaim subtree from being rediscovered as
an uncontrolled dynamic child and reset indirectly.

The traversal boundary set contains every normalized configured reclaim root,
including roots classified as absent. This closes the race where a missing root
is created after classification but before reset traversal. Boundary matching
uses path components, not raw string prefixes.

This reset runs only on the first round of a disabled epoch. It is not part of
the steady-state `ReconcileDisabled` operation.

### Steady-State Reclaim-Only Convergence

Build a separate normal-mode DAG that contains only:

- each existing configured reclaim root;
- each existing configured NUMA bucket;
- dynamic descendants required by the coordinator to enforce hierarchy safety.

Targets are:

```text
reclaim root -> DesiredView.ReclaimEffective
NUMA bucket  -> DesiredView.ReclaimEffectivePerNUMA[numaID]
dynamic child -> inherited or expected target accepted by the existing planner
```

The reclaim-only DAG contains a single ownership domain. It therefore does not
apply the full topology invariant that primary and reclaim domains must be
disjoint. The physical primary reset target may overlap the reclaim target by
design while full topology is disabled.

The normal coordinator remains responsible for:

- cgroup identity checks;
- grow, shrink, and mixed-transfer ordering;
- bounded replan behavior;
- cgroup v1 parent-child subset constraints;
- cgroup v2 controller semantics;
- convergence verification;
- fresh final snapshot publication.

Only expected, deferred, and protected rels beneath an existing reclaim root
are passed into the reclaim-only coordinator:

```text
ExpectedCPUSetByRel
DeferredCPUSetByRel
ProtectedCPUSetByRel
```

`ProtectedPendingCPUSet` is empty in reclaim-only mode. It has no resolved rel
and therefore cannot be safely attributed to a reclaim subtree. The full
topology behavior that folds pending protection into primary does not apply.

## Transaction Budget

The transition reset and reclaim-only convergence share one absolute
disabled-round deadline:

```text
deadline = min(
  caller context deadline,
  disabled round start + configured topology convergence deadline,
)
```

The child context is created once before either operation and is passed to both
coordinators. A second coordinator invocation must not derive a later deadline
from a new `time.Now()`.

Each coordinator retains its own round, snapshot, node-visit, and I/O counters.
The shared absolute deadline is the aggregate wall-clock bound. If the
transition reset consumes the available time, its completion state remains
recorded after a successful generation fence; reclaim convergence fails closed
for that round and runs without another reset on the next round.

Steady-state disabled rounds contain only reclaim convergence and therefore use
the full transaction deadline.

## Cgroup v1 Ordering

For each parent-child edge in the reclaim-only hierarchy:

```text
child target subset-of parent target
```

The existing DAG traversal and planner rules remain mandatory:

- grow writes the parent before the child;
- shrink writes the child before the parent;
- mixed transfers use the existing drain/transfer plan;
- a stale identity or stale current CPUSet invalidates the plan;
- a final snapshot mismatch prevents publication.

Dynamic reclaim descendants may be rewritten to fit the new ancestor target.
Restricting writes to configured roots and NUMA buckets alone is unsafe because
a parent shrink can fail while a child still references CPUs outside the new
parent target.

## Missing Paths

Missing reclaim paths are an expected no-op, not a reconciliation error.

Path existence is classified through an identity-aware preflight observation,
not through independent unpinned `StatDir` calls whose result is assumed valid
for the whole round.

Apply the following rules:

- normalize every configured relative path;
- capture existence and stable directory identity before building the active
  reclaim DAG;
- when a reclaim root is absent, log and exclude the root and all NUMA buckets
  beneath it from the active DAG;
- when an individual NUMA bucket is absent, log and exclude only that bucket;
- keep every absent configured root or bucket as a named scan/traversal boundary
  for the remainder of the round;
- do not create a missing root or bucket;
- never keep a child whose declared parent was excluded;
- filter expected, deferred, and protected rel maps to descendants of active
  reclaim roots;
- return non-`ENOENT` errors, including permission, I/O, controller, and invalid
  path failures.

The first coordinator snapshot validates the preflight classification before
any write:

- an existing configured rel must retain the observed identity;
- disappearance or identity replacement invalidates the plan;
- an absent configured descendant that appears during the round must not be
  treated as an ordinary dynamic child;
- a classification change causes the plugin to rebuild the active DAG and retry
  within the same transaction deadline.

A stable absence remains a successful no-op. Repeated create/delete or identity
churn may exhaust the transaction deadline and fail the round, but it must not
publish a view based on ambiguous ownership.

For an absent top-level reclaim root that is outside every active snapshot
subtree, perform a final absence check before publication. If it appeared, mark
the plan stale and rebuild. Only a final current observation may authorize an
empty reclaim-only result.

Suggested log shape:

```text
cpuset_topology: reclaim-only rel path does not exist, skipping, rel=%q
```

No metric is added for missing paths in the initial implementation. The log
keeps the behavior observable without introducing a potentially high-cardinality
label.

If every configured reclaim root is absent, reconciliation produces a valid
empty reclaim-only result. It does not retain the previous applied reclaim
CPUSet.

## AppliedView

Add a strongly typed level:

```go
const AppliedViewLevelReclaimOnly AppliedViewLevel = "reclaim_only"
```

The reclaim-only builder classifies every partition field explicitly:

| Field | Value source | Physical proof |
|---|---|---|
| `Reserve` | desired deep copy | no |
| `Dedicated` | desired deep copy | no |
| `ReclaimRaw` | desired deep copy | no |
| `SharePool` | desired deep copy | no |
| `SharePoolMap` | desired deep copy | no |
| `Isolation` | desired deep copy | no |
| `DesiredNonReclaimPool` | desired deep copy | no |
| `DesiredReclaimEffective` | desired deep copy | no |
| `DesiredReclaimEffectivePerNUMA` | desired deep copy | no |
| `TransientProtectedNonReclaim` | empty | no |
| `TransientProtectedNonReclaimPerNUMA` | non-nil empty map | no |
| `NonReclaimPool` | empty | no |
| `ReclaimEffective` | proved existing reclaim roots | yes |
| `ReclaimEffectivePerNUMA` | proved existing NUMA buckets | yes |
| `ContainerCPUSetByPod` | non-nil empty map | no |
| `CPUSetByRel` | proved reclaim roots and NUMA buckets | yes |
| `RelProofByRel` | proved reclaim roots and NUMA buckets | yes |
| `Level` | `AppliedViewLevelReclaimOnly` | scope marker |

Absent NUMA buckets do not create empty keys in
`ReclaimEffectivePerNUMA`. All map fields are initialized to non-nil empty maps
when they have no entries.

Purely logical field changes participate in `EqualAppliedView` and may increment
`AppliedViewRevision`. This is intentional because downstream plugins such as
RDT CPUList consume logical pool fields from `HandlerContext.View`.

The builder must not use the normal `appliedViewFromFinalSnapshotWithContext`
container-leaf publication path. That path intentionally requires complete
container proofs and would turn the partial ownership scope into a false
full-topology claim.

Multiple configured reclaim roots receive the same aggregate reclaim target.
`ReclaimEffective` is the union of their verified proofs. Missing roots
contribute nothing.

An empty applied reclaim means that no physical reclaim cgroup was proved in
this round. It does not mean that logical QRM reclaim ownership is empty.
Consequently:

- update the manager's `appliedView` and `latestAppliedReclaim` to empty;
- pass the empty reclaim-only view to downstream plugins;
- do not emit an empty `CPUSetAdjustmentCommitOverride`;
- do not clear the reclaim pool in QRM checkpoint state;
- preserve the current empty-CPUSet sentinel semantics in the outer adjustment
  runner.

## Downstream Plugins

The manager no longer blocks later plugins after successful reclaim-only
publication.

Each plugin still follows its own contracts:

- a plugin that consumes aggregate reclaim state may use the verified
  `ReclaimEffective`;
- a plugin that consumes only logical partition fields may use their deep-copied
  values;
- a plugin that requires a specific rel proof must check `CPUSetByRel` or
  `RelProofByRel` and skip when the proof is absent;
- no downstream plugin receives fabricated primary or sibling proof.

In particular, a `system_service` target outside the reclaim-only proof scope
may skip its migration even when its own enable switch is true. This is plugin
self-protection, not manager-level suppression.

Periodical handlers may receive the reclaim-only `AppliedView` because the
result is fully converged for its declared scope. They must continue to enforce
their own level and proof requirements.

## State Transitions

### Static option disabled

Behavior is unchanged. Dynamic topology disablement triggers the current
one-time reset and stops dependent plugins.

### Full topology to reclaim-only

The first disabled round:

1. resets non-reclaim topology;
2. converges reclaim topology to the current desired view;
3. publishes a reclaim-only view;
4. continues later plugins.

The successful transition reset is recorded independently. Subsequent disabled
rounds run only reclaim-only reconciliation so View changes continue to reach
cgroups.

If reset succeeds and reclaim convergence fails, the next round skips reset and
retries reclaim convergence. If reset itself fails or its generation fence is
stale, the next round retries reset.

### Reclaim-only to full topology

When the dynamic topology flag becomes true, `Enable` becomes true and the
existing full `TopologyPlugin.Apply` path restores full topology ownership and
publishes a full applied view.

### Overlap enabled

`AllowSharedCoresOverlapReclaimedCores=true` disables reclaim-only mode. The
current disabled reset and downstream stop behavior applies.

### Cgroup v2 gate disabled

When `EnableBulkheadCpusetTopologyOnCgroupV2=false`, the plugin remains inert on
cgroup v2. Neither reset nor reclaim-only convergence writes cgroups.

### Global Bulkhead disabled

The manager's hard gate remains unchanged. No enabled handler, disabled handler,
or reclaim-only reconciler runs.

## Failure Semantics

The mode is fail-closed for publication and downstream side effects:

- a reset phase failure fails the round;
- a reclaim convergence failure fails the round;
- non-current final snapshot fails the round;
- missing required proof for an existing controlled reclaim rel fails the round;
- desired-view drift after convergence fails the round;
- generation drift fails the round;
- no candidate `AppliedView` is committed after failure;
- later enabled plugins do not run after failure.

Physical writes completed before a later failure are not rolled back. The next
round replans from the observed hierarchy, matching the current coordinator
transaction model.

Missing configured reclaim paths are the only intentionally tolerated path
absence. Stable absence is excluded before becoming an active controlled node;
identity or existence changes during the transaction cause bounded replan.

## Observability

Reuse existing topology summary and plugin result metrics for:

- reset convergence;
- reclaim-only convergence;
- non-convergence reason;
- final snapshot status;
- handler result.

Add mode information to logs so operators can distinguish:

```text
mode=full
mode=reclaim_only
mode=reset
```

Missing reclaim paths are logged at an informational verbose level. Do not use
the rel path as a metric label.

Applied partition metrics report the reclaim-only view. `NonReclaimPool` is
empty because it is not physically proved in this mode; dashboards must not
interpret that field as full topology ownership.

## Core Changes

Expected Core touch points:

- `pkg/config/agent/qrm/bulkhead/bulkhead.go`
- `cmd/katalyst-agent/app/options/qrm/bulkhead/bulkhead.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/api/types.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/model/view.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/manager.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/reset_writer.go`
- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/snapshot.go`
- related unit test files

Implementation should extract small helpers rather than grow
`CPUSetAdjustmentDisabledHandler` into a second monolithic topology path.
Suggested responsibilities:

- mode eligibility;
- identity-aware reclaim rel classification;
- reset/reclaim spec partitioning;
- reset traversal boundaries;
- reclaim-only applied-view construction;
- disabled reset completion state;
- manager typed result acceptance.

## Adapter Changes

Use the worktree for `feat/default-share-residual-backfill-adapter`, not the
dirty main Adapter checkout.

Expected Adapter changes:

- add the environment-to-flag entry in
  `build/katalyst-agent/bytedance_run.sh`;
- update the Core dependency to the final Core commit;
- update module sums only when dependency resolution requires it;
- avoid unrelated configuration or script formatting changes.

## Test Matrix

### Configuration

- default value is false;
- command-line true and false values parse correctly;
- `ApplyTo` copies the field;
- Adapter maps the exact environment variable to the exact flag.

### Eligibility

- v1, dynamic topology false, preserve true, overlap false: active;
- v1, dynamic topology true: inactive because full topology owns the round;
- preserve false: inactive;
- overlap true: inactive;
- v2 opt-in false: inactive and mutation-free;
- v2 opt-in true: active;
- global Bulkhead false: no plugin entry point runs.

### Manager

- disabled reconciler runs every disabled round;
- non-reclaim reset runs only once per disabled epoch;
- reset success followed by reconcile failure does not replay reset;
- reset failure remains pending and is retried;
- leaving reclaim-only mode keeps reset pending after a reset failure and retries
  it on the next disabled round;
- leaving reclaim-only mode keeps reset pending after a stale generation fence
  and retries it on the next disabled round;
- enabling topology or a successful authoritative exit reset clears reset
  completion;
- successful result does not set `topologyStopped`;
- later plugins are invoked according to their own enablement;
- non-converged result blocks later plugins;
- missing applied view blocks later plugins;
- stale final snapshot blocks later plugins;
- changed desired view blocks publication;
- stale generation blocks publication;
- revision changes only when the applied view changes;
- latest applied reclaim is replaced with the verified result, including empty.

### Reset Scope

- primary is reset;
- primary dynamic descendants retain current reset behavior;
- configured siblings are reset;
- discovered siblings are reset;
- reclaim roots are not reset;
- reclaim NUMA buckets are not reset;
- absent configured reclaim roots remain traversal boundaries;
- reset propagation cannot cross into an excluded reclaim subtree.

### Reclaim Convergence

- reclaim grow uses parent-before-child order;
- reclaim shrink uses child-before-parent order;
- mixed change uses the existing planner;
- every child target is a subset of its parent target on cgroup v1;
- dynamic descendants are adjusted when required for parent safety;
- per-NUMA targets remain within their NUMA upper bounds;
- cgroup identity change invalidates the plan;
- every snapshot used to build or rebase a write plan revalidates all
  `RequiredIdentityByRel` entries before any subsequent write;
- final snapshot mismatch prevents publication.

### Missing Paths

- one missing reclaim root is logged and skipped;
- a root skip also removes its NUMA children;
- one missing NUMA bucket is logged and skipped independently;
- all roots missing returns an empty successful reclaim-only view;
- no missing reclaim path is created;
- a configured bucket created after preflight triggers replan rather than
  dynamic-child inheritance;
- disappearance after preflight is reclassified within the transaction budget;
- same-path inode replacement invalidates the plan;
- final absence is rechecked before publishing an empty result;
- permission and I/O errors still fail.

### AppliedView

- level is `reclaim_only`;
- aggregate reclaim contains only proved existing roots;
- per-NUMA reclaim contains only proved existing buckets;
- rel proofs include stable device and inode identity;
- primary and sibling proofs are absent;
- non-reclaim and container applied fields are empty;
- logical desired fields follow the field-source matrix;
- transient protection fields are empty;
- absent NUMA buckets do not leave empty map keys;
- `DeepCopy` isolates all nested maps and CPU sets;
- `EqualAppliedView` compares the new level and proof contents.

### Transaction Budget

- transition reset and reclaim convergence share one absolute deadline;
- the second coordinator cannot derive a later deadline;
- reset success is retained when reclaim reaches the shared deadline;
- the next round skips the completed reset and retries reclaim;
- steady-state reclaim-only rounds receive the full configured deadline;
- manager lock acquisition and coordinator work remain context-cancelable.

### Downstream Behavior

- an enabled downstream plugin is reached after successful reconciliation;
- a plugin using verified aggregate reclaim can act;
- a plugin requiring an absent sibling proof skips itself;
- no manager-level stop is applied solely because full topology is disabled.

### Observability

- successful reclaim-only convergence emits a topology summary with
  `phase=reclaim_only`;
- terminal reclaim-only failure emits a topology summary with
  `phase=reclaim_only` and `status=error`;
- retryable stale classification changes do not emit a terminal failure summary
  before the in-transaction retry is exhausted.

### Regression

- preserve=false retains the exact current disabled transition behavior;
- normal full topology apply is unchanged;
- admission parent-safe behavior is unchanged;
- cgroup v2 inert behavior is unchanged without opt-in;
- global Bulkhead hard-off remains mutation-free.

## Verification

Before integration:

1. run focused config, model, manager, topology utility, and cpuset topology
   package tests;
2. run downstream plugin tests that consume `AppliedView`;
3. run the Core repository precheck required by the branch;
4. update Adapter to the verified Core SHA;
5. run Adapter script checks, unit tests, and precheck;
6. inspect both repository diffs for unrelated changes;
7. verify the Core and Adapter SHAs recorded for deployment are aligned.

Real-node validation should include standard and high-churn rounds on cgroup v1.
The test must exercise reclaim grow and shrink while the dynamic topology switch
is false, verify every configured existing root and NUMA bucket, and validate
parent-child subset constraints after every adjustment.

## Atomic Delivery

Recommended commit boundaries:

1. Core API, configuration, and manager disabled-reconcile framework.
2. Core reclaim-only topology implementation and tests.
3. Adapter Core dependency and environment mapping.
4. Documentation may ship with the first Core commit or as an isolated docs
   commit.

Every commit must remain buildable and keep unrelated worktree changes out of
the diff.

## Acceptance Criteria

The implementation is accepted when:

- the default behavior is unchanged;
- enabling the static option keeps existing reclaim roots and NUMA buckets
  aligned with the latest desired reclaim view while full topology is disabled;
- primary and sibling paths retain disabled reset semantics;
- missing reclaim paths are logged, skipped, and never created;
- cgroup v1 parent-child constraints hold during grow, shrink, and mixed change;
- only fresh, observed reclaim proof is published;
- later plugins are not manager-blocked after successful reclaim-only
  convergence;
- failures prevent publication and later side effects;
- cgroup v2 and global Bulkhead gates remain intact;
- Core and Adapter tests and prechecks pass.
