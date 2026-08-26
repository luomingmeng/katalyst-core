# Non-Reclaim Exclusive-DNB Steady Allocation Design

## Status

Approved for implementation. This design narrows the active target semantics in
`2026-08-26-ramp-up-scoped-hard-reclaim-target-design.md` for workloads that
the existing SPD policy classifies as non-reclaimable.

## Goal

A NUMA-exclusive dedicated Pod with `PodEnableReclaim=false` starts with the
steady partition instead of the optional ramp-up ratio:

```text
non-reclaimable: 30 dedicated + 2 reclaim
reclaimable:     26 dedicated + 6 reclaim
```

The example assumes 32 eligible logical CPUs, a two-CPU steady reclaim reserve,
and `InitialRampUpReclaimCPUSetRatio=0.2`.

## Ownership

The adapter SPD manager remains the canonical owner of performance-level
fallback. QRM does not duplicate SPD, baseline, or legacy NUMA-exclusive
classification.

`ResourceRequest` already carries the Pod UID, namespace, name, labels, and
annotations. QRM captures an immutable `ObjectMeta` snapshot before QoS parsing
filters those maps. The adapter manager uses that snapshot directly when it
proves legacy NUMA-exclusive semantics; other callers retain the existing Pod
fetch behavior.

The metadata fast path answers only whether the performance level is
definitively `Poor`. Baseline evaluation still uses a fetched Pod because
`ResourceRequest` has no creation timestamp. The final reclaimability result is
captured once before allocation and passed through the existing NUMA-binding
allocation call.

## Admission Behavior

For exclusive NUMA-binding allocation with hard partition enabled:

1. Capture `ObjectMeta` before QoS parsing mutates request labels and
   annotations.
2. Query the adapter-backed performance level without a preliminary Pod
   informer lookup.
3. If the result is `Poor`, classify the Pod as non-reclaimable.
4. Otherwise run the existing full Pod, SPD, and baseline check.
5. If the Pod informer is not ready, conservatively classify the Pod as
   reclaimable for this admission. SysAdvisor corrects the partition after its
   metadata converges.
6. For `PodEnableReclaim=false`, mark the new exclusive allocation
   `RampUp=false`; it starts in steady state and must not activate the
   node-level ratio target in pending advisor validation.
7. For `PodEnableReclaim=true`, preserve the complete ratio-based hard floor.
8. For `PodEnableReclaim=false`, derive the steady target count for every NUMA
   from `ResolvePerNUMAReservedForReclaim`; no non-hinted NUMA retains a
   ratio-derived target.
9. Treat `reservedReclaimedCPUSet` only as mandatory CPU identity. Complete
   each identity to its full physical core, supplement it from the canonical
   reclaim-eligible set until the dynamic steady count is met, and reject the
   request if the result is not whole-core aligned.
10. Validate selected CPUs against reclaim eligibility on every NUMA. This
    excludes disable-reclaim resource packages and fails closed if a mandatory
    identity is no longer eligible.
11. Allocate dedicated CPUs from the remaining hinted partition and validate
    full, disjoint NUMA coverage.
12. Exclude any NUMA already wholly owned by a committed steady
    (`RampUp=false`) non-reclaim exclusive DNB allocation from a later
    workload's ramp-up floor derivation. That partition is finalized and keeps
    only its steady reclaim reserve (e.g. two CPUs), which is smaller than the
    ratio-derived ramp-up target (e.g. six CPUs). Re-imposing the node-level
    target on it would fail admission closed for every other ramp-up QoS on the
    node.
13. Apply the same steady-exclusive carve-out in the bulkhead precommit
    per-NUMA reclaim validator. It is a second, independent enforcement site
    that projects the materialized partition; without the carve-out it re-checks
    the ratio-derived target against the finalized steady reserve and rejects
    every other ramp-up QoS on the node.
14. Apply the same steady-exclusive carve-out in the advisor block-plan
    imbalance validator. It is a third, independent enforcement site that checks
    cross-NUMA reclaim distribution; without the carve-out it compares the
    finalized steady reserve against the ratio-derived target and reports a false
    minimum or imbalance failure, again rejecting every other ramp-up QoS on the
    node.
15. Apply the same steady-exclusive carve-out in the advisor hard-partition
    block planner itself. It is a fourth, independent enforcement site: before
    the validator ever runs, the planner water-fills the fake mandatory-reclaim
    block across eligible NUMAs and then re-checks its own per-NUMA minimum and
    cross-NUMA imbalance. A steady-exclusive NUMA is capacity-locked at its
    finalized reserve while ramp-up NUMAs fill to the ratio target, so without
    the carve-out the planner fails closed on a false imbalance one step earlier
    than the validator.
16. Apply the carve-out at the SysAdvisor producer boundary as well. Capture
    steady-exclusive NUMAs together with the cycle-start `RampUp` snapshot and
    omit those NUMAs from `rampUpReclaimCPUSetCap`. Otherwise a ramp-up Pod on
    another NUMA can make the node-level `hardActive` decision re-expand a
    finalized `30 dedicated + 2 reclaim` partition back toward `26 + 6` before
    QRM sees the advice.

The four enforcement sites (ramp-up floor derivation, bulkhead precommit
validator, advisor block-plan validator, advisor block planner) share a single
`PodEntries.SteadyExclusiveNUMAs` helper so the carve-out cannot drift between
them. SysAdvisor derives the corresponding set from its immutable cycle-start
container snapshot because it owns a separate metadata representation.

Synchronous advice is generation-bound. QRM captures Pod entries, machine
state, and the state revision under one read lock when constructing
`GetAdviceRequest`. After the RPC returns, QRM checks that request revision
under the policy lock before validating or planning the response. The existing
precommit CAS remains responsible for mutations that occur after planning
starts; the request-revision check protects the RPC round trip itself.

The adapter still queries SPD with the original request metadata. SPD presence
overrides the default legacy NUMA-exclusive level exactly as before. SPD
not-found uses `DefaultPerformanceLevelForNumaExclusive`. A transient metadata
or Pod lookup failure never produces a non-reclaimable classification.

## Alternatives

Setting `InitialRampUpReclaimCPUSetRatio=0` globally would produce the desired
layout but would remove ramp-up protection from reclaimable workloads.

Adding a new annotation or protobuf field would make the decision explicit but
would expand the change across API, adapter, kubelet request generation, and
checkpoint compatibility. The existing metaserver contract already provides
the required classification.

Returning an Allocate error while waiting for Pod informer convergence is not
viable: kubelet marks the Pod `UnexpectedAdmissionError` and does not retry the
same allocation. Runtime evidence from the first implementation attempt
invalidated that approach.

## Compatibility

- Non-exclusive and non-NUMA-binding allocations are unchanged.
- Hard partition disabled behavior is unchanged.
- Reclaim disabled behavior is unchanged.
- Legacy overlap mode is unchanged.
- Reclaimable exclusive-DNB keeps the ratio-derived ramp-up floor.
- Non-reclaimable exclusive-DNB uses the configured steady count on every
  NUMA, including NUMAs outside the current hint.
- Static reclaim reserve carries identity only; it does not override the
  configured steady count.
- Reclaim floors are complete physical cores on SMT systems.
- Validator and precommit continue to enforce non-zero results, partition
  coverage, and dedicated/reclaim disjointness.
- No API, protobuf, checkpoint, or CRD schema changes are introduced.

## Tests

The regression suite must demonstrate:

- non-reclaimable exclusive-DNB selects only the steady reserve;
- reclaimable exclusive-DNB preserves the ratio-derived floor;
- QoS filtering does not remove metadata used by the SPD decision;
- request metadata reaches the adapter SPD manager without a Pod informer hit;
- a real SPD still overrides the default exclusive-DNB performance level;
- metadata-only evaluation never runs baseline logic with a zero timestamp;
- a missing Pod informer entry falls back to reclaimable admission;
- a definitively non-reclaimable exclusive allocation is committed with
  `RampUp=false`, so pending advisor validation uses the steady target;
- the configured steady count applies to hinted and non-hinted NUMAs;
- static identities are preserved and completed to whole physical cores;
- a NUMA wholly owned by a steady non-reclaim exclusive DNB is skipped when a
  later workload derives its ramp-up floor, so other QoS still admit;
- the bulkhead precommit per-NUMA reclaim validator skips a steady non-reclaim
  exclusive NUMA, so its finalized steady reserve does not fail admission for
  other ramp-up QoS;
- the advisor block-plan imbalance validator skips a steady non-reclaim
  exclusive NUMA, so its finalized steady reserve triggers neither a false
  per-NUMA minimum nor a false cross-NUMA imbalance failure;
- SysAdvisor keeps a steady-exclusive NUMA at the steady reserve while a
  different NUMA still has active ramp-up work;
- a synchronous advisor response is rejected when the state revision changes
  during the RPC even if the active-ramp-up boolean remains unchanged;
- disable-reclaim resource-package CPUs are excluded from reclaim eligibility;
- existing partition coverage and eligibility failures remain fail-closed;
- the real-node lifecycle starts and remains at `30 dedicated + 2 reclaim`.
