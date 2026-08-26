# Non-Reclaim Exclusive-DNB Steady Allocation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a non-reclaimable NUMA-exclusive dedicated Pod use the steady reclaim reserve from its first successful allocation while preserving the ratio-derived ramp-up floor for reclaimable Pods.

**Architecture:** Capture immutable Pod metadata from `ResourceRequest` before QoS filtering. Use the adapter-backed manager to identify a definite `Poor` performance level without a Pod informer lookup; otherwise run the complete Pod/SPD/baseline path, with only Pod NotFound falling back to reclaimable admission. Propagate the resulting snapshot into the partition selector.

**Tech Stack:** Go, Katalyst QRM DynamicPolicy, testify, Go race detector

---

### Task 1: Resolve reclaimability from request metadata

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers.go`
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/resource/helper/helper.go`
- Test: `pkg/agent/sysadvisor/plugin/qosaware/resource/helper/helper_test.go`
- Modify: adapter `pkg/metaserver/spd/manager.go`
- Test: adapter `pkg/metaserver/spd/manager_test.go`

- [ ] **Step 1: Add metadata helper tests**

Add a helper that accepts `metav1.ObjectMeta` and evaluates only performance
level without calling `MetaServer.GetPod` or baseline. Keep the UID-based helper
unchanged for complete Pod/SPD/baseline evaluation.

```go
func PodMetaEnableReclaim(
    ctx context.Context,
    metaServer *metaserver.MetaServer,
    podMeta metav1.ObjectMeta,
    nodeEnableReclaim bool,
) (bool, error)
```

- [ ] **Step 2: Verify helper tests fail**

Run:

```bash
go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/helper -count=1
```

Expected: FAIL because the metadata helper does not exist.

- [ ] **Step 3: Add adapter metadata fast-path tests**

Use a PodFetcher that returns an error. Pass metadata whose annotations prove
legacy NUMA-exclusive and verify:

- no-SPD returns `DefaultPerformanceLevelForNumaExclusive`;
- an existing SPD still overrides that default;
- metadata without explicit exclusive semantics preserves the PodFetcher error.

- [ ] **Step 4: Implement the metadata path**

QRM deep-copies `ObjectMeta` from `ResourceRequest` before QoS filtering and
passes the snapshot through request context. `Poor` returns non-reclaimable;
other levels run the complete UID-based helper. Only the existing Pod NotFound
error falls back to reclaimable admission; SPD, baseline, cancellation, and
other Pod errors propagate. The adapter uses a synthetic Pod from metadata only
when it already proves legacy NUMA-exclusive semantics.

- [ ] **Step 5: Verify core and adapter tests**

Run focused helper, DynamicPolicy, and adapter SPD manager tests. Expected: all
PASS.

### Task 2: Select the steady floor

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers.go`

- [ ] **Step 1: Change the partition test**

For `podReclaimEnabled=false`, expect only
`reservedReclaimedCPUSet`, while `podReclaimEnabled=true` continues to expect
the full derived floor:

```go
{
    name:              "pod reclaim false uses steady reserve",
    podReclaimEnabled: false,
    want:              machine.NewCPUSet(0, 1),
}
```

- [ ] **Step 2: Run the focused partition test**

Run:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run TestDynamicPolicy_selectNumaBindingReclaimPartitionPreservesHardFloor \
  -count=1
```

Expected: FAIL because the selector currently ignores `podReclaimEnabled`.

- [ ] **Step 3: Implement the target selection**

Name the existing boolean parameter `podReclaimEnabled`. For each hinted NUMA:

```go
target := derivedInNUMA.Size()
if !podReclaimEnabled {
    target = reserveTarget
}
target = general.Max(target, base.Size())
```

Retain eligibility bounds, topology-aware selection, and all fail-closed
errors.

- [ ] **Step 4: Align lifecycle state**

When the same exclusive hard-partition allocation is definitively
non-reclaimable, create its `AllocationInfo` with `RampUp=false`. Reclaimable
exclusive allocations remain `RampUp=true`. Add a candidate precommit test that
would otherwise report a two-CPU floor against a six-CPU ramp-up target.

- [ ] **Step 5: Verify focused and package tests**

Run:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'TestDynamicPolicy(PodEnableReclaimForNumaBindingAllocation|_selectNumaBindingReclaimPartition)' \
  -count=1
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/... -count=1
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'TestDynamicPolicy(PodEnableReclaimForNumaBindingAllocation|_selectNumaBindingReclaimPartition)' \
  -count=1
```

Expected: all commands PASS.

### Task 3: Integrate and validate

**Files:**
- Modify: adapter `pkg/metaserver/spd/manager.go`
- Test: adapter `pkg/metaserver/spd/manager_test.go`
- Modify: adapter `go.mod`
- Modify: adapter `go.sum`
- Update: `qrm-bulkhead-test-artifacts/qrm-rampup-c059ab7-validation-summary.md`

- [ ] **Step 1: Commit and push core**

Commit only the production and test changes with a detailed Conventional Commit
message, then push `feat/default-share-residual-backfill`.

- [ ] **Step 2: Update adapter**

Point the adapter replace directive to the new core commit, refresh module
checksums, preserve the user's unrelated Go-version change, commit, and push
`feat/default-share-residual-backfill-adapter`.

- [ ] **Step 3: Build and deploy**

Build the Linux amd64 SKIPCGO agent, verify SHA256, upload through
`jumpinf1-hl`, back up both existing binaries, atomically replace QRM and
SysAdvisor binaries, and restart them through runsv.

- [ ] **Step 4: Verify the real-node lifecycle**

With the adapter fallback temporarily set to `PerformanceLevelPoor`, create one
exclusive-DNB and verify its first authoritative QRM state is:

```text
dedicated=30
reclaim NUMA0=2
overlap=none
```

Confirm the same state after ramp-up exits, the demo process affinity matches
QRM state, and QRM health remains ready.

- [ ] **Step 5: Restore and document**

Delete the probe, restore the original performance-level override, retain
`tae.bytedance.com/sandbox-enabled=true`, remove temporary transfer files, and
update the validation report with exact state, cgroup, log, and health
evidence.
