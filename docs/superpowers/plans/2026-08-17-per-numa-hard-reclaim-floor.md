# Per-NUMA Hard Reclaim Floor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make hard reclaim a stable per-NUMA invariant derived from immutable capacity, allow dedicated excess to transfer to reclaim, and keep QRM, sysadvisor, Bulkhead, and ramp-up lifecycle consistent.

**Architecture:** Sysadvisor owns per-NUMA quantity targets; QRM owns concrete CPU identities and commits one validated candidate partition. Bulkhead projects the committed partition without trimming it. Admission may bootstrap the same capacity formula before advice, but all mutation paths share one precommit validator.

**Tech Stack:** Go, gRPC CPU advisor protocol, QRM dynamic policy state, Bulkhead cpuset topology, testify.

---

### Task 1: Define canonical per-NUMA floor

**Files:**
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/advisor_helper.go`
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/advisor_helper_test.go`
- Modify: `pkg/util/machine/ramp_up_reclaim.go`
- Modify: `pkg/util/machine/ramp_up_reclaim_test.go`

- [ ] Add failing table tests for `capacity=32, ratio=0.2 => 6`, heterogeneous `24/32 => 4/6`, invalid ratio, and stable results independent of available CPU.
- [ ] Run the focused machine and advisor helper tests and verify the new cases fail because the per-NUMA helper is missing.
- [ ] Add `CalculatePerNUMAHardReclaimTarget(capacity, ratio, minimum, configuredReserve int) (int, error)` using floor then even alignment.
- [ ] Change hard-partition `updateReservedForReclaim` to use immutable topology capacity per NUMA and the shared helper; retain legacy behavior when hard partition is disabled.
- [ ] Run focused tests and commit.

### Task 2: Make sysadvisor publish canonical targets

**Files:**
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/server/cpu_server.go`
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/server/cpu_server_test.go`
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler/assembler_common.go`
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler/dedicated_reclaim_partition.go`
- Modify corresponding assembler tests.

- [ ] Add failing tests proving hard floor remains present without active ramp-up and is not overwritten by live QRM reclaim size.
- [ ] Add failing assembler tests proving every physical NUMA emits at least its canonical floor and dedicated excess increases reclaim target.
- [ ] Run focused tests and verify expected failures.
- [ ] Remove `hasActiveRampUpContainer` and live reclaim size override from hard-floor publication.
- [ ] Ensure floor-only reclaim results are emitted for physical NUMAs without workload regions.
- [ ] Make dedicated/reclaim quantity planning preserve the floor before optional dedicated excess.
- [ ] Run sysadvisor CPU and server tests and commit.

### Task 3: Jointly solve concrete dedicated/reclaim CPU identities

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/hard_reclaim_partition_test.go`

- [ ] Add failing tests for fixed six-CPU floor on every 32-CPU NUMA after ordinary DNB and exclusive DNB allocations.
- [ ] Add failing tests where current free reclaim-eligible CPU is below target but same-NUMA dedicated excess can donate enough CPUs.
- [ ] Add a failing test proving no donor may fall below `ceil(RequestQuantity)` and non-reclaimable package CPUs cannot be donated.
- [ ] Run focused tests and verify they fail for the intended quantity/ownership mismatch.
- [ ] Implement a pure `planHardReclaimPartition` that jointly selects reclaim and dedicated CPU IDs from the same NUMA scope, preferring current ownership and minimizing movement.
- [ ] Use sysadvisor targets on advice; use the same immutable capacity formula only as admission bootstrap.
- [ ] Replace global `CalculateGlobalRampUpReclaimTarget`/`DistributeNUMATarget` hard-partition usage.
- [ ] Integrate the plan into shared/SNB, DNB, and exclusive DNB paths.
- [ ] Run dynamicpolicy focused tests and commit.

### Task 4: Establish one precommit pipeline

**Files:**
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/partition_precommit.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/partition_precommit_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_async_handler.go`

- [ ] Add failing tests proving hooks run before validation, revision changes reject the candidate, and override/deletion paths cannot bypass validation.
- [ ] Run focused tests and verify the expected stale/invalid candidates are currently committed.
- [ ] Implement `commitPendingCPUPartition`: clone, run pure hooks, normalize, regenerate machine state, validate, then CAS commit.
- [ ] Route advisor apply, pool adjustment, DNB admission, cpuset override, and deletion fallback through this function.
- [ ] Ensure no target-mutating hook runs after validation.
- [ ] Run dynamicpolicy tests and commit.

### Task 5: Make Bulkhead projection-only

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/view.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/view_test.go`
- Modify callers constructing `CPUSetPartitionViewOptions`.

- [ ] Add failing tests proving legal asymmetric reclaim above each NUMA floor is preserved.
- [ ] Add failing tests proving reclaim below the per-NUMA capacity-derived target is rejected.
- [ ] Run Bulkhead tests and verify the current rebalance trims legal reclaim.
- [ ] Remove `rebalanceHardPartitionReclaimEffective`.
- [ ] Replace fixed minimum and cross-NUMA balance checks with `targetByNUMA` lower-bound validation.
- [ ] Keep overlap, coverage, parent/child, and reclaim-eligibility checks.
- [ ] Run Bulkhead tests and commit.

### Task 6: Repair ramp-up lifecycle

**Files:**
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/advisor.go`
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/advisor_test.go`
- Modify the existing QRM ramp-up reconcile path and its tests.

- [ ] Add a failing test where a NUMA-binding shared container has `RampUp=true` and an empty owner pool; the whole advice must not fail.
- [ ] Add lifecycle tests proving SNB and non-SNB containers both remain in ramp-up before 30 seconds and exit after the same 30-second transition period.
- [ ] Run focused tests and verify the owner-pool error and stuck state.
- [ ] Move the `ci.RampUp` guard before NUMA-binding owner validation.
- [ ] Ensure QRM assigns the final pool and clears `RampUp` only after the shared 30-second transition period and a successful candidate commit.
- [ ] Add age/timeout observability without silently clearing ramp-up on failure.
- [ ] Run sysadvisor and dynamicpolicy tests and commit.

### Task 7: Full verification and node validation

**Files:**
- Modify only test artifacts and summary documents outside the Core repository.

- [ ] Run `go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/... -count=1`.
- [ ] Run `go test ./pkg/agent/sysadvisor/plugin/qosaware/server -count=1`.
- [ ] Run `go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/... -count=1`.
- [ ] Run `go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/... -count=1`.
- [ ] Build Adapter `feat/default-share-residual-backfill-adapter@d603d7f4` with a temporary replace to the new Core commit, then restore `go.mod`.
- [ ] Upload the binary to `fdbd:dc05:d:44e::17`, preserve the old binary, restart, and verify SHA and health.
- [ ] Run shared, SNB, DNB, exclusive DNB sequential/concurrent, mixed 20-second hold, standard three rounds, and high-churn.
- [ ] Verify every NUMA has at least six reclaim CPUs, no owner drops below request, ramp-up clears within two advisor periods, and state/cgroup/RDT remain consistent.
- [ ] Run an independent final code review and record all artifacts.
