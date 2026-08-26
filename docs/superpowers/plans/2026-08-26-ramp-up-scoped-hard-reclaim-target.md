# Ramp-Up-Scoped Hard Reclaim Target Implementation Plan

## Goal

Restore `reservedForReclaim` as the steady reclaim floor and make the
ratio-derived hard target active only while a workload is entering or remains
in ramp-up.

## Architecture

SysAdvisor owns steady and active quantity calculation. QRM owns concrete CPU
identity selection and transactional commit. QRM state owns the definition of
an active ramp-up allocation. Bulkhead validates and projects the candidate
partition without defining a second activation rule.

## Tech Stack

Go, QRM dynamic policy state, SysAdvisor CPU provision assembler, Bulkhead
cpuset view, testify.

## Baseline and Authority

- `pkg/config/agent/dynamic/adminqos/qrm/cpu_plugin.go`: the feature applies
  while a workload is in ramp-up.
- `docs/superpowers/specs/2026-08-26-ramp-up-scoped-hard-reclaim-target-design.md`:
  approved target behavior.
- `docs/superpowers/specs/2026-08-17-always-on-hard-reclaim-floor-design.md`:
  superseded behavior to retire.
- `docs/superpowers/specs/2026-08-04-ramp-up-reclaim-floor-all-qos-design.md`:
  active-ramp-up lifecycle and node-level target selection.

## Compatibility Boundary

- No API, protobuf, checkpoint, or dynamic-configuration schema changes.
- No behavior change when reclaim or hard partition is disabled.
- Existing overlap switch semantics and partition precommit transaction order
  remain unchanged.
- Active hard partition remains node-level and whole-core aligned.
- Kubernetes 1.18 compatibility remains unchanged.

## Verification

Focused RED/GREEN tests are followed by:

```bash
go test ./pkg/util/machine -count=1
go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/... -count=1
go test ./pkg/agent/sysadvisor/plugin/qosaware/server -count=1
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/state -count=1
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/... -count=1
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/... -count=1
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/... -count=1
```

Real-node validation must cover `26 -> steady` DNB transition, CPU sibling
alignment, no reclaim overlap, and cleanup/state drain.

## Plan Basis

### Facts

- Current hard mode replaces steady `reservedForReclaim` with
  `ResolveHardPartitionReclaimTargets`.
- `updateRampUpReclaimCPUSetCap` already carries a separate runtime target map.
- `deriveRampUpReclaimFloorForMode` ignores its `enteringRampUp` argument.
- Bulkhead resolves and validates hard targets whenever the configuration is
  enabled, even without ramp-up workloads.
- CPU server suppresses live reclaim backfill whenever hard mode is configured.
- Real-node evidence showed ratio-derived six CPUs persisted as the region
  reserve after ramp-up.

### Assumptions

- Any active ramp-up workload activates the node-level target on every physical
  NUMA, matching the original cross-QoS design.
- `RampUp` in candidate `PodEntries` is the QRM runtime source of truth.
- Pre-occupation and pool entries do not activate the target.

### Unknowns to falsify in tests

- Whether all advisor apply paths can provide candidate entries before target
  validation.
- Whether stale advisor responses can carry an old hard target after the last
  ramp-up exits.
- Whether legacy overlap mode has hidden dependencies on the always-on floor.

## Architecture Integrity Lens

- **Invariant:** steady reserve is independent of an initial ramp-up ratio;
  active candidate partitions satisfy one shared temporary target.
- **Canonical owner:** `pkg/util/machine` owns quantity math;
  `dynamicpolicy/state` owns QRM ramp-up detection.
- **Responsibility overlap:** current SysAdvisor, QRM, CPU server, and Bulkhead
  each encode part of the always-on activation rule.
- **Higher-level simplification:** pass one runtime `hardActive` decision into
  existing planners instead of adding new local scans or fallback branches.
- **Retirement:** always-on comments, tests, and config-only gates are deleted
  in the same change.
- **Verdict:** revise the existing design; do not add a compatibility path for
  the internal always-on behavior.

## Plan Pressure Test

- **Owner / contract / retirement:** owners are explicit; the superseded
  internal behavior has no external schema boundary.
- **Architecture integrity:** no new solver, cache, persistent state, or
  adapter is needed.
- **Verification scope:** SysAdvisor, QRM admission/advisor, precommit,
  Bulkhead, race, and real-node evidence are covered.
- **Task executability:** each task has a focused RED/GREEN boundary.
- **Pressure result:** proceed.

## Plan-Time Complexity Check

| File | Size signal | Risk | Decision |
| --- | ---: | --- | --- |
| `advisor_helper.go` | 429 lines | low | edit in place |
| `assembler_common.go` | 2236 lines | high | add one small private gate; do not add another solver |
| `cpu_server.go` | 1090 lines | medium | replace existing condition/helper only |
| `policy_allocation_handlers.go` | 3778 lines | high | signature/gate plumbing only |
| `policy_advisor_handler.go` | 2559 lines | high | compute active once at orchestration boundary |
| `bulkhead/utils/view.go` | 481 lines | low | accept runtime active input; keep validation pure |
| `partition_precommit.go` | 157 lines | low | reuse candidate state, no new validation pipeline |

The shared QRM activation predicate belongs in existing
`dynamicpolicy/state/util.go`; adding a new package or manager would move
complexity rather than remove it.

## Rejected Alternatives

1. **Only remove the SysAdvisor branch.** Rejected because QRM and Bulkhead
   would still reject the lower steady target.
2. **Rename the feature as persistent hard partition.** Rejected because it
   contradicts the approved runtime behavior and existing configuration
   contract.
3. **Persist an active hard-target state.** Rejected because `RampUp` already
   exists in candidate state; persistence adds migration and stale-state risk.
4. **Add cached active-target maps.** Rejected because target computation is
   cheap and snapshot-local; caching creates invalidation and generation
   consistency problems.

## Task 1: Define one QRM activation predicate

**Files**

- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/state/util.go`
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/state/util_test.go`

**Why**

QRM admission, advisor planning, precommit, and Bulkhead must not implement
separate scans or count pool/pre-occupation entries as workloads.

**Impact**

Internal helper only; no schema change.

**Steps**

- [ ] Add table tests for empty entries, pool-only entries, real
  `RampUp=false`, real `RampUp=true`, sidecar/main combinations, and cloned
  candidate entries.
- [ ] Run:

  ```bash
  go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/state \
    -run TestPodEntriesHasActiveRampUp -count=1
  ```

  Verify RED because the helper does not exist.
- [ ] Add a pure method:

  ```go
  func (pe PodEntries) HasActiveRampUp() bool
  ```

  Iterate real pod/container entries once, ignore nil allocations and generic
  pool entries, return on the first `RampUp=true`.
- [ ] Run the focused test and the full state package; verify GREEN.
- [ ] Review allocations: the helper must allocate no maps/slices and must be
  `O(entries)` with early exit.

## Task 2: Separate SysAdvisor steady reserve from active target

**Files**

- Modify `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/advisor.go`
- Modify `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/advisor_helper.go`
- Modify `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/advisor_helper_test.go`
- Modify `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/advisor_test.go`

**Repair track**

- Root cause: an initial ratio replaced the steady reserve map.
- Correct owner: steady resolver remains `ResolvePerNUMAReservedForReclaim`;
  active target remains a separate map.

**Retirement track**

- Delete the hard-mode branch from `updateReservedForReclaim`.
- Delete constructor logic that skips steady initialization in hard mode.

**Steps**

- [ ] Add RED tests proving hard configured/inactive produces the same steady
  map as hard disabled, and ratio changes do not alter it.
- [ ] Add lifecycle tests for no ramp-up, first active ramp-up, multiple active
  ramp-ups, and last ramp-up exit.
- [ ] Run:

  ```bash
  go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu \
    -run 'TestCPUResourceAdvisor(UpdateReservedForReclaim|RampUpReclaim)' \
    -count=1
  ```

- [ ] Make `updateReservedForReclaim` always call
  `ResolvePerNUMAReservedForReclaim`.
- [ ] Make `updateRampUpReclaimCPUSetCap` scan containers once:
  inactive publishes an empty map; active calls
  `ResolveHardPartitionReclaimTargets` for all physical NUMAs with the steady
  map as `perNUMAReservedFloor`.
- [ ] Build maps locally and replace fields atomically; do not mutate existing
  maps key by key.
- [ ] Initialize the steady map regardless of hard-mode configuration.
- [ ] Run focused and full SysAdvisor CPU tests; verify GREEN.

## Task 3: Remove config-only behavior from assembler and CPU server

**Files**

- Modify `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler/assembler_common.go`
- Modify `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler/assembler_common_test.go`
- Modify `pkg/agent/sysadvisor/types/cpu.go`
- Modify `pkg/agent/sysadvisor/plugin/qosaware/server/cpu_server.go`
- Modify `pkg/agent/sysadvisor/plugin/qosaware/server/cpu_server_test.go`

**Why**

Even after Task 2, unconditional `effectiveHard` and live-backfill suppression
would preserve the old behavior.

**Steps**

- [ ] Add RED tests for hard configured/inactive equivalence across:
  non-exclusive, exclusive-disjoint, exclusive-legacy, empty NUMA, overlap
  metadata, and default-share backfill.
- [ ] Add RED CPU server tests proving inactive mode uses live reclaim and
  active mode preserves advisor target.
- [ ] Run focused assembler/server tests.
- [ ] Add one private assembler predicate whose runtime truth is a non-empty
  active target map; replace all config-only `effectiveHard` branches.
- [ ] Use the active target as the current effective reserve in region
  upper-bound, exclusive, non-exclusive, and empty-NUMA calculations.
- [ ] Add in-process-only `InternalCPUCalculationResult.RampUpActive` and
  `RampUpHardPartitionActive` fields. Do not serialize them into advisor proto
  or checkpoint state.
- [ ] Rename `getRampUpHardReclaimSizeByNUMA` to describe live reclaim data;
  use live fallback only when `RampUpActive=true` and
  `RampUpHardPartitionActive=false`, without rescanning MetaCache.
- [ ] Preserve size/quota sentinel behavior and verify no target is expanded by
  the cap function.
- [ ] Run:

  ```bash
  go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler -count=1
  go test ./pkg/agent/sysadvisor/plugin/qosaware/server -count=1
  ```

## Task 4: Restore candidate-aware QRM activation

**Files**

- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers.go`
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers_test.go`
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy.go`
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_test.go`

**Why**

The first entering workload needs protection before commit, while ordinary
recalculation without ramp-up must not retain the temporary target.

**Steps**

- [ ] Add RED tests for first entering shared/SNB/DNB/exclusive-DNB,
  already-active recalculation, one-of-many exit, and final exit.
- [ ] Add candidate-vs-committed-state tests proving candidate entries win.
- [ ] Run focused tests around `deriveRampUpReclaimFloor`.
- [ ] Restore the `enteringRampUp` argument and add candidate `PodEntries` to
  `deriveRampUpReclaimFloorForMode`.
- [ ] Gate the existing resolver with:

  ```text
  configured && (enteringRampUp || candidateEntries.HasActiveRampUp())
  ```

- [ ] Update call sites to pass the immutable candidate snapshot already owned
  by that transaction; do not read mutable global state inside the helper.
- [ ] Remove tests and comments requiring a floor without ramp-up.
- [ ] Run focused and full allocation-handler tests.

## Task 5: Gate advisor planners once at orchestration boundary

**Files**

- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler.go`
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go`
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_source_pool.go`
- Modify corresponding source-pool and block-planner tests

**Why**

Legacy preallocation, disjoint expansion, block validation, and explicit-floor
selection currently each inherit config-only behavior.

**Steps**

- [ ] Add RED tests showing inactive advice skips hard expansion/validation but
  retains ordinary ownership checks.
- [ ] Add active tests for legacy/disjoint planning, malformed targets, donor
  request floor, and stale advice after final ramp-up exit.
- [ ] Compute `hardActive` once from candidate/current entries at the top-level
  advice transaction.
- [ ] For synchronous advice, derive request-generation ramp-up state from
  `GetAdviceRequest`; reject it without side effects when it differs from the
  locked current state. Keep the request-less ListAndWatch compatibility path.
- [ ] Pass the boolean into existing planner functions; do not let deep helpers
  rescan state or configuration.
- [ ] Use reclaim entries as an explicit ramp-up floor only when active.
- [ ] Preserve fail-closed behavior and the existing precommit/CAS order.
- [ ] Run:

  ```bash
  go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
    -run 'Test.*(HardPartition|RampUp|AdvisorBlock)' -count=1
  ```

## Task 6: Make Bulkhead runtime-active and candidate-aware

**Files**

- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/view.go`
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/view_test.go`
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/manager.go`
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/partition_precommit_test.go`
- Modify the QRM option-construction call site in `policy_advisor_handler.go`

**Why**

Bulkhead must validate the same candidate activation state and must not resolve
targets during inactive periodic reconciles.

**Steps**

- [ ] Add RED tests for configured/inactive, first active candidate, final-exit
  candidate, invalid ratio while inactive, and insufficient active target.
- [ ] Change `NewCPUSetPartitionViewOptions` to accept runtime `hardActive`.
- [ ] Resolve target maps only when `hardActive=true`.
- [ ] At both call sites, derive runtime active from the exact readonly/candidate
  state using `PodEntries.HasActiveRampUp`.
- [ ] Keep overlap, coverage, parent/child, and reclaim eligibility validation
  unchanged.
- [ ] Verify inactive mode restores normal non-reclaim padding and
  default-share behavior.
- [ ] Run Bulkhead and precommit tests.

## Task 7: Retire obsolete design and tests

**Files**

- Update `docs/superpowers/specs/2026-08-17-always-on-hard-reclaim-floor-design.md`
- Update affected comments and test names

**Steps**

- [ ] Mark the always-on design superseded by the new design.
- [ ] Remove statements describing the initial ratio as a persistent invariant.
- [ ] Search:

  ```bash
  rg -n 'persistent per-physical-NUMA|hard floor must remain active|without active ramp-up|without a ramp-up workload'
  ```

- [ ] Classify every remaining match as current behavior, historical context,
  or stale text; delete stale matches.
- [ ] Do not retain a compatibility branch for the retired internal behavior.

## Task 8: Full verification and deep review

**Verification**

- [ ] Run all commands in the Verification section.
- [ ] Run formatting and static checks on changed Go files:

  ```bash
  gofmt -w <changed-go-files>
  go vet ./pkg/util/machine \
    ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/... \
    ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/...
  ```

- [ ] Inspect `git diff --stat`, `git diff --check`, and the full diff.
- [ ] Verify no new duplicate resolver, target cache, runtime fallback,
  goroutine, timer, or persistent field was introduced.
- [ ] Run `bits-code-guard` against the complete diff.
- [ ] Run an independent implementation reviewer focused on:
  candidate snapshot correctness, stale advice, first-entry/final-exit races,
  pool-entry filtering, whole-core alignment, and performance.
- [ ] Fix all P0/P1 findings and justified P2 findings, then rerun focused and
  full tests.
- [ ] Verify the node with a tagged DNB probe:
  ramp-up target, post-ramp steady behavior, cgroup/state equality, sibling
  completeness, zero overlap, no precommit error, cleanup, and live state drain.

## Deep Review Checklist

### Correctness

- One immutable config snapshot and one candidate state drive each calculation.
- First admission cannot observe inactive hard partition.
- Last exit cannot retain an old explicit floor.
- Steady reserve is never lower than configured whole-core minimum.
- Active target is never lower than steady reserve.
- QRM/Bulkhead do not validate different target maps.

### Simplicity and readability

- One steady resolver, one active target resolver, one QRM activation helper.
- No boolean whose name says configured but is used as runtime active.
- Large handlers contain orchestration only; pure math/state semantics remain in
  small owner functions.
- No nested fallback path for old always-on behavior.

### Performance

- SysAdvisor scans containers once per update.
- QRM scans candidate entries once per transaction with early exit.
- Bulkhead receives runtime activation and does not rescan or resolve targets
  while inactive.
- CPU server consumes the in-process activation bit and does not perform a
  second MetaCache container scan.
- No per-CPU loops are added outside existing topology selection.
- No map cloning occurs inside NUMA/CPU inner loops.

### Regression safety

- Both overlap modes remain covered.
- Shared, SNB, DNB, and exclusive DNB paths are covered.
- SMT1/SMT2/SMT4 and heterogeneous NUMA capacities are covered.
- Inactive invalid hard configuration does not break steady reconcile.
- Active invalid configuration fails before state/cgroup/RDT mutation.

## Atomic Commit Shape

1. `refactor(qrm-cpu): centralize ramp-up activation state`
2. `fix(sysadvisor): scope hard reclaim target to ramp-up`
3. `fix(qrm-cpu): gate hard partition by candidate ramp-up`
4. `fix(qrm-cpu): align bulkhead hard target activation`
5. `docs(qrm-cpu): retire always-on hard reclaim floor`

Each commit includes its corresponding tests and a detailed body describing the
invariant, compatibility boundary, and verification commands.
