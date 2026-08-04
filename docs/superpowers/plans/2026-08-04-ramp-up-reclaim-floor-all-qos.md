# Ramp-up Reclaim Floor for All QoS Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply one all-NUMA reclaim floor to every shared and dedicated ramp-up path.

**Architecture:** A deterministic helper derives per-NUMA floors from reserved-reclaimed and current reclaim state. Allocation, assembler, and advisor paths subtract the same derived floor; bulkhead validates that ownership computation retains it.

**Tech Stack:** Go, QRM CPU state, machine topology, bulkhead partition view.

---

### Task 1: Ratio semantics

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/util/ramp_up_reclaim.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/util/ramp_up_reclaim_test.go`

- [ ] Make `ratio=0` return reserve exactly.
- [ ] Apply non-zero ratio independently of Pod reclaim annotation.
- [ ] Preserve cap and exclusive remainder validation.

### Task 2: All-NUMA floor

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers_test.go`

- [ ] Derive deterministic floor for every machine reclaim NUMA.
- [ ] Prefer current reclaim CPUs within each NUMA.
- [ ] Use per-NUMA reserved-reclaimed quota for `ratio=0`.
- [ ] Exclude the floor from dedicated allocation candidates.

### Task 3: Shared ramp-up

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers_test.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go`

- [ ] Exclude the floor from initial shared pooled CPUs.
- [ ] Exclude the floor from assembler `rampUpCPUs`.
- [ ] Exclude the floor from advisor `rampUpCPUs`.
- [ ] Assert no ramp-up allocation intersects the floor.

### Task 4: View invariant and verification

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/view.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/view_test.go`

- [ ] Pass the derived floor into partition-view options.
- [ ] Keep the ownership formula unchanged.
- [ ] Fail closed if any floor CPU is absent from `ReclaimEffective`.
- [ ] Run targeted, package, race, and full E2E validation.
