# Deadlock A-B-C Phase Cost Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reproduce the E2E A→B→C transition, identify exact deadlock context phase costs, and optimize only the confirmed dominant phase.

**Architecture:** Add disjoint phase counters to the existing probe statistics. Build an unprotected 262-entry/249-edge/23-atom fixture and reserve=0,24 assertions. If child membership dominates, replace eager child counts with exact singleton-atom sibling scans; stop if frontier remains over budget.

**Tech Stack:** Go, standard `testing`, existing topology planner and bulkhead view helpers.

---

### Task 1: Phase cost accounting

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/deadlock.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/drain_projection.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/deadlock_test.go`

- [ ] Add rel-index, child-membership, frontier-membership, and ancestor-closure counters.
- [ ] Make every context charge increment exactly one counter.
- [ ] Assert the counter sum equals `ContextOperations`.
- [ ] Include all counters in wrapped budget errors.

### Task 2: Real-shape RED fixture

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/deadlock_test.go`

- [ ] Build exactly 262 entries and 249 child edges.
- [ ] Use reserve CPUs `0,24`.
- [ ] Set B△C to exactly 23 singleton atoms.
- [ ] Keep protected rels and pending CPUs empty.
- [ ] Repeat transfer CPUs in dynamic descendants without adding them to `DynamicByRel`.
- [ ] Verify the unoptimized implementation fails at `AtomIndex=-1`.
- [ ] Record the dominant phase from the exact counters.

### Task 3: Conditional child-membership optimization

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/drain_projection.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/drain_projection_test.go`
- Test: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/deadlock_test.go`

- [ ] Remove `baseChildCountByRel` only if Task 2 confirms child membership dominates.
- [ ] For the singleton atom CPU, scan the affected parent's children and use projected targets for changed children and base targets for unchanged children.
- [ ] Charge each sibling target inspected.
- [ ] Compare every atom result against canonical full projection.
- [ ] Re-run the 262 fixture.
- [ ] Stop without further algorithm changes if frontier still exhausts the budget.

### Task 4: Lifecycle and reserve tests

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/view_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/coordinator_test.go`

- [ ] Assert reserve pool `allocation_result=0,24` produces `Reserve={0,24}` with size two.
- [ ] Model A→B with transient protection and B→C after protection clears.
- [ ] Assert successful completion yields desired=applied=observed=C.
- [ ] Assert a planning failure preserves applied=observed=B without partial writes.

### Task 5: Verification

**Files:**
- Verify all modified topology and bulkhead files.

- [ ] Run targeted phase, fixture, oracle, lifecycle, and reserve tests.
- [ ] Run the topology package tests.
- [ ] Run the bulkhead utils tests.
- [ ] Run topology package tests with `-race`.
- [ ] Run `git diff --check`.
