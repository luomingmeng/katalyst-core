# QRM and SysAdvisor Canonicalization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Canonicalize QRM candidate pools before quantity derivation and canonicalize SysAdvisor generic default-share entries to fake-NUMA scope.

**Architecture:** QRM reuses `cleanPoolsFromPodEntries` at the common adjustment boundary, so every caller receives the same owner-based pool view. SysAdvisor adds a focused result canonicalizer that preserves fake-NUMA history, removes invalid real-NUMA generic share entries, and runs before budget construction.

**Tech Stack:** Go, testify, Katalyst dynamicpolicy state, SysAdvisor provision assembler.

---

### Task 1: QRM regression test

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_test.go`

- [ ] **Step 1: Add the failing integration test**

Add `TestRemovePodLastOwnerBackfillsOrphanCPUsInSingleRevision` using a 12-CPU, 3-NUMA topology:

```go
topology, err := machine.GenerateDummyCPUTopology(12, 1, 3)
require.NoError(t, err)
stateDir := t.TempDir()
policy, err := getTestDynamicPolicyWithInitialization(topology, stateDir)
require.NoError(t, err)
policy.reservedCPUs = machine.NewCPUSet()
policy.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
policy.dynamicConfig.GetDynamicConfiguration().FillDefaultSharePoolWithNonReclaimCPUs = true
policy.state.SetAllowSharedCoresOverlapReclaimedCores(false, false)
policy.enableCPUAdvisor = true
policy.advisorClient = &advisorPoolTestClient{}
```

Construct:

```text
NUMA0 = ordinary pool share-NUMA0, owned by last-owner
NUMA1 = reclaim
NUMA2 = synthetic share
```

After `RemovePod(last-owner)`, assert:

```go
require.Equal(t, initialRevision+1, policy.state.GetRevision())
require.NotContains(t, committedEntries, removedPodUID)
require.NotContains(t, committedEntries, orphanPool)
require.True(t, orphanCPUs.IsSubsetOf(committedShare))
require.Equal(t, committedEntries, restarted.GetPodEntries())
require.True(t, reflect.DeepEqual(committedMachineState, restarted.GetMachineState()))
```

- [ ] **Step 2: Verify RED**

Run:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run TestRemovePodLastOwnerBackfillsOrphanCPUsInSingleRevision -count=1
```

Expected: FAIL because the orphan pool still participates in quantity derivation.

- [ ] **Step 3: Commit the test**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_test.go
git commit -m "test(dynamicpolicy): cover orphan cleanup before share backfill"
```

### Task 2: QRM candidate canonicalization

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers.go`

- [ ] **Step 1: Canonicalize before quantity derivation**

At the start of `adjustAllocationEntriesWithRampUpFloorForModeAtRevision`, after the timing defer and before `poolsQuantityMap` is built, add:

```go
// Remove orphan non-resident pools from this adjustment's candidate before
// deriving pool quantities and materializing the default-share residual.
// Precommit cleanup remains a defense against pools introduced by later hooks.
p.cleanPoolsFromPodEntries(entries)
```

- [ ] **Step 2: Verify GREEN**

Run:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'TestRemovePodLastOwnerBackfillsOrphanCPUsInSingleRevision|TestCleanPoolsFromPodEntries' \
  -count=1
```

Expected: PASS.

- [ ] **Step 3: Commit the implementation**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers.go
git commit -m "fix(dynamicpolicy): canonicalize pools before share planning"
```

### Task 3: SysAdvisor canonicalization test

**Files:**
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler/assembler_common_test.go`

- [ ] **Step 1: Add the failing unit test**

Add `TestCanonicalizeDefaultShareEntries`:

```go
result := types.NewInternalCPUCalculationResult()
result.SetPoolEntry(commonstate.PoolNameShare, commonstate.FakedNUMAID, 4, -1)
result.SetPoolEntry(commonstate.PoolNameShare, 0, 6, -1)
result.SetPoolEntry(commonstate.PoolNameShare, 1, 8, -1)
result.SetPoolEntry("share-NUMA0", 0, 2, -1)
result.SetPoolEntry(commonstate.PoolNameReclaim, 0, 4, -1)

before := canonicalizeDefaultShareEntries(result)

require.Equal(t, 4, before)
require.Len(t, result.PoolEntries[commonstate.PoolNameShare], 1)
require.Contains(t, result.PoolEntries[commonstate.PoolNameShare], commonstate.FakedNUMAID)
require.Contains(t, result.PoolEntries, "share-NUMA0")
require.Contains(t, result.PoolEntries, commonstate.PoolNameReclaim)
```

Add a second case containing only real-NUMA plain share entries and assert that the generic share map is removed while unrelated pools remain.

- [ ] **Step 2: Verify RED**

Run:

```bash
go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler \
  -run TestCanonicalizeDefaultShareEntries -count=1
```

Expected: build failure because `canonicalizeDefaultShareEntries` does not exist.

- [ ] **Step 3: Commit the test**

```bash
git add pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler/assembler_common_test.go
git commit -m "test(sysadvisor): cover default-share result canonicalization"
```

### Task 4: SysAdvisor result canonicalization

**Files:**
- Modify: `pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler/assembler_common.go`

- [ ] **Step 1: Add the canonicalizer**

```go
func canonicalizeDefaultShareEntries(result *types.InternalCPUCalculationResult) int {
	if result == nil {
		return 0
	}
	byNUMA := result.PoolEntries[commonstate.PoolNameShare]
	if byNUMA == nil {
		return 0
	}
	before := 0
	if existing := byNUMA[commonstate.FakedNUMAID]; existing != nil {
		before = existing.Size
	}
	for numaID := range byNUMA {
		if numaID != commonstate.FakedNUMAID {
			delete(byNUMA, numaID)
		}
	}
	if len(byNUMA) == 0 {
		delete(result.PoolEntries, commonstate.PoolNameShare)
	}
	return before
}
```

- [ ] **Step 2: Invoke it before budget construction**

In `finalizeDefaultShareBackfill`:

```go
before := canonicalizeDefaultShareEntries(result)
budgetByNUMA, summary, err := pa.buildDefaultShareBudget(regionHelper, result)
```

Remove the later duplicate `before` lookup. Keep `SetPoolEntry` as the single writer of the final fake-NUMA generic share.

- [ ] **Step 3: Verify GREEN**

Run:

```bash
go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler \
  -run 'TestCanonicalizeDefaultShareEntries|Test.*DefaultShare' -count=1
```

Expected: PASS.

- [ ] **Step 4: Commit the implementation**

```bash
git add pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler/assembler_common.go
git commit -m "fix(sysadvisor): canonicalize generic default-share scope"
```

### Task 5: Verification and review

**Files:**
- Verify all files changed in Tasks 1-4.

- [ ] **Step 1: Run focused tests repeatedly**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'TestRemovePodLastOwnerBackfillsOrphanCPUsInSingleRevision|TestCleanPoolsFromPodEntries' \
  -count=10
go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler \
  -run 'TestCanonicalizeDefaultShareEntries|Test.*DefaultShare' -count=10
```

- [ ] **Step 2: Run race tests**

```bash
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy \
  -run 'TestRemovePodLastOwnerBackfillsOrphanCPUsInSingleRevision|TestCleanPoolsFromPodEntries' \
  -count=1
go test -race ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler \
  -run 'TestCanonicalizeDefaultShareEntries|Test.*DefaultShare' -count=1
```

- [ ] **Step 3: Run package tests**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy -count=1
go test ./pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler -count=1
```

Record unrelated baseline failures explicitly.

- [ ] **Step 4: Review**

Review the final diff for:

- accidental deletion of synthetic share in QRM canonicalization;
- accidental deletion of SNB pools in SysAdvisor;
- mutation of source responses outside the intended candidate/result;
- non-deterministic map behavior affecting metrics;
- duplicate or post-commit cleanup semantics.
