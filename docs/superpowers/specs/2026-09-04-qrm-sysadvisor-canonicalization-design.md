# QRM and SysAdvisor Canonicalization Design

## Goal

Canonicalize QRM allocation candidates and SysAdvisor default-share results before either side derives quantities. This removes historical ownerless ordinary pools from QRM planning and guarantees that SysAdvisor publishes the generic default-share upper bound only at fake-NUMA scope.

## Scope

This slice changes canonicalization only. It does not:

- allow an empty default-share residual in normal admission;
- add recovery materialization;
- change the SysAdvisor upper-bound formula;
- add advice generation or QRM revision fields;
- change Bulkhead commit overrides.

Those behaviors remain later slices of the approved cross-component solution.

## QRM Canonicalization

`adjustAllocationEntriesWithRampUpFloorForModeAtRevision` canonicalizes its transient `entries` before deriving `poolsQuantityMap`.

The existing `cleanPoolsFromPodEntries` classification remains authoritative:

- delete ownerless ordinary pools;
- retain pools with live owners;
- retain resident pools;
- retain system pools;
- retain synthetic `share` while `FillDefaultSharePoolWithNonReclaimCPUs` is enabled.

Synthetic `share` is intentionally not deleted by generic orphan cleanup. Its presence is decided later by Normal or Recovery residual materialization.

## SysAdvisor Canonicalization

`finalizeDefaultShareBackfill` canonicalizes the generic `share` entry before building the default-share budget:

- preserve the fake-NUMA generic share entry as the previous published value;
- delete any plain `share` entry on a real NUMA;
- delete the `share` map if no fake-NUMA entry remains;
- leave SNB pools such as `share-NUMA7` unchanged;
- leave reclaim, reserve, dedicated and custom shared pools unchanged.

The canonicalizer returns the previous fake-NUMA size for backfill diagnostics. The finalizer then writes exactly one generic share entry at fake-NUMA scope.

## Invariants

After QRM canonicalization:

```text
ownerless ordinary non-resident pools = empty
synthetic default share may remain
resident and system pools remain
```

After SysAdvisor canonicalization and finalization:

```text
PoolEntries["share"] contains exactly FakedNUMAID
plain real-NUMA share entries do not affect fixed-pool diagnostics
SNB pools remain distinct
```

## Verification

QRM integration testing must prove that deleting the last owner removes its ordinary pool before quantity derivation, backfills the released CPUs, and persists the result in one revision.

SysAdvisor unit testing must prove that real-NUMA generic share entries are removed, fake-NUMA history is preserved for diagnostics, and unrelated pool entries remain unchanged.

Focused tests run first, followed by package tests and race tests. Existing unrelated baseline failures must be recorded rather than hidden.
