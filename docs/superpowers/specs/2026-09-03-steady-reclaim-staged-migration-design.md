# Steady Reclaim Staged Migration Design

## Context

The steady fake-NUMA whole-core planner currently computes a complete target and
rejects it when the replacement churn exceeds eight changed CPU IDs. This is
safe for invalid fragmented baselines, but it can permanently reject a valid
whole-core baseline that needs a larger topology-preserving rearrangement.

The migration limit applies only to avoidable CPU replacement. Pure expansion
and pure shrink are quantity changes and do not consume migration budget.

## Goals

- Converge a valid committed reclaim allocation toward the latest desired
  whole-core allocation over multiple successful advisor cycles.
- Limit each committed step to at most eight replacement CPU IDs.
- Preserve advisor quantity, whole-core alignment, per-NUMA floors, donor
  floors, eligibility, and partition disjointness in every committed step.
- Keep retries deterministic and latest-state safe.
- Preserve a lightweight migration target across advisor cycles and process
  restart while its feasibility constraints are unchanged.
- Retain fail-closed behavior for fragmented or otherwise invalid committed
  baselines.

## Non-goals

- Persisting a migration cursor, candidate frontier, or full per-demand plan.
- Committing fragmented intermediate allocations.
- Applying multiple state mutations inside one advisor response.
- Relaxing advisor quantity conservation or hard-partition constraints.

## Definitions

For committed set `current` and unrestricted desired set `desired`:

- `removed = current - desired`
- `added = desired - current`
- `replacementPairs = min(len(removed), len(added))`
- `replacementChurn = 2 * replacementPairs`

Pure expansion has `removed=0`; pure shrink has `added=0`. Both have zero
replacement churn.

## Planner Flow

1. Canonicalize topology, demand quantity/class/eligibility, fake-demand keys,
   and core-floor constraints into a SHA-256 constraint digest.
2. If a durable target exists with the same digest, reconstruct a legal
   per-demand desired assignment whose fake-demand union equals that target.
   Otherwise compute the unrestricted whole-core `desired` allocation with
   existing deterministic priorities and all hard constraints.
3. When replacement churn exceeds eight, atomically persist only the target
   fake CPUSet and constraint digest. A changed digest atomically replaces the
   old target.
4. If the committed allocation is fragmented or otherwise invalid, require a
   complete one-cycle repair. Reject the response if that repair exceeds the
   replacement budget.
5. If the committed allocation is valid and `replacementChurn <= 8`, return
   `desired`.
6. Otherwise construct a bounded intermediate `next` allocation:
   - preserve the advisor-requested quantity;
   - preserve unavoidable expansion or shrink;
   - apply at most four replacement pairs, equivalent to eight changed CPU IDs;
   - replace complete physical cores only;
   - retain deterministic candidate ordering;
   - validate all final hard constraints before returning.
7. Commit exactly one `next` state through the existing transaction boundary.
8. On the next advisor cycle, continue from the newly committed state toward
   the durable target. Once the committed fake union equals the target, remove
   the target checkpoint and fsync its directory.

No migration cursor or search frontier is persisted. A changed topology,
demand identity/quantity/class/eligibility, fake-demand set, or core-floor set
changes the digest and atomically supersedes the previous target.

## Lightweight Target Checkpoint

The checkpoint is a separate, versioned JSON record containing only:

- canonical constraint digest;
- sorted target CPU IDs;
- checksum over version, digest, and target.

Writes use temp-file write, file fsync, rename, and directory fsync. Loads are
strict: malformed JSON, unknown fields, unsupported versions, empty digests,
checksum mismatch, duplicate CPUs, CPUs outside topology, and non-whole-core
targets are corruption. DynamicPolicy initialization fails closed and leaves
the corrupted file intact for operator recovery.

## Intermediate Selection

The intermediate planner starts from `desired`, then preserves enough complete
cores from `current` to reduce replacement churn to the budget. Preservation is
valid only when the retained cores remain eligible and do not violate demand,
NUMA quota, donor-floor, or disjointness constraints.

Candidate ordering follows the existing deterministic preferences:

1. committed reclaim complete cores;
2. non-current-request non-bound complete cores;
3. largest DNB/SNB bound allocation;
4. current request's previous cpuset fallback;
5. stable topology and owner ordering.

If no valid intermediate allocation exists within the bounded search budget,
the cycle fails closed without state or bulkhead mutation.

## Idempotency and Transactions

- Repeating planning against the same committed state and advice returns the
  same `next` set.
- Restart reloads the same target and resumes from checkpointed committed
  state; it does not depend on reconstructing the target from current-state
  preferences.
- A successful state/bulkhead commit changes the committed input, allowing the
  next cycle to advance.
- A failed apply does not change committed state, so retries return the same
  intermediate result.
- Once `current == desired`, repeated cycles are strict no-ops.

## Error Handling

- Invalid committed baseline plus repair over budget: fail closed.
- Valid baseline but no legal staged step: fail closed with a staged-migration
  diagnostic.
- Search truncation before proving feasibility: return an explicit bounded
  search error.
- Apply or checkpoint failure: preserve the previous committed allocation.
- Migration target checkpoint corruption: fail DynamicPolicy initialization;
  never ignore or delete the corrupted record.
- Advisor post-commit reconciliation success removes and fsyncs both active
  and staging WAL slots.
- DNB apply rollback first restores memory with `persist=false`, then performs
  exactly one explicit `StoreState`; a write failure is reported while the
  restored in-memory source remains authoritative.

## Tests

- SMT1, SMT2, and SMT4 multi-cycle convergence.
- More than eight replacement IDs converges in deterministic bounded steps.
- Every intermediate allocation is whole-core aligned and quantity preserving.
- Every intermediate allocation satisfies per-NUMA and donor floors.
- Pure expansion and shrink complete in one cycle without migration charge.
- Invalid fragmented baseline cannot be partially committed.
- Apply failure retries the identical intermediate state.
- Advice, eligibility, and donor ownership changes redirect from the latest
  committed state.
- Restart continues from the lightweight target checkpoint without a
  persisted cursor or search plan.
- Corrupted migration target checkpoint fails DynamicPolicy initialization.
- Constraint changes replace the durable target; convergence removes it.
- Advisor WAL success and stale cleanup remove active plus staging files.
- DNB rollback performs one explicit write and covers one injected write
  failure.
- Final converged state is idempotent and does not trigger bulkhead mutation.

## Acceptance Criteria

- No successful cycle changes more than eight CPU IDs due to replacement.
- No intermediate committed allocation violates any hard constraint.
- A legal target with replacement churn greater than eight eventually
  converges under stable advice.
- Pure 48-to-204 expansion succeeds directly.
- Existing exact DC05 whole-core and idempotency regressions remain green.
