# Mixed-QoS Repeated E2E Design

## Goal

Validate the frozen admission and scoped pending implementation under repeated
timing stress and mixed QoS placement on `fdbd-dc02-27-49--14`. The tested
binary must remain SHA256
`0594ded74169df6c4a17866461d3b01ecab2b7d7e7c92e39f99f9b387224037d`.

## Coverage

Run three independent canonical suites. Each suite contains reset dry-run,
reset, target dry-run, target, three standard rounds, five high-churn rounds,
three overlap-churn rounds, and final reset.

Then run the complete QoS matrix:

- cohorts `P0` (`preserve=false`) and `P1` (`preserve=true`);
- profiles `A1` through `A5`, plus the `P1/A2-follow` transition;
- one NUMA-exclusive dedicated Pod, two ordinary dedicated Pods, two SNB
  shared Pods, two non-binding shared Pods, and two system Pods per phase;
- stable, recreate, early-delete, configuration transition, and cleanup
  phases.

The profiles vary topology enforcement, shared overlap, dedicated overlap,
hard partition, shared ramp-up, and the NUMA reclaim floor.

## Isolation

Every suite uses a unique short run tag and private output directory. A suite
starts only after process identity, agent SHA, health, checkpoint readability,
whole-core alignment, and absence of another E2E process are confirmed.
Each suite owns only Pods carrying its run identity.

Configuration mutation is serialized through the QoS matrix controller. AQC
operations use its append-only journal, readback fence, and at-most-once
protocol. No workload phase may start from an unconfirmed AQC state.

## Failure Handling

The run fails closed on process identity drift, stale runtime evidence,
checkpoint drift, non-whole-core reclaim state, unresolved business/reclaim
overlap, cgroup hierarchy violation, sched-domain overlap, schedstat errors,
health failure, failed stable/recreate Pods, state-drain timeout, or reset
failure.

On the first failure, later stress phases stop. The failing state is preserved,
diagnostic logs are collected, and cleanup/final reset is attempted without
deleting unexplained checkpoint state.

## Success Criteria

All three canonical suites must report every `PHASE_DONE` with `rc=0` and end
with `FULL_E2E_DONE rc=0 final_reset_rc=0`.

All ten logical QoS profiles and the `P1/A2-follow` stage must complete with
their expected immutable evidence generations. Business Pod ownership,
checkpoint logical isolation, cgroup topology, workqueue, system-service,
health, sched-domain, and schedstat checks must pass. Both cohorts must restore
the original AQC state and complete final reset.

Every log archive must be re-opened successfully after creation. Its local
size and SHA256 must match the remote values.
