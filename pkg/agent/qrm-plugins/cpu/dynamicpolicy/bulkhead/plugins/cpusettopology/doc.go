// Package cpusettopology is the topology owner plugin: it builds and converges
// the cpuset partition DAG, and owns the disabled-reset transition state.
//
// Why it owns reset state: the manager no longer tracks a per-name reset state
// machine; the plugin reports NeedsDisabledReset() and is marked complete only
// inside a generation-fence commit, so a stale fence leaves the reset pending
// for the next round.
package cpusettopology
