/*
Copyright 2026 The Katalyst Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cpu

import (
	"k8s.io/klog/v2"

	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/assembler/provisionassembler"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/types"
)

// reclaimConstraintScopeState tracks the per-scope ACK and ceiling for one
// ReclaimConstraintScope. Each scope is independent: an un-ACKed scope holds
// its own ceiling without blocking any other scope.
type reclaimConstraintScopeState struct {
	// lastPublished is the reclaim pool size we published for this scope in the
	// previous successful cycle. It is compared against the observed value to
	// confirm QRM has applied the ceiling (ACK).
	lastPublished int
	// hasPublished is false before the first successful commit for this scope
	// and after the scope leaves the active set (RampUp exit). A scope without
	// a published baseline cannot advance; it holds at the floor until QRM
	// acknowledges the first publication.
	hasPublished bool
	// ceiling is the most recently published ceiling for this scope. It is
	// preserved across RampUp exit to avoid floor<->ceiling oscillation when
	// the same scope re-activates shortly after.
	ceiling int
}

type reclaimConstraintGuard struct {
	scopeState map[provisionassembler.ReclaimConstraintScope]reclaimConstraintScopeState
	targets    map[string]reclaimConstraintTarget
}

type reclaimConstraintTarget = types.ReclaimConstraintTarget

// constraint decides the reclaim constraint and per-scope ceilings for the
// current cycle.
//
// Activation is scope-driven: only scopes listed in activeScopes are
// constrained. Every other scope passes through ApplyReclaimConstraint
// untouched, so a ramp-up on one NUMA cannot compress the reclaim pool of an
// unrelated (dedicated) NUMA.
//
// The ACK check is per-scope: for each active scope, the observed reclaim
// size is compared against the last published size. A scope whose ACK has
// not arrived holds at its remembered ceiling; an ACKed scope advances
// toward its target by at most maxRampUpStep. One un-ACKed scope never
// blocks another.
//
// ACK is defined as observed == lastPublished (value equality), deliberately
// with no wall-clock timeout: advancing the reclaim ceiling is the only way
// this guard can hand dedicated CPUs back to reclaim. If QRM has not actually
// reached the published ceiling yet, forcing an advance on a timer would
// over-claim reclaim before the lower bound is safe. Holding at the last
// confirmed ceiling is therefore the fail-closed behavior, not a livelock to
// break. A coincidental equality is not a false ACK: it means QRM is already
// at the published size, which is exactly the steady state we want to advance
// from. The observed value is broken down per scope, so QRM drift on an
// unrelated NUMA never advances or stalls this scope.
func (g *reclaimConstraintGuard) constraint(
	activeScopes map[provisionassembler.ReclaimConstraintScope]bool,
	observedByScope map[provisionassembler.ReclaimConstraintScope]int,
	observedOKByScope map[provisionassembler.ReclaimConstraintScope]bool,
	maxRampUpStep int,
) (
	provisionassembler.ReclaimConstraint,
	map[provisionassembler.ReclaimConstraintScope]int,
	map[provisionassembler.ReclaimConstraintScope]bool,
) {
	if len(activeScopes) == 0 {
		return provisionassembler.ReclaimConstraintNone, nil, nil
	}
	if maxRampUpStep <= 0 {
		return provisionassembler.ReclaimConstraintReservedFloor, nil, cloneReclaimActiveScopes(activeScopes)
	}

	// Split active scopes into those that may advance (ACK passed) and those
	// that must hold. The current ceiling of each scope is its remembered
	// value from scopeState (0 for a never-published scope).
	toAdvance := make(map[provisionassembler.ReclaimConstraintScope]int)
	held := make(map[provisionassembler.ReclaimConstraintScope]int)
	for scope := range activeScopes {
		state, ok := g.scopeState[scope]
		observed, observedOK := observedByScope[scope], observedOKByScope[scope]
		acked := ok && state.hasPublished && observedOK && observed == state.lastPublished
		current := 0
		if ok {
			current = state.ceiling
		}
		if acked {
			toAdvance[scope] = current
		} else {
			held[scope] = current
		}
	}

	advanced := advanceReclaimCeilings(toAdvance, g.targets, maxRampUpStep)

	ceilings := make(map[provisionassembler.ReclaimConstraintScope]int, len(activeScopes))
	for scope, ceiling := range held {
		ceilings[scope] = ceiling
	}
	for scope, ceiling := range advanced {
		ceilings[scope] = ceiling
	}
	return provisionassembler.ReclaimConstraintReservedFloor, ceilings, cloneReclaimActiveScopes(activeScopes)
}

// commit records the published per-scope ceilings and ACK baselines after a
// successful update cycle.
//
// Scopes that remain active update their lastPublished/hasPublished/ceiling.
// Scopes that leave the active set (RampUp exit) retain their ceiling but
// reset hasPublished to false, so re-activation starts from a clean ACK
// slate without losing the anti-flap ceiling. When no scope is active the
// entire guard is cleared.
func (g *reclaimConstraintGuard) commit(
	activeScopes map[provisionassembler.ReclaimConstraintScope]bool,
	ceilings map[provisionassembler.ReclaimConstraintScope]int,
	targets map[string]reclaimConstraintTarget,
	publishedByScope map[provisionassembler.ReclaimConstraintScope]int,
	maxRampUpStep int,
) {
	if len(activeScopes) == 0 {
		*g = reclaimConstraintGuard{}
		return
	}

	g.targets = cloneReclaimTargets(targets)
	published := publishedReclaimCeilings(ceilings, targets)

	newState := make(map[provisionassembler.ReclaimConstraintScope]reclaimConstraintScopeState, len(g.scopeState))
	// Preserve but de-ACK scopes that left the active set.
	for scope, state := range g.scopeState {
		if activeScopes[scope] {
			continue
		}
		state.hasPublished = false
		newState[scope] = state
	}
	// Update scopes that remain active.
	for scope := range activeScopes {
		state := reclaimConstraintScopeState{
			lastPublished: publishedByScope[scope],
			hasPublished:  true,
		}
		if ceiling, ok := published[scope]; ok {
			state.ceiling = ceiling
		} else if previous, ok := g.scopeState[scope]; ok {
			state.ceiling = previous.ceiling
		}
		newState[scope] = state
	}
	g.scopeState = newState

	if maxRampUpStep <= 0 {
		for scope, state := range g.scopeState {
			state.ceiling = 0
			g.scopeState[scope] = state
		}
		klog.Warningf("[qosaware-cpu] keep reclaim constraint at reserved floor because MaxRampUpStep is non-positive: %d", maxRampUpStep)
	}
}

func cloneReclaimActiveScopes(
	activeScopes map[provisionassembler.ReclaimConstraintScope]bool,
) map[provisionassembler.ReclaimConstraintScope]bool {
	cloned := make(map[provisionassembler.ReclaimConstraintScope]bool, len(activeScopes))
	for scope, active := range activeScopes {
		cloned[scope] = active
	}
	return cloned
}

func cloneReclaimTargets(targets map[string]reclaimConstraintTarget) map[string]reclaimConstraintTarget {
	cloned := make(map[string]reclaimConstraintTarget, len(targets))
	for scope, target := range targets {
		cloned[scope] = target
	}
	return cloned
}

// advanceReclaimCeilings advances each scope in current toward its target
// Desired by at most maxRampUpStep. Scopes without a target keep their
// current ceiling. When Desired shrinks below the current ceiling, the
// ceiling follows Desired immediately (no slow ramp-down).
func advanceReclaimCeilings(
	current map[provisionassembler.ReclaimConstraintScope]int,
	targets map[string]reclaimConstraintTarget,
	maxRampUpStep int,
) map[provisionassembler.ReclaimConstraintScope]int {
	next := make(map[provisionassembler.ReclaimConstraintScope]int, len(current))
	for scope, currentCeiling := range current {
		target, ok := targets[string(scope)]
		if !ok {
			next[scope] = currentCeiling
			continue
		}
		baseline := target.Floor
		if currentCeiling > baseline {
			baseline = currentCeiling
		}
		if target.Desired > baseline {
			increment := target.Desired - baseline
			if increment > maxRampUpStep {
				increment = maxRampUpStep
			}
			next[scope] = baseline + increment
		} else {
			next[scope] = target.Desired
		}
	}
	return next
}

// publishedReclaimCeilings computes the actually-published ceiling for each
// target scope: min(Desired, max(Floor, configured ceiling)). A scope
// without a configured ceiling publishes at its Floor.
func publishedReclaimCeilings(
	ceilings map[provisionassembler.ReclaimConstraintScope]int,
	targets map[string]reclaimConstraintTarget,
) map[provisionassembler.ReclaimConstraintScope]int {
	published := make(map[provisionassembler.ReclaimConstraintScope]int, len(targets))
	for scopeStr, target := range targets {
		scope := provisionassembler.ReclaimConstraintScope(scopeStr)
		ceiling := target.Floor
		if configured, ok := ceilings[scope]; ok && configured > ceiling {
			ceiling = configured
		}
		size := target.Desired
		if size > ceiling {
			size = ceiling
		}
		published[scope] = size
	}
	return published
}
