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

package dynamicpolicy

import (
	stderrors "errors"
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

// hardReclaimAdviceErrorKind classifies steady real-NUMA hard reclaim failures
// so callers can branch on them (errors.As) while the wrapped error text stays
// byte-for-byte compatible with existing monitoring and logs.
type hardReclaimAdviceErrorKind string

const (
	// hardReclaimErrOddAdviceNotWholeCore marks that an advisor published a reclaim
	// target that cannot be expressed as complete physical cores on this NUMA and
	// could not be normalized to a legal whole-core value.
	hardReclaimErrOddAdviceNotWholeCore hardReclaimAdviceErrorKind = "odd_advice_not_whole_core"
	// hardReclaimErrCommittedInvariant marks that an already-committed reclaim split
	// is itself not whole-core aligned, which is an invariant break rather than a
	// transient advice oscillation.
	hardReclaimErrCommittedInvariant hardReclaimAdviceErrorKind = "committed_invariant_broken"
	// hardReclaimErrInsufficientWholeCore marks that the whole-core DP could not
	// assemble the requested reclaim (the "NUMA %d needs %d more reclaim CPUs"
	// family).
	hardReclaimErrInsufficientWholeCore hardReclaimAdviceErrorKind = "insufficient_whole_core"
	// hardReclaimErrDonorFloorViolation marks that donating a core would push a
	// dedicated group below its requestQuantity floor.
	hardReclaimErrDonorFloorViolation hardReclaimAdviceErrorKind = "donor_floor_violation"
	// hardReclaimErrGlobalInfeasible marks that no reclaim selection is feasible at
	// all.
	hardReclaimErrGlobalInfeasible hardReclaimAdviceErrorKind = "global_infeasible"
)

// hardReclaimAdviceError wraps a planning error with a stable, machine-readable
// kind. Error() returns the wrapped error's own text so existing log/metric
// consumers are unchanged; Kind() exposes the classification.
type hardReclaimAdviceError struct {
	kind      hardReclaimAdviceErrorKind
	oddAdvice bool
	cause     error
}

func (e *hardReclaimAdviceError) Error() string {
	if e.cause == nil {
		return string(e.kind)
	}
	return e.cause.Error()
}

func (e *hardReclaimAdviceError) Unwrap() error { return e.cause }

func (e *hardReclaimAdviceError) Kind() hardReclaimAdviceErrorKind { return e.kind }

// adviceHadOddNUMA reports whether this error (or its wrapped chain) originated
// from a round in which an odd reclaim advice had to be normalized.
func adviceHadOddNUMA(err error) bool {
	var advErr *hardReclaimAdviceError
	if errorsAsAdvice(err, &advErr) {
		return advErr.oddAdvice
	}
	return false
}

// errorsAsAdvice is the errors.As entry point for hardReclaimAdviceError.
func errorsAsAdvice(err error, target **hardReclaimAdviceError) bool {
	return stderrors.As(err, target)
}

func classifyHardReclaimPlanningError(err error) hardReclaimAdviceErrorKind {
	if err == nil {
		return ""
	}
	var advErr *hardReclaimAdviceError
	if stderrors.As(err, &advErr) {
		return advErr.kind
	}
	msg := err.Error()
	switch {
	case containsSubstring(msg, "needs") && containsSubstring(msg, "more reclaim CPUs"):
		return hardReclaimErrInsufficientWholeCore
	case containsSubstring(msg, "overlapping donor ownership"),
		containsSubstring(msg, "request quantity"),
		containsSubstring(msg, "donation"):
		return hardReclaimErrDonorFloorViolation
	case containsSubstring(msg, "not core-aligned"),
		containsSubstring(msg, "violated core alignment"):
		return hardReclaimErrCommittedInvariant
	case containsSubstring(msg, "no feasible"),
		containsSubstring(msg, "search frontier truncated"):
		return hardReclaimErrGlobalInfeasible
	default:
		return hardReclaimErrGlobalInfeasible
	}
}

func containsSubstring(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// steadyReclaimNormalizeDecision records the outcome of normalizing one real
// NUMA's odd reclaim advice to a whole-core representable target.
type steadyReclaimNormalizeDecision string

const (
	// steadyReclaimDecisionAligned means the advice was already whole-core aligned
	// and the descriptors were left untouched.
	steadyReclaimDecisionAligned steadyReclaimNormalizeDecision = "aligned"
	// steadyReclaimDecisionRoundDown means the odd reclaim advice was rounded down
	// to the nearest whole-core multiple that stays anchored on the committed
	// reclaim (no dedicated migration required).
	steadyReclaimDecisionRoundDown steadyReclaimNormalizeDecision = "round_down"
	// steadyReclaimDecisionRoundUp means the odd reclaim advice was rounded up to a
	// whole-core multiple whose growth from committed is fully covered by free whole
	// cores (no donation out of running dedicated).
	steadyReclaimDecisionRoundUp steadyReclaimNormalizeDecision = "round_up"
	// steadyReclaimDecisionInfeasible means no legal whole-core value could be
	// chosen on this NUMA; the descriptors are passed through unchanged and the
	// downstream solver / committed fallback owns the decision.
	steadyReclaimDecisionInfeasible steadyReclaimNormalizeDecision = "infeasible"
)

type steadyReclaimNormalizeRecord struct {
	NUMAID            int
	CPUsPerCore       int
	AdvisorReclaim    int
	ReclaimAfter      int
	CommittedReclaim  int
	FreeWholeCoreCPUs int
	Decision          steadyReclaimNormalizeDecision
	Reason            string
}

type steadyReclaimNormalizeReport struct {
	Records      []steadyReclaimNormalizeRecord
	HadOddAdvice bool
}

// normalizeSteadyRealNUMATargets joints the mandatory-reclaim and dedicated
// targets of each real NUMA and rewrites an odd (non-whole-core) reclaim advice
// into a whole-core representable target. It is only wired on the steady
// (!hardActive), disjoint-hard-partition path; fake-NUMA reclaim blocks and
// skipped NUMAs are left to their own solvers.
//
// The choice between floor(R/w)*w and ceil(R/w)*w is governed by what can be
// materialized without migrating running dedicated cores: a candidate reclaim
// target T must not shrink the committed reclaim (T >= committed) and any growth
// beyond committed must be fully covered by free whole cores (never by donating a
// core out of a running dedicated group). Among feasible candidates the closest
// to committed wins, then the closest to the advisor value, then the smaller
// reclaim. When no legal whole-core value exists the NUMA is passed through
// unchanged so the existing solver / committed fallback owns it.
func normalizeSteadyRealNUMATargets(
	descriptors []advisorBlockDescriptor,
	available machine.CPUSet,
	topology *machine.CPUTopology,
	skipNUMAs sets.Int,
) ([]advisorBlockDescriptor, steadyReclaimNormalizeReport, error) {
	var report steadyReclaimNormalizeReport
	if topology == nil {
		return nil, report, fmt.Errorf("cannot normalize steady real-NUMA targets with nil CPU topology")
	}
	w := topology.CPUsPerCore()
	if w <= 1 {
		// SMT1 (or unknown width): every CPU is its own core, there is no odd/even
		// distinction, so descriptors pass through byte-for-byte.
		return append([]advisorBlockDescriptor(nil), descriptors...), report, nil
	}

	normalized := append([]advisorBlockDescriptor(nil), descriptors...)

	type numaGroup struct {
		mandatory []int
		dedicated []int
	}
	groups := make(map[int]*numaGroup)
	for i, descriptor := range normalized {
		if descriptor.NUMAID == commonstate.FakedNUMAID || skipNUMAs.Has(descriptor.NUMAID) {
			continue
		}
		g, found := groups[descriptor.NUMAID]
		if !found {
			g = &numaGroup{}
			groups[descriptor.NUMAID] = g
		}
		switch descriptor.Class {
		case advisorBlockClassMandatoryReclaim:
			g.mandatory = append(g.mandatory, i)
		case advisorBlockClassDedicated:
			g.dedicated = append(g.dedicated, i)
		}
	}

	numaIDs := make([]int, 0, len(groups))
	for numaID := range groups {
		numaIDs = append(numaIDs, numaID)
	}
	sort.Ints(numaIDs)

	floorReclaim := minimumHardReclaimCoresPerNUMA * w
	for _, numaID := range numaIDs {
		group := groups[numaID]
		if len(group.mandatory) == 0 {
			continue
		}
		sort.Slice(group.mandatory, func(a, b int) bool {
			return advisorBlockDescriptorLess(normalized[group.mandatory[a]], normalized[group.mandatory[b]])
		})
		sort.Slice(group.dedicated, func(a, b int) bool {
			return advisorBlockDescriptorLess(normalized[group.dedicated[a]], normalized[group.dedicated[b]])
		})

		advisorReclaim := 0
		reclaimEligible := machine.NewCPUSet()
		committedReclaim := machine.NewCPUSet()
		for _, mi := range group.mandatory {
			advisorReclaim += normalized[mi].Quantity
			reclaimEligible = reclaimEligible.Union(
				normalized[mi].Eligible.Intersection(available).Intersection(
					topology.CPUDetails.CPUsInNUMANodes(numaID)))
			committedReclaim = committedReclaim.Union(normalized[mi].Committed)
		}
		committedReclaim = committedReclaim.Intersection(reclaimEligible)

		if advisorReclaim%w == 0 {
			report.Records = append(report.Records, steadyReclaimNormalizeRecord{
				NUMAID:           numaID,
				CPUsPerCore:      w,
				AdvisorReclaim:   advisorReclaim,
				ReclaimAfter:     advisorReclaim,
				CommittedReclaim: committedReclaim.Size(),
				Decision:         steadyReclaimDecisionAligned,
			})
			continue
		}
		report.HadOddAdvice = true

		dedicatedCommitted := machine.NewCPUSet()
		dedicatedEligibleCap := 0
		for _, di := range group.dedicated {
			dedicatedCommitted = dedicatedCommitted.Union(normalized[di].Committed)
			dedicatedEligibleCap += normalized[di].Eligible.Size()
		}
		freeCPUs := reclaimEligible.Difference(committedReclaim).Difference(dedicatedCommitted)
		freeCores := coreAlignedCandidates(topology, freeCPUs, machine.NewCPUSet())
		freeWholeCoreCPUs := 0
		for _, core := range freeCores {
			freeWholeCoreCPUs += core.cpus.Size()
		}

		lowerR := (advisorReclaim / w) * w
		upperR := ((advisorReclaim + w - 1) / w) * w
		committedSize := committedReclaim.Size()

		type candidate struct {
			target int
		}
		feasible := make([]candidate, 0, 2)
		for _, t := range []int{lowerR, upperR} {
			growth := t - committedSize
			if t < floorReclaim {
				continue
			}
			if t > reclaimEligible.Size() {
				continue
			}
			if growth < 0 {
				// never shrink an already-committed reclaim below its anchor.
				continue
			}
			if growth > freeWholeCoreCPUs {
				// growth beyond committed must be covered by free whole cores, never
				// by donating a core out of running dedicated.
				continue
			}
			feasible = append(feasible, candidate{target: t})
		}

		chosen := -1
		reason := ""
		switch len(feasible) {
		case 0:
			reason = "no legal whole-core reclaim value within committed+free bounds; passing through"
		case 1:
			chosen = feasible[0].target
			reason = "only feasible whole-core value anchored on committed reclaim"
		default:
			sort.Slice(feasible, func(a, b int) bool {
				da := feasible[a].target - committedSize
				db := feasible[b].target - committedSize
				if da != db {
					return da < db
				}
				aa := absInt(feasible[a].target - advisorReclaim)
				ab := absInt(feasible[b].target - advisorReclaim)
				if aa != ab {
					return aa < ab
				}
				return feasible[a].target < feasible[b].target
			})
			chosen = feasible[0].target
			reason = "closest whole-core value to committed reclaim"
		}

		if chosen < 0 {
			report.Records = append(report.Records, steadyReclaimNormalizeRecord{
				NUMAID:            numaID,
				CPUsPerCore:       w,
				AdvisorReclaim:    advisorReclaim,
				ReclaimAfter:      advisorReclaim,
				CommittedReclaim:  committedSize,
				FreeWholeCoreCPUs: freeWholeCoreCPUs,
				Decision:          steadyReclaimDecisionInfeasible,
				Reason:            reason,
			})
			general.InfoS("steady real-NUMA reclaim advice not whole-core representable; passing through",
				"numaID", numaID,
				"cpusPerCore", w,
				"advisorReclaim", advisorReclaim,
				"committedReclaim", committedSize,
				"freeWholeCoreCPUs", freeWholeCoreCPUs,
				"floorReclaim", floorReclaim,
				"reason", reason)
			continue
		}

		reclaimDelta := chosen - advisorReclaim
		dedicatedDelta := -reclaimDelta

		// Snapshot the dedicated quantities before distributing the delta so a
		// failed absorb (remaining != 0 in either direction) can roll back every
		// partially-taken dedicated block, not just the mandatory block. Without
		// this, on SMT>2 a 2..w-1 CPU delta partially absorbed by an earlier
		// dedicated block would leave that block raised (round-down) or lowered
		// (round-up) and break per-NUMA total conservation while still being
		// recorded as a pass-through.
		dedicatedSnapshot := make(map[int]int, len(group.dedicated))
		for _, di := range group.dedicated {
			dedicatedSnapshot[di] = normalized[di].Quantity
		}

		// Distribute the reclaim delta on the first sorted mandatory block and the
		// dedicated delta across sorted dedicated blocks, conserving the NUMA total.
		normalized[group.mandatory[0]].Quantity += reclaimDelta
		remaining := dedicatedDelta
		for _, di := range group.dedicated {
			if remaining == 0 {
				break
			}
			take := remaining
			if take > 0 {
				headroom := normalized[di].Eligible.Size() - normalized[di].Quantity
				if headroom < take {
					take = headroom
				}
			} else {
				if -take > normalized[di].Quantity {
					take = -normalized[di].Quantity
				}
			}
			if take == 0 {
				continue
			}
			normalized[di].Quantity += take
			remaining -= take
		}
		if remaining != 0 {
			// dedicated pool cannot absorb the delta in either direction (round-down:
			// not enough headroom to grow; round-up: not enough quantity to shrink);
			// roll back the mandatory delta and every partially-taken dedicated block,
			// then pass through unchanged.
			normalized[group.mandatory[0]].Quantity -= reclaimDelta
			for _, di := range group.dedicated {
				normalized[di].Quantity = dedicatedSnapshot[di]
			}
			report.Records = append(report.Records, steadyReclaimNormalizeRecord{
				NUMAID:            numaID,
				CPUsPerCore:       w,
				AdvisorReclaim:    advisorReclaim,
				ReclaimAfter:      advisorReclaim,
				CommittedReclaim:  committedSize,
				FreeWholeCoreCPUs: freeWholeCoreCPUs,
				Decision:          steadyReclaimDecisionInfeasible,
				Reason:            "dedicated pool cannot absorb the reclaim delta",
			})
			continue
		}

		decision := steadyReclaimDecisionRoundDown
		if chosen > advisorReclaim {
			decision = steadyReclaimDecisionRoundUp
		}
		report.Records = append(report.Records, steadyReclaimNormalizeRecord{
			NUMAID:            numaID,
			CPUsPerCore:       w,
			AdvisorReclaim:    advisorReclaim,
			ReclaimAfter:      chosen,
			CommittedReclaim:  committedSize,
			FreeWholeCoreCPUs: freeWholeCoreCPUs,
			Decision:          decision,
			Reason:            reason,
		})
		general.InfoS("steady real-NUMA reclaim advice normalized to whole-core target",
			"numaID", numaID,
			"cpusPerCore", w,
			"mandatoryBefore", advisorReclaim,
			"mandatoryAfter", chosen,
			"committedReclaim", committedSize,
			"freeWholeCoreCPUs", freeWholeCoreCPUs,
			"dedicatedEligibleCap", dedicatedEligibleCap,
			"decision", decision,
			"reason", reason)
	}

	return normalized, report, nil
}

const (
	metricNameSteadyReclaimNormalizeDecision = "qrm_hard_reclaim_normalize_decision_total"
	metricNameSteadyReclaimPlanOutcome       = "qrm_hard_reclaim_plan_outcome_total"
)

// emitSteadyReclaimNormalizeMetrics records, with low-cardinality labels only
// (decision/outcome), how the steady real-NUMA normalization decided each NUMA.
// Per-NUMA / per-block detail stays in structured logs.
func (p *DynamicPolicy) emitSteadyReclaimNormalizeMetrics(report steadyReclaimNormalizeReport) {
	if p == nil || p.emitter == nil {
		return
	}
	for _, record := range report.Records {
		_ = p.emitter.StoreInt64(metricNameSteadyReclaimNormalizeDecision, 1,
			metrics.MetricTypeNameCount,
			metrics.MetricTag{Key: "decision", Val: string(record.Decision)})
	}
}

// annotateSteadyReclaimNormalizeError attaches a stable typed classification to a
// steady real-NUMA planning failure, flagging whether the failure originated in a
// round that had to normalize odd advice. The wrapped text is preserved.
func (p *DynamicPolicy) annotateSteadyReclaimNormalizeError(
	err error,
	report steadyReclaimNormalizeReport,
) error {
	if err == nil {
		return nil
	}
	kind := classifyHardReclaimPlanningError(err)
	if kind == "" {
		return err
	}
	annotated := &hardReclaimAdviceError{
		kind:      kind,
		oddAdvice: report.HadOddAdvice,
		cause:     err,
	}
	if p != nil && p.emitter != nil {
		_ = p.emitter.StoreInt64(metricNameSteadyReclaimPlanOutcome, 1,
			metrics.MetricTypeNameCount,
			metrics.MetricTag{Key: "outcome", Val: string(kind)})
	}
	return annotated
}
