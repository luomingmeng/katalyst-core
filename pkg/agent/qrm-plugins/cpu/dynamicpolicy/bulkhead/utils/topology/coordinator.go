/*
Copyright 2022 The Katalyst Authors.

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

package topology

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	cgroupclient "github.com/kubewharf/katalyst-core/pkg/util/cgroup/client"
	cgcommon "github.com/kubewharf/katalyst-core/pkg/util/cgroup/common"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

const (
	// Auto mode retains retry/replan headroom for small workloads while scaling
	// above it when the drain workload requires more rounds.
	defaultCoordinatorAutoRounds = 32
	// One completion round covers phase handoffs that become drainable only
	// after the last workload-derived transfer round.
	coordinatorPhaseCompletionRounds = 1
)

var ErrCoordinatorPlanStale = errors.New("coordinator plan is stale and requires replan")

type PlanStaleError struct {
	Rel       string
	Direction WriteDirection
	Resource  string
	Current   string
	Target    string
	Err       error
}

func (e *PlanStaleError) Error() string {
	message := fmt.Sprintf("%v: rel=%q direction=%s resource=%s current=%s target=%s",
		ErrCoordinatorPlanStale, e.Rel, e.Direction, e.Resource, e.Current, e.Target)
	if e.Err != nil {
		message += ": " + e.Err.Error()
	}
	return message
}

func (e *PlanStaleError) Unwrap() error { return ErrCoordinatorPlanStale }

func (e *PlanStaleError) ReplanRequired() bool { return true }

type CoordinatorBlockedError struct {
	Blocker error
}

func (e *CoordinatorBlockedError) Error() string {
	return fmt.Sprintf("topology coordinator blocked after repeated no-progress rounds: %v", e.Blocker)
}

func (e *CoordinatorBlockedError) Unwrap() error { return e.Blocker }

type CoordinatorMode string

const (
	CoordinatorModeNormal CoordinatorMode = "normal"
	CoordinatorModeReset  CoordinatorMode = "reset"
)

type ModeGuard struct {
	mode CoordinatorMode
	gate *ModeGate
}

func NormalModeGuard() ModeGuard { return ModeGuard{mode: CoordinatorModeNormal} }

func ResetModeGuard() ModeGuard { return ModeGuard{mode: CoordinatorModeReset} }

func NormalModeGuardWithGate(gate *ModeGate) ModeGuard {
	return ModeGuard{mode: CoordinatorModeNormal, gate: gate}
}

func ResetModeGuardWithGate(gate *ModeGate) ModeGuard {
	return ModeGuard{mode: CoordinatorModeReset, gate: gate}
}

func (g ModeGuard) modeOrDefault() CoordinatorMode {
	if g.mode == "" {
		return CoordinatorModeNormal
	}
	return g.mode
}

func (g ModeGuard) validate() error {
	switch g.modeOrDefault() {
	case CoordinatorModeNormal, CoordinatorModeReset:
		return nil
	default:
		return fmt.Errorf("unsupported coordinator mode %q", g.mode)
	}
}

type ModeGate struct {
	mu     sync.Mutex
	active CoordinatorMode
	held   bool
}

func NewModeGate() *ModeGate { return &ModeGate{} }

type ModeToken struct {
	gate *ModeGate
	mode CoordinatorMode
}

func (t ModeToken) Exit() {
	if t.gate == nil {
		return
	}
	t.gate.exit(t.mode)
}

type CoordinatorBusyError struct {
	Requested CoordinatorMode
	Active    CoordinatorMode
}

func (e *CoordinatorBusyError) Error() string {
	return fmt.Sprintf("topology coordinator busy: requested=%s active=%s", e.Requested, e.Active)
}

func (g ModeGuard) TryEnter() (ModeToken, error) {
	mode := g.modeOrDefault()
	if err := g.validate(); err != nil {
		return ModeToken{}, err
	}
	if g.gate == nil {
		return ModeToken{mode: mode}, nil
	}
	return g.gate.tryEnter(mode)
}

func (g *ModeGate) tryEnter(mode CoordinatorMode) (ModeToken, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.held {
		return ModeToken{}, &CoordinatorBusyError{Requested: mode, Active: g.active}
	}
	g.held = true
	g.active = mode
	return ModeToken{gate: g, mode: mode}, nil
}

func (g *ModeGate) exit(mode CoordinatorMode) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.held || g.active != mode {
		return
	}
	g.held = false
	g.active = ""
}

type ConvergenceState string

const (
	ConvergenceStateConverged              ConvergenceState = "converged"
	ConvergenceStateParentSafeLeafDeferred ConvergenceState = "parent_safe_leaf_deferred"
	ConvergenceStateNonConverged           ConvergenceState = "non_converged"
	ConvergenceStateBlocked                ConvergenceState = "blocked"
)

type ConvergenceObjective string

const (
	ConvergenceObjectiveFull       ConvergenceObjective = "full"
	ConvergenceObjectiveParentSafe ConvergenceObjective = "parent_safe"
)

func (o ConvergenceObjective) orFullDefault() ConvergenceObjective {
	if o == "" {
		return ConvergenceObjectiveFull
	}
	return o
}

type AdmissionConvergenceBudget struct {
	MaxRequiredWrites int
}

type ConvergenceResult struct {
	Attempted int
	Applied   int
	Skipped   int
	Failed    int
	// Deferred counts controlled nodes whose final generational shrink could not
	// land this pass but left the parent as a valid cgroup v1 superset. These are
	// not failures: the next periodical reconcile finishes the shrink. A non-zero
	// Deferred implies Converged is false.
	Deferred          int
	Converged         bool
	State             ConvergenceState
	ConvergenceReport ConvergenceReport
	Journal           []AppliedPlanOperation
	Rounds            []RoundOutcome
	FinalSnapshot     *CompleteSnapshot
	// FinalSnapshotCurrent is true only when FinalSnapshot is the snapshot used
	// for the successful convergence decision and no later hierarchy write ran.
	FinalSnapshotCurrent bool
	ParentSafe           bool
	DeferredLeafCount    int
	DeferredCPUCount     int
}

func (r ConvergenceResult) FirstBlocker() string {
	for _, round := range r.Rounds {
		if round.Blocker != nil {
			return round.Blocker.Error()
		}
	}
	if len(r.ConvergenceReport.NonConvergedTargets) > 0 {
		mismatch := r.ConvergenceReport.NonConvergedTargets[0]
		return fmt.Sprintf("rel=%q observed=%s target=%s observed_mems=%q target_mems=%q reason=%s",
			mismatch.Rel, mismatch.Observed.String(), mismatch.Target.String(),
			mismatch.ObservedMems, mismatch.TargetMems, mismatch.Reason)
	}
	return "none"
}

type CoordinatorInput struct {
	DAG    *TopoDAG
	Cgroup cgroupclient.CgroupClient
	Mems   string
	Mode   ModeGuard

	CPUDetails          machine.CPUDetails
	ReservedCPUSet      machine.CPUSet
	ExpectedCPUSetByRel map[string]machine.CPUSet
	// ProtectedPendingCPUSet is the union of container allocations that already
	// exist in QRM state but whose cgroup leaf has not been created yet (pod
	// admit window). These have no resolvable rel, so the writer folds them into
	// the primary node's effective target to guarantee the primary cgroup never
	// shrinks below an allocation that is about to materialize.
	ProtectedPendingCPUSet machine.CPUSet
	// ProtectedCPUSetByRel records cgroup rels whose current/pending cpuset must
	// stay covered during a short runtime creation window.
	ProtectedCPUSetByRel map[string]machine.CPUSet
	// TraversalBoundaries are normalized cgroup rels that reset propagation
	// must neither write nor descend into.
	TraversalBoundaries map[string]struct{}
	// RequiredIdentityByRel binds preflight classification to every coordinator
	// snapshot before that snapshot can authorize a hierarchy write or replan.
	RequiredIdentityByRel map[string]CgroupIdentity
	// ExpectedAbsentRels binds preflight absence classification to every
	// coordinator snapshot before that snapshot can authorize a hierarchy write.
	ExpectedAbsentRels  map[string]struct{}
	Objective           ConvergenceObjective
	DeferredCPUSetByRel map[string]machine.CPUSet
	AdmissionBudget     *AdmissionConvergenceBudget

	Budget         ConvergenceBudget
	DrainSelection DrainSelectionPolicy
	// PublishFinalSnapshot runs while both the coordinator mode gate and the
	// caller's manager mutex are still held. The coordinator invokes it only
	// after a fresh complete snapshot has the same publish-relevant controlled
	// and explicit-leaf state as the snapshot that proved convergence.
	PublishFinalSnapshot func(*CompleteSnapshot) error
	// PublishParentSafeSnapshot publishes a partition-safe view while exact
	// cleanup for the supplied rels remains deferred.
	PublishParentSafeSnapshot func(*CompleteSnapshot, map[string]struct{}) error
}

type TopologyCoordinator struct{}

func (c TopologyCoordinator) Converge(ctx context.Context, in CoordinatorInput) (ConvergenceResult, error) {
	res := ConvergenceResult{}
	if in.DAG == nil {
		return res, errors.New("TopologyCoordinator.Converge: nil DAG")
	}
	if in.Cgroup == nil {
		return res, errors.New("TopologyCoordinator.Converge: nil Cgroup client")
	}
	if err := in.Mode.validate(); err != nil {
		return res, err
	}
	switch in.Objective.orFullDefault() {
	case ConvergenceObjectiveFull, ConvergenceObjectiveParentSafe:
	default:
		return res, fmt.Errorf("unsupported convergence objective %q", in.Objective)
	}
	token, err := in.Mode.TryEnter()
	if err != nil {
		return res, err
	}
	defer token.Exit()
	budgetLimit := BudgetWithInvocationDeadline(ctx, in.Budget, time.Now())
	budget := NewBudgetTracker(budgetLimit)
	var result ConvergenceResult
	switch in.Mode.modeOrDefault() {
	case CoordinatorModeReset:
		result, err = c.convergeReset(ctx, in, &res, budget)
	case CoordinatorModeNormal:
		result, err = c.convergeNormal(ctx, in, &res, budget)
	default:
		return res, fmt.Errorf("unsupported coordinator mode %q", in.Mode.modeOrDefault())
	}
	successfulCurrentProof := result.FinalSnapshotCurrent &&
		result.FinalSnapshot != nil &&
		(result.Converged || result.ParentSafe)
	if deadlineErr := ctx.Err(); deadlineErr != nil &&
		in.Objective.orFullDefault() == ConvergenceObjectiveParentSafe &&
		!successfulCurrentProof {
		deadlineSummary := formatParentSafetyDeadlineSummary(result.ConvergenceReport)
		result.Converged = false
		result.ParentSafe = false
		result.FinalSnapshot = nil
		result.FinalSnapshotCurrent = false
		result.State = ConvergenceStateNonConverged
		return result, fmt.Errorf("admission parent-safe deadline exceeded: %w; last_report=%s", deadlineErr, deadlineSummary)
	}
	var snapshotErr *SnapshotError
	if errors.As(err, &snapshotErr) && snapshotErr.Class == HierarchyErrorStale {
		result.Converged = false
		result.State = ConvergenceStateNonConverged
		result.FinalSnapshot = nil
		result.FinalSnapshotCurrent = false
	}
	return result, err
}

func formatParentSafetyDeadlineSummary(report ConvergenceReport) string {
	parts := []string{
		fmt.Sprintf("fully_converged=%v", report.FullyConverged),
		fmt.Sprintf("pending_to_primary=%s", report.PendingToPrimary.String()),
		fmt.Sprintf("pending_to_reclaim=%s", report.PendingToReclaim.String()),
		fmt.Sprintf("cleanup_pending_primary=%s", report.CleanupPendingPrimary.String()),
		fmt.Sprintf("cleanup_pending_reclaim=%s", report.CleanupPendingReclaim.String()),
		fmt.Sprintf("non_converged_count=%d", len(report.NonConvergedTargets)),
	}
	if len(report.NonConvergedTargets) > 0 {
		limit := len(report.NonConvergedTargets)
		if limit > 5 {
			limit = 5
		}
		for i := 0; i < limit; i++ {
			item := report.NonConvergedTargets[i]
			parts = append(parts, fmt.Sprintf("non_converged[%d]=rel:%s reason:%s observed:%s target:%s observed_mems:%s target_mems:%s",
				i, item.Rel, item.Reason, item.Observed.String(), item.Target.String(), item.ObservedMems, item.TargetMems))
		}
	}
	return strings.Join(parts, ",")
}

func (c TopologyCoordinator) convergeNormal(ctx context.Context, in CoordinatorInput, res *ConvergenceResult, budget *BudgetTracker) (ConvergenceResult, error) {
	if len(in.CPUDetails) == 0 {
		return *res, errors.New("TopologyCoordinator.Converge: empty CPUDetails in normal mode")
	}
	allowEmptyTarget := in.Cgroup.Version(ctx) == cgroupclient.CgroupVersionV2
	effectiveTargets, err := computeEffectiveTargets(in.DAG, allowEmptyTarget, in.CPUDetails, in.ProtectedPendingCPUSet, in.ProtectedCPUSetByRel)
	if err != nil {
		return *res, err
	}
	parentSafetyTargets := desiredTargets(in.DAG)
	snapshotDriver, err := snapshotDriverForCoordinator(ctx, in.Cgroup)
	if err != nil {
		return *res, err
	}
	defer snapshotDriver.Close()
	round := newCoordinatorRoundWithBudget(in.DAG, in.Cgroup, effectiveTargets, in.CPUDetails, in.ReservedCPUSet, in.DrainSelection, budget)
	round.dynamicByRel = cloneCPUSetMap(in.ExpectedCPUSetByRel)
	round.objective = in.Objective.orFullDefault()
	round.deferredByRel = cloneCPUSetMap(in.DeferredCPUSetByRel)
	round.admissionBudget = in.AdmissionBudget
	round.allowEmptyTarget = allowEmptyTarget
	round.protectedPending = in.ProtectedPendingCPUSet.Clone()
	round.protectedByRel = cloneCPUSetMap(in.ProtectedCPUSetByRel)
	round.requiredIdentityByRel = cloneIdentityMap(in.RequiredIdentityByRel)
	round.expectedAbsentRels = cloneRelSet(in.ExpectedAbsentRels)
	round.snapshotSource = newCompleteSnapshotSource(snapshotDriver, in.DAG, budget, in.TraversalBoundaries)
	round.driver = snapshotDriver
	initialSnapshot, err := round.nextSnapshot(ctx)
	if err != nil {
		return *res, err
	}
	effectiveTargets = normalizeV1NonEmptyReclaimDesiredTargets(in.DAG, initialSnapshot, effectiveTargets, allowEmptyTarget)
	round.targetByRel = effectiveTargets
	round.maxRounds = coordinatorMaxRoundsForPlanInput(PhasePlanInput{
		Kind: PhaseDrain, DAG: in.DAG, Snapshot: initialSnapshot,
		DesiredByRel: effectiveTargets, AllowedCPUs: round.allowedCPUs(),
		ProtectedPending: in.ProtectedPendingCPUSet, ProtectedByRel: in.ProtectedCPUSetByRel,
		CPUDetails: in.CPUDetails, Selection: round.selection,
	}, in.Budget.MaxRounds)
	autoBudgetInput, err := coordinatorAutoCumulativeBudgetInput(round.maxRounds, in.DAG, initialSnapshot, budget.Usage())
	if err != nil {
		return *res, err
	}
	if err := budget.configureAutoCumulativeLimitsFromInput(autoBudgetInput); err != nil {
		return *res, err
	}
	round.pendingSnapshot = initialSnapshot

	var lastNoProgressSignature string
	var repeatedNoProgress int
	replanBlocked := func(outcome RoundOutcome) bool {
		if in.Budget.MaxRounds != 0 || roundOutcomeMadeNetProgress(outcome) {
			lastNoProgressSignature = ""
			repeatedNoProgress = 0
			return false
		}
		signature := staleBlockedSignature(outcome)
		if signature == lastNoProgressSignature {
			repeatedNoProgress++
		} else {
			lastNoProgressSignature = signature
			repeatedNoProgress = 1
		}
		return repeatedNoProgress >= 2
	}
	for {
		outcome, err := round.executeFixedPointRound(ctx, in.Mems, res)
		if err != nil {
			err = prioritizeRoundStalePlanError(outcome, err)
			if preflightObservationStale(err) {
				res.Rounds = append(res.Rounds, outcome)
				res.State = ConvergenceStateNonConverged
				return *res, err
			}
			if replanRequired(err) {
				res.Rounds = append(res.Rounds, outcome)
				if replanBlocked(outcome) {
					res.State = ConvergenceStateBlocked
					return *res, &CoordinatorBlockedError{Blocker: outcome.Blocker}
				}
				res.State = ConvergenceStateNonConverged
				continue
			}
			return *res, err
		}
		res.Rounds = append(res.Rounds, outcome)
		snapshot := outcome.Snapshot
		if snapshot == nil {
			return *res, errors.New("TopologyCoordinator.Converge: fixed-point round completed without final snapshot")
		}
		evaluation, err := evaluateCoordinatorSnapshot(
			snapshot, in.DAG, effectiveTargets, parentSafetyTargets, round.desiredMemsByRel(),
			round.desiredDomainUnion(), round.allowedCPUs(),
			in.ExpectedCPUSetByRel, in.DeferredCPUSetByRel,
			round.deferredCleanupRels,
			round.admissionSafetyCPUSet(), snapshotDriver.Capabilities(), allowEmptyTarget,
		)
		if err != nil {
			return *res, err
		}
		res.ConvergenceReport = evaluation.Report
		admissionBudgetExceeded := false
		if in.Objective.orFullDefault() == ConvergenceObjectiveParentSafe && in.AdmissionBudget != nil {
			admissionBudgetExceeded = round.admissionBudgetReached(res)
			if admissionBudgetExceeded && !evaluation.ParentSafety.Safe {
				return *res, fmt.Errorf("admission convergence budget exhausted before parent-safe proof")
			}
		}
		parentSafeDeferred := in.Objective.orFullDefault() == ConvergenceObjectiveParentSafe &&
			evaluation.ParentSafety.Safe && !evaluation.Report.FullyConverged
		if evaluation.Report.FullyConverged || parentSafeDeferred {
			fresh, err := round.nextSnapshot(ctx)
			if err != nil {
				return *res, err
			}
			publishExpected := mergeCPUSetMaps(in.ExpectedCPUSetByRel, in.DeferredCPUSetByRel)
			if !publishRelevantSnapshotsEqual(in.DAG, publishExpected, snapshot, fresh) {
				staleErr := &PlanStaleError{
					Rel: "controlled", Direction: WritePublish, Resource: "final_snapshot",
					Current: snapshotLogicalState(fresh), Target: snapshotLogicalState(snapshot),
					Err: fmt.Errorf("fresh publish-relevant snapshot state differs from convergence snapshot"),
				}
				outcome.Status = RoundStatusStale
				outcome.Snapshot = fresh
				outcome.Blocker = staleErr
				res.Rounds[len(res.Rounds)-1] = outcome
				round.pendingSnapshot = fresh
				if replanBlocked(outcome) {
					res.State = ConvergenceStateBlocked
					return *res, &CoordinatorBlockedError{Blocker: staleErr}
				}
				res.State = ConvergenceStateNonConverged
				continue
			}
			freshEvaluation, err := evaluateCoordinatorSnapshot(
				fresh, in.DAG, effectiveTargets, parentSafetyTargets, round.desiredMemsByRel(),
				round.desiredDomainUnion(), round.allowedCPUs(),
				in.ExpectedCPUSetByRel, in.DeferredCPUSetByRel,
				round.deferredCleanupRels,
				round.admissionSafetyCPUSet(), snapshotDriver.Capabilities(), allowEmptyTarget,
			)
			if err != nil {
				return *res, err
			}
			res.ConvergenceReport = freshEvaluation.Report
			admissionBudgetExceeded = round.admissionBudgetReached(res)
			if admissionBudgetExceeded && !freshEvaluation.ParentSafety.Safe {
				return *res, fmt.Errorf("admission convergence budget exhausted before fresh parent-safe proof")
			}
			parentSafeDeferred = in.Objective.orFullDefault() == ConvergenceObjectiveParentSafe &&
				freshEvaluation.ParentSafety.Safe &&
				!freshEvaluation.Report.FullyConverged
			if !freshEvaluation.Report.FullyConverged && !parentSafeDeferred {
				staleErr := &PlanStaleError{
					Rel: "controlled", Direction: WritePublish, Resource: "fresh_convergence_proof",
					Current: snapshotLogicalState(fresh), Target: snapshotLogicalState(snapshot),
					Err: fmt.Errorf("fresh snapshot no longer satisfies the convergence objective"),
				}
				outcome.Status = RoundStatusStale
				outcome.Snapshot = fresh
				outcome.Blocker = staleErr
				res.Rounds[len(res.Rounds)-1] = outcome
				round.pendingSnapshot = fresh
				if replanBlocked(outcome) {
					res.State = ConvergenceStateBlocked
					return *res, &CoordinatorBlockedError{Blocker: staleErr}
				}
				res.State = ConvergenceStateNonConverged
				continue
			}
			if err := ctx.Err(); err != nil {
				return *res, err
			}
			res.Converged = true
			res.State = ConvergenceStateConverged
			if parentSafeDeferred {
				res.Converged = false
				res.ParentSafe = true
				res.State = ConvergenceStateParentSafeLeafDeferred
				res.DeferredLeafCount = len(in.DeferredCPUSetByRel)
				for _, cpus := range in.DeferredCPUSetByRel {
					res.DeferredCPUCount += cpus.Size()
				}
			}
			outcome.Status = RoundStatusConverged
			outcome.Snapshot = fresh
			res.Rounds[len(res.Rounds)-1] = outcome
			res.FinalSnapshot = fresh
			res.FinalSnapshotCurrent = true
			publish := func() error {
				if parentSafeDeferred && in.PublishParentSafeSnapshot != nil {
					deferredCleanupRels := make(map[string]struct{}, len(round.deferredCleanupRels))
					for rel := range round.deferredCleanupRels {
						deferredCleanupRels[rel] = struct{}{}
					}
					return in.PublishParentSafeSnapshot(fresh, deferredCleanupRels)
				}
				if in.PublishFinalSnapshot != nil {
					return in.PublishFinalSnapshot(fresh)
				}
				return nil
			}
			if err := publish(); err != nil {
				res.FinalSnapshotCurrent = false
				res.FinalSnapshot = nil
				res.Converged = false
				res.ParentSafe = false
				if replanRequired(err) {
					outcome.Status = RoundStatusStale
					outcome.Snapshot = fresh
					outcome.Blocker = err
					res.Rounds[len(res.Rounds)-1] = outcome
					round.pendingSnapshot = fresh
					round.dynamicByRel = cloneCPUSetMap(in.ExpectedCPUSetByRel)
					if replanBlocked(outcome) {
						res.State = ConvergenceStateBlocked
						return *res, &CoordinatorBlockedError{Blocker: err}
					}
					res.State = ConvergenceStateNonConverged
					continue
				}
				return *res, err
			}
			return *res, nil
		}
		res.Converged = false
		if outcome.Status == RoundStatusBlocked {
			var structural *StructuralV1NonEmptyDeadlock
			if errors.As(outcome.Blocker, &structural) {
				res.State = ConvergenceStateBlocked
				return *res, nil
			}
			signature := noWriteBlockedSignature(snapshot, outcome.Witnesses, evaluation.Report)
			if signature == lastNoProgressSignature {
				repeatedNoProgress++
			} else {
				lastNoProgressSignature = signature
				repeatedNoProgress = 1
			}
			if repeatedNoProgress >= 2 {
				res.State = ConvergenceStateBlocked
				return *res, nil
			}
			res.State = ConvergenceStateNonConverged
			continue
		}
		lastNoProgressSignature = ""
		repeatedNoProgress = 0
		res.State = ConvergenceStateNonConverged
		continue
	}
}

func coordinatorAutoCumulativeBudgetInput(
	rounds int,
	dag *TopoDAG,
	snapshot *CompleteSnapshot,
	usage BudgetUsage,
) (AutoCumulativeBudgetInput, error) {
	if dag == nil || snapshot == nil {
		return AutoCumulativeBudgetInput{}, fmt.Errorf("%w: DAG and initial snapshot are required", ErrAutoCumulativeBudgetInvalid)
	}
	snapshotNodes := len(snapshot.Entries)
	if snapshotNodes == 0 {
		snapshotNodes = 1
	}
	snapshotIO := snapshot.Cost.HierarchyIOOperations
	if snapshotIO <= 0 {
		snapshotIO = saturatingAdd(saturatingMultiply(snapshotNodes, 2), 1)
	}
	drainFrontiers, err := checkedAutoBudgetMultiply(rounds, len(dag.index))
	if err != nil {
		return AutoCumulativeBudgetInput{}, err
	}
	growDomains, err := checkedAutoBudgetMultiply(rounds, len(snapshot.DomainUnion))
	if err != nil {
		return AutoCumulativeBudgetInput{}, err
	}
	planOperations, err := coordinatorAutoPlanOperationsTotal(rounds, snapshot)
	if err != nil {
		return AutoCumulativeBudgetInput{}, err
	}
	childMemberships, err := checkedAutoBudgetMultiply(rounds, snapshotChildEdgeCount(snapshot))
	if err != nil {
		return AutoCumulativeBudgetInput{}, err
	}
	return AutoCumulativeBudgetInput{
		CurrentUsedIO:            usage.HierarchyIOOperations,
		RemainingRounds:          rounds,
		SnapshotIOUpperBound:     snapshotIO,
		MaxDrainFrontiersTotal:   drainFrontiers,
		MaxGrowDomainsTotal:      growDomains,
		MaxPlanOperationsTotal:   planOperations,
		MaxChildMembershipsTotal: childMemberships,
		StaleRetryAllowance:      rounds,
	}, nil
}

func coordinatorAutoPlanOperationsTotal(rounds int, snapshot *CompleteSnapshot) (int, error) {
	if rounds <= 0 || snapshot == nil {
		return 0, fmt.Errorf("%w: positive rounds and initial snapshot are required for plan operations",
			ErrAutoCumulativeBudgetInvalid)
	}
	snapshotNodes := len(snapshot.Entries)
	if snapshotNodes == 0 {
		snapshotNodes = 1
	}

	// One drain build charges a projection walk over every entry and child
	// membership, followed by at most one monotonic shrink operation per entry.
	// Expand likewise contributes at most one monotonic grow operation per entry.
	initialPlan, err := checkedAutoBudgetSum(
		snapshotNodes,
		snapshotChildEdgeCount(snapshot),
		snapshotNodes,
	)
	if err != nil {
		return 0, err
	}
	perRound, err := checkedAutoBudgetSum(initialPlan, snapshotNodes)
	if err != nil {
		return 0, err
	}

	// Drain executes one depth frontier at a time. Every completed frontier
	// rebases the plan and charges the number of operations still pending.
	// Derive that cumulative triangular term from the initial hierarchy shape:
	// wide siblings share one frontier, while a deep chain has one per depth.
	depthByRel := buildSnapshotDepthByRel(snapshot, nil)
	frontierWidths := make(map[int]int)
	for rel := range snapshot.Entries {
		frontierWidths[depthByRel[rel]]++
	}
	depths := make([]int, 0, len(frontierWidths))
	for depth := range frontierWidths {
		depths = append(depths, depth)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(depths)))
	remaining := len(snapshot.Entries)
	for _, depth := range depths {
		remaining -= frontierWidths[depth]
		perRound, err = checkedAutoBudgetSum(perRound, remaining)
		if err != nil {
			return 0, err
		}
	}
	return checkedAutoBudgetMultiply(rounds, perRound)
}

func prioritizeRoundStalePlanError(outcome RoundOutcome, err error) error {
	if err == nil || outcome.Blocker == nil || !replanRequired(outcome.Blocker) {
		return err
	}
	if errors.Is(err, ErrHierarchyIOOperationBudgetExceeded) {
		return outcome.Blocker
	}
	return err
}

func includeMaterializedDynamicConvergence(
	report *ConvergenceReport,
	snapshot *CompleteSnapshot,
	expected map[string]machine.CPUSet,
	capabilities HierarchyCapabilities,
) {
	if report == nil || snapshot == nil {
		return
	}
	for rel, target := range expected {
		entry, materialized := snapshot.Entries[rel]
		if !materialized || observedCPUsForTargetProof(entry, target, capabilities).Equals(target) {
			continue
		}
		report.NonConvergedTargets = append(report.NonConvergedTargets, RelConvergence{
			Rel: rel, Observed: observedCPUsForTargetProof(entry, target, capabilities).Clone(), Target: target.Clone(),
			ObservedMems: entry.Mems, Reason: "dynamic_target_mismatch",
		})
	}
	report.FullyConverged = report.FullyConverged && len(report.NonConvergedTargets) == 0
}

func publishRelevantSnapshotsEqual(
	dag *TopoDAG,
	expectedCPUSetByRel map[string]machine.CPUSet,
	converged, fresh *CompleteSnapshot,
) bool {
	if dag == nil || converged == nil || fresh == nil {
		return false
	}
	for rel := range dag.index {
		if !publishRelevantEntriesEqual(converged.Entries, fresh.Entries, rel) {
			return false
		}
	}
	for rel := range expectedCPUSetByRel {
		_, wasMaterialized := converged.Entries[rel]
		_, isMaterialized := fresh.Entries[rel]
		if !wasMaterialized && !isMaterialized {
			continue
		}
		if !publishRelevantEntriesEqual(converged.Entries, fresh.Entries, rel) {
			return false
		}
	}
	return true
}

func publishRelevantEntriesEqual(before, after map[string]EntryState, rel string) bool {
	beforeEntry, beforeExists := before[rel]
	afterEntry, afterExists := after[rel]
	return beforeExists == afterExists &&
		beforeExists &&
		beforeEntry.Identity == afterEntry.Identity &&
		beforeEntry.CPUs.Equals(afterEntry.CPUs) &&
		beforeEntry.Mems == afterEntry.Mems &&
		beforeEntry.ConfiguredCPUs.Equals(afterEntry.ConfiguredCPUs) &&
		beforeEntry.ConfiguredMems == afterEntry.ConfiguredMems
}

func roundOutcomeMadeNetProgress(outcome RoundOutcome) bool {
	measure := outcome.Progress
	if measure.DrainChangedRels == 0 {
		measure.DrainChangedRels = len(outcome.ChangedRels)
	}
	for _, applied := range outcome.Journal {
		if applied.Observed.CPUs.Equals(applied.Target.CPUs) && applied.Observed.Mems == applied.Target.Mems {
			measure.VerifiedWrites++
		}
	}
	return measure.MadeProgress()
}

func staleBlockedSignature(outcome RoundOutcome) string {
	var stale *PlanStaleError
	if errors.As(outcome.Blocker, &stale) {
		return fmt.Sprintf("state=%s;rel=%s;direction=%s;resource=%s;current=%s;target=%s",
			snapshotLogicalState(outcome.Snapshot), stale.Rel, stale.Direction,
			stale.Resource, stale.Current, stale.Target)
	}
	var snapshotErr *SnapshotError
	if errors.As(outcome.Blocker, &snapshotErr) {
		return fmt.Sprintf("generation=%v;rel=%s;direction=snapshot;resource=%s;error=%T",
			snapshotErr.Identity,
			snapshotErr.Rel, snapshotErr.Operation, snapshotErr.Err)
	}
	if errors.Is(outcome.Blocker, ErrCgroupIdentityChanged) {
		return "rel=unknown;direction=unknown;resource=identity"
	}
	return fmt.Sprintf("rel=unknown;direction=unknown;resource=%T", outcome.Blocker)
}

func snapshotRelGeneration(snapshot *CompleteSnapshot, rel string) string {
	if snapshot == nil {
		return "unknown"
	}
	entry, ok := snapshot.Entries[rel]
	if !ok {
		return "missing"
	}
	return fmt.Sprintf("%v", entry.Identity)
}

func snapshotLogicalState(snapshot *CompleteSnapshot) string {
	if snapshot == nil {
		return "unknown"
	}
	rels := make([]string, 0, len(snapshot.Entries))
	for rel := range snapshot.Entries {
		rels = append(rels, rel)
	}
	sort.Strings(rels)
	var b strings.Builder
	for _, rel := range rels {
		entry := snapshot.Entries[rel]
		_, _ = fmt.Fprintf(&b, "%s:%v:%s:%s:%s:%s;",
			rel, entry.Identity, entry.CPUs.String(), entry.Mems,
			entry.ConfiguredCPUs.String(), entry.ConfiguredMems)
	}
	return b.String()
}

func configuredRelMissing(dag *TopoDAG, err error) bool {
	if dag == nil || !isCgroupNotFoundError(err) {
		return false
	}
	var stale *PlanStaleError
	if errors.As(err, &stale) {
		return dag.index[stale.Rel] != nil
	}
	var snapshotErr *SnapshotError
	return errors.As(err, &snapshotErr) && dag.index[snapshotErr.Rel] != nil
}

func noWriteBlockedSignature(snapshot *CompleteSnapshot, witnesses []ReleaseWitness, report ConvergenceReport) string {
	var b strings.Builder
	_, _ = fmt.Fprintf(&b, "state=%s;", snapshotLogicalState(snapshot))
	witnessParts := make([]string, 0, len(witnesses))
	for _, witness := range witnesses {
		witnessParts = append(witnessParts, fmt.Sprintf("%s>%s:%s", witness.Source, witness.Destination, witness.CPUs.String()))
	}
	sort.Strings(witnessParts)
	b.WriteString("witnesses=")
	for _, part := range witnessParts {
		_, _ = fmt.Fprintf(&b, "%s,", part)
	}
	mismatchParts := make([]string, 0, len(report.NonConvergedTargets))
	for _, mismatch := range report.NonConvergedTargets {
		mismatchParts = append(mismatchParts, fmt.Sprintf("%s:%s:%s:%s:%s",
			mismatch.Rel, snapshotRelGeneration(snapshot, mismatch.Rel),
			mismatch.Observed.String(), mismatch.Target.String(), mismatch.Reason))
	}
	sort.Strings(mismatchParts)
	b.WriteString(";mismatch=")
	for _, part := range mismatchParts {
		_, _ = fmt.Fprintf(&b, "%s,", part)
	}
	_, _ = fmt.Fprintf(&b, ";pending=%s/%s/%s/%s",
		report.PendingToPrimary.String(), report.PendingToReclaim.String(),
		report.CleanupPendingPrimary.String(), report.CleanupPendingReclaim.String())
	return b.String()
}

func replanRequired(err error) bool {
	var stale interface{ ReplanRequired() bool }
	return (errors.As(err, &stale) && stale.ReplanRequired()) || errors.Is(err, ErrCgroupIdentityChanged)
}

func preflightObservationStale(err error) bool {
	var stale *PlanStaleError
	return errors.As(err, &stale) && strings.HasPrefix(stale.Resource, "preflight_")
}

func (c TopologyCoordinator) convergeReset(ctx context.Context, in CoordinatorInput, res *ConvergenceResult, budget *BudgetTracker) (ConvergenceResult, error) {
	allowEmptyTarget := in.Cgroup.Version(ctx) == cgroupclient.CgroupVersionV2
	targets := desiredTargets(in.DAG)
	driver, err := snapshotDriverForCoordinator(ctx, in.Cgroup)
	if err != nil {
		return *res, err
	}
	defer driver.Close()
	if in.Budget.MaxHierarchyIOOperations == 0 {
		initialSnapshot, err := newCompleteSnapshotSource(driver, in.DAG, budget, in.TraversalBoundaries)(ctx)
		if err != nil {
			return *res, err
		}
		autoBudgetInput, err := coordinatorAutoCumulativeBudgetInput(1, in.DAG, initialSnapshot, budget.Usage())
		if err != nil {
			return *res, err
		}
		if err := budget.configureAutoCumulativeLimitsFromInput(autoBudgetInput); err != nil {
			return *res, err
		}
	}
	writer := newResetCoordinatorWriter(driver, budget, in.Mems, res)
	if err := writer.execute(ctx, in.DAG, targets, allowEmptyTarget, in.ExpectedCPUSetByRel, in.TraversalBoundaries); err != nil {
		return *res, err
	}
	report, err := verifyResetConvergence(ctx, driver, budget, in.DAG, targets)
	if err != nil {
		return *res, err
	}
	res.ConvergenceReport = report
	res.Converged = report.FullyConverged
	if res.Converged {
		res.State = ConvergenceStateConverged
	} else {
		res.State = ConvergenceStateNonConverged
	}
	return *res, nil
}

func snapshotDriverForCoordinator(ctx context.Context, cg cgroupclient.CgroupClient) (HierarchyDriver, error) {
	if provider, ok := cg.(interface{ SnapshotDriver() HierarchyDriver }); ok {
		driver := provider.SnapshotDriver()
		if driver == nil {
			return nil, errors.New("TopologyCoordinator.Converge: nil snapshot driver")
		}
		return driver, nil
	}
	return newCoordinatorHierarchyDriver(ctx, cg, cgroupRootPathCPUSet())
}

func newCoordinatorHierarchyDriver(
	ctx context.Context,
	cg cgroupclient.CgroupClient,
	rootPath string,
) (HierarchyDriver, error) {
	// CgroupClient only handles version detection and partition, sched-load-balance,
	// and PID attachment above the coordinator; hierarchy snapshots and writes use the pinned-root FD engine.
	if cg.Version(ctx) == cgroupclient.CgroupVersionV2 {
		driver, err := NewCgroupV2Driver(rootPath, nil)
		if err != nil {
			return nil, fmt.Errorf("TopologyCoordinator.Converge: create cgroup v2 snapshot driver: %w", err)
		}
		return driver, nil
	}
	driver, err := NewCgroupV1Driver(rootPath, nil)
	if err != nil {
		return nil, fmt.Errorf("TopologyCoordinator.Converge: create cgroup v1 snapshot driver: %w", err)
	}
	return driver, nil
}

type coordinatorRound struct {
	dag                   *TopoDAG
	targetByRel           map[string]machine.CPUSet
	dynamicByRel          map[string]machine.CPUSet
	deferredByRel         map[string]machine.CPUSet
	deferredCleanupRels   map[string]struct{}
	objective             ConvergenceObjective
	admissionBudget       *AdmissionConvergenceBudget
	allowEmptyTarget      bool
	protectedPending      machine.CPUSet
	protectedByRel        map[string]machine.CPUSet
	requiredIdentityByRel map[string]CgroupIdentity
	expectedAbsentRels    map[string]struct{}
	cpuDetails            machine.CPUDetails
	reservedCPUs          machine.CPUSet
	selection             DrainSelectionPolicy
	snapshotSource        func(context.Context) (*CompleteSnapshot, error)
	driver                HierarchyDriver
	budget                *BudgetTracker
	planID                string
	witnesses             []ReleaseWitness
	blocked               map[DomainID]machine.CPUSet
	pendingSnapshot       *CompleteSnapshot
	round                 int
	maxRounds             int
}

func newCoordinatorRoundWithBudget(
	dag *TopoDAG,
	cg cgroupclient.CgroupClient,
	targetByRel map[string]machine.CPUSet,
	cpuDetails machine.CPUDetails,
	reservedCPUs machine.CPUSet,
	selection DrainSelectionPolicy,
	budget *BudgetTracker,
) *coordinatorRound {
	r := &coordinatorRound{
		dag: dag, targetByRel: cloneCPUSetMap(targetByRel), cpuDetails: cpuDetails,
		reservedCPUs: reservedCPUs.Clone(), budget: budget, maxRounds: budget.limit.MaxRounds,
		selection: NormalizeDrainSelectionPolicy(selection),
		blocked:   make(map[DomainID]machine.CPUSet),
	}
	if provider, ok := cg.(interface{ SnapshotDriver() HierarchyDriver }); ok {
		r.driver = provider.SnapshotDriver()
		r.snapshotSource = newCompleteSnapshotSource(r.driver, dag, budget)
	}
	return r
}

func DefaultDrainSelectionPolicy() DrainSelectionPolicy {
	return DrainSelectionPolicy{
		MaxCPUsDrainRatio:         0,
		GroupByNUMA:               false,
		RequirePairedSwapProgress: true,
	}
}

func NormalizeDrainSelectionPolicy(policy DrainSelectionPolicy) DrainSelectionPolicy {
	defaults := DefaultDrainSelectionPolicy()
	if policy.MaxCPUsDrainRatio == 0 && !policy.GroupByNUMA && !policy.RequirePairedSwapProgress {
		return defaults
	}
	return policy
}

func coordinatorMaxRoundsForPlanInput(in PhasePlanInput, explicitMaxRounds int) int {
	if explicitMaxRounds != 0 {
		return explicitMaxRounds
	}
	required := defaultCoordinatorAutoRounds
	if in.DAG == nil || in.Snapshot == nil {
		return required
	}

	desiredByDomain := desiredDomainUnions(in.DAG, in.DesiredByRel)
	protectedByDomain := protectedCPUSetByDomain(in.ProtectedByRel, in.ProtectedPending, in.DAG)

	batchLimit := maxCPUsPerDrainRound(len(in.CPUDetails), in.Selection.MaxCPUsDrainRatio)
	for _, domain := range sortedDomains(in.Snapshot.DomainUnion, desiredByDomain) {
		// Count every CPU that must leave this domain, not only CPUs paired to
		// another domain's deficit. The latter misses pure cleanup work.
		workload := in.Snapshot.DomainUnion[domain].
			Difference(desiredByDomain[domain]).
			Difference(protectedByDomain[domain])
		transferRounds := 1
		if batchLimit > 0 {
			transferRounds = (workload.Size() + batchLimit - 1) / batchLimit
		}
		if workload.IsEmpty() {
			transferRounds = 0
		}
		candidate := transferRounds + coordinatorPhaseCompletionRounds
		if candidate > required {
			required = candidate
		}
	}
	return required
}

func (r *coordinatorRound) allowedCPUs() machine.CPUSet {
	if len(r.cpuDetails) == 0 {
		return machine.NewCPUSet()
	}
	return r.cpuDetails.CPUs().Difference(r.reservedCPUs)
}

func (r *coordinatorRound) desiredDomainUnion() map[DomainID]machine.CPUSet {
	return desiredDomainUnions(r.dag, r.targetByRel)
}

func (r *coordinatorRound) desiredMemsByRel() map[string]string {
	out := make(map[string]string, len(r.dag.index))
	for rel, node := range r.dag.index {
		out[rel] = node.Mems
	}
	return out
}

func (r *coordinatorRound) nextSnapshot(ctx context.Context) (*CompleteSnapshot, error) {
	if r.pendingSnapshot != nil {
		snapshot := r.pendingSnapshot
		r.pendingSnapshot = nil
		return snapshot, r.validatePreflightObservations(ctx, snapshot)
	}
	if r.snapshotSource == nil {
		return nil, fmt.Errorf("topology coordinator requires complete snapshot source")
	}
	if r.budget == nil {
		return nil, fmt.Errorf("topology coordinator requires convergence budget")
	}
	var lastMissingSignature string
	var repeatedMissing int
	for {
		snapshot, err := r.snapshotSource(ctx)
		if err == nil {
			return snapshot, r.validatePreflightObservations(ctx, snapshot)
		}
		var snapshotErr *SnapshotError
		if !errors.As(err, &snapshotErr) || snapshotErr.Class != HierarchyErrorStale {
			return nil, err
		}
		if !configuredRelMissing(r.dag, err) {
			lastMissingSignature = ""
			repeatedMissing = 0
			continue
		}
		// Evidence IDs describe individual scans. Identity binds repeated missing
		// observations to the same cgroup generation without merging replacements.
		signature := fmt.Sprintf("identity=%v;rel=%s;operation=%s",
			snapshotErr.Identity, snapshotErr.Rel, snapshotErr.Operation)
		if signature == lastMissingSignature {
			repeatedMissing++
		} else {
			lastMissingSignature = signature
			repeatedMissing = 1
		}
		if repeatedMissing >= 2 {
			return nil, err
		}
	}
}

func (r *coordinatorRound) validatePreflightObservations(ctx context.Context, snapshot *CompleteSnapshot) error {
	for rel, required := range r.requiredIdentityByRel {
		entry, ok := snapshot.Entries[rel]
		if !ok || entry.Identity != required {
			current := "missing"
			if ok {
				current = fmt.Sprintf("%v", entry.Identity)
			}
			return &PlanStaleError{
				Rel:       rel,
				Direction: WritePublish,
				Resource:  "preflight_identity",
				Current:   current,
				Target:    fmt.Sprintf("%v", required),
			}
		}
	}
	driver := r.driver
	if wrapped, ok := driver.(*budgetedHierarchyDriver); !ok || wrapped.budget != r.budget {
		driver = NewBudgetedHierarchyDriver(driver, r.budget)
	}
	for rel := range r.expectedAbsentRels {
		identity, err := driver.StatIdentity(ctx, rel)
		if err == nil {
			return &PlanStaleError{
				Rel:       rel,
				Direction: WritePublish,
				Resource:  "preflight_absence",
				Current:   fmt.Sprintf("%v", identity),
				Target:    "absent",
			}
		}
		if !isCgroupNotFoundError(err) {
			return fmt.Errorf("validate expected absent rel %q: %w", rel, err)
		}
	}
	return nil
}

func cloneIdentityMap(in map[string]CgroupIdentity) map[string]CgroupIdentity {
	if in == nil {
		return nil
	}
	out := make(map[string]CgroupIdentity, len(in))
	for rel, identity := range in {
		out[rel] = identity
	}
	return out
}

func cloneRelSet(in map[string]struct{}) map[string]struct{} {
	if in == nil {
		return nil
	}
	out := make(map[string]struct{}, len(in))
	for rel := range in {
		out[rel] = struct{}{}
	}
	return out
}

func (r *coordinatorRound) buildPlan(ctx context.Context, kind PhaseKind, snapshot *CompleteSnapshot) (PhasePlan, error) {
	plan, err := BuildPhasePlan(PhasePlanInput{
		Context: ctx, Kind: kind, DAG: r.dag, Snapshot: snapshot,
		DesiredByRel: r.targetByRel, DynamicByRel: r.dynamicByRel, DesiredMemsByRel: r.desiredMemsByRel(),
		AllowedCPUs: r.allowedCPUs(), AllowEmptyTarget: r.allowEmptyTarget,
		Capabilities: r.driver.Capabilities(),
		Witnesses:    r.witnesses, ProtectedPending: r.protectedPending,
		ProtectedByRel: r.protectedByRel, CPUDetails: r.cpuDetails,
		Selection: r.selection, Budget: r.budget,
	})
	if err == nil && r.objective == ConvergenceObjectiveParentSafe {
		required, deferred, splitErr := SplitPlanForAdmission(&plan, AdmissionSafetyInput{
			ProtectedPendingCPUSet: r.admissionSafetyCPUSet(),
			DeferredCPUSetByRel:    r.deferredByRel,
		})
		if splitErr != nil {
			return PhasePlan{}, splitErr
		}
		for _, operation := range deferred.Operations {
			r.deferredCleanupRels[operation.Rel] = struct{}{}
		}
		plan = *required
	}
	if err == nil {
		r.planID = plan.PlanID
	}
	return plan, err
}

func (r *coordinatorRound) admissionSafetyCPUSet() machine.CPUSet {
	return r.protectedPending.Clone()
}

func (r *coordinatorRound) executePlan(ctx context.Context, plan PhasePlan, res *ConvergenceResult) error {
	if plan.PlanID == "" || plan.PlanID != r.planID || canonicalExecutionPlanID(plan) != plan.PlanID {
		return fmt.Errorf("phase writer requires current canonical PlanID")
	}
	if r.driver == nil {
		return fmt.Errorf("phase writer requires hierarchy driver")
	}
	if r.budget == nil {
		return fmt.Errorf("phase writer requires convergence budget")
	}
	if err := r.checkAdmissionExecutionBudget(plan, res); err != nil {
		return err
	}
	if err := r.revalidateGrowAuthorization(ctx, plan); err != nil {
		return err
	}
	return newSafeCPUSetWriter(r.driver, r.budget, res).execute(ctx, plan)
}

func (r *coordinatorRound) admissionBudgetReached(res *ConvergenceResult) bool {
	if r == nil || r.objective != ConvergenceObjectiveParentSafe || r.admissionBudget == nil {
		return false
	}
	if r.admissionBudget.MaxRequiredWrites > 0 && res != nil &&
		res.Applied >= r.admissionBudget.MaxRequiredWrites {
		return true
	}
	return false
}

func (r *coordinatorRound) checkAdmissionExecutionBudget(plan PhasePlan, res *ConvergenceResult) error {
	if r == nil || r.objective != ConvergenceObjectiveParentSafe ||
		r.admissionBudget == nil || len(plan.Operations) == 0 {
		return nil
	}
	applied := 0
	if res != nil {
		applied = res.Applied
	}
	if limit := r.admissionBudget.MaxRequiredWrites; limit > 0 &&
		applied+len(plan.Operations) > limit {
		return fmt.Errorf("admission convergence write budget exhausted before safety closure: limit=%d applied=%d required=%d",
			limit, applied, len(plan.Operations))
	}
	return nil
}

func (r *coordinatorRound) revalidateGrowAuthorization(ctx context.Context, plan PhasePlan) error {
	operationsByDomain := make(map[DomainID][]PlanOperation)
	for _, operation := range plan.Operations {
		if operation.Direction != WriteGrow {
			continue
		}
		domain := plan.Base.DomainByRel[operation.Rel]
		if domain == "" {
			if node := r.dag.index[operation.Rel]; node != nil {
				domain = node.Domain
			}
		}
		operationsByDomain[domain] = append(operationsByDomain[domain], operation)
	}
	if len(operationsByDomain) == 0 {
		return nil
	}
	for _, domain := range sortedOperationDomains(operationsByDomain) {
		fresh, err := r.nextSnapshot(ctx)
		if err != nil {
			return err
		}
		staleWithFresh := func(stale *PlanStaleError) error {
			r.pendingSnapshot = fresh
			return stale
		}
		gate, err := NewDomainGate(plan.ConvergenceID, fresh, r.desiredDomainUnion(), r.allowedCPUs(), plan.Witnesses)
		if err != nil {
			return err
		}
		priorGrowTargetByRel := make(map[string]machine.CPUSet, len(operationsByDomain[domain]))
		for _, operation := range operationsByDomain[domain] {
			current, ok := fresh.Entries[operation.Rel]
			if !ok {
				return staleWithFresh(&PlanStaleError{
					Rel: operation.Rel, Direction: operation.Direction, Resource: "authorization",
					Current: "missing", Target: operation.Target.CPUs.String(),
				})
			}
			delta := operation.Target.CPUs.Difference(current.CPUs)
			authorized := fresh.DomainUnion[domain].Union(gate.AllowedEntering(domain))
			if !delta.IsSubsetOf(authorized) {
				return staleWithFresh(&PlanStaleError{
					Rel: operation.Rel, Direction: operation.Direction, Resource: "authorization",
					Current: current.CPUs.String(), Target: operation.Target.CPUs.String(),
					Err: fmt.Errorf("fresh grow delta %s is not authorized for domain %q", delta.String(), domain),
				})
			}
			if operation.ParentRel != "" {
				parent, ok := fresh.Entries[operation.ParentRel]
				if !ok {
					return staleWithFresh(&PlanStaleError{
						Rel: operation.Rel, Direction: operation.Direction, Resource: "parent_cpuset",
						Current: "missing", Target: operation.Target.CPUs.String(),
					})
				}
				parentCovers := operation.Target.CPUs.IsSubsetOf(parent.CPUs)
				if plannedParent, planned := priorGrowTargetByRel[operation.ParentRel]; planned {
					parentCovers = parentCovers || operation.Target.CPUs.IsSubsetOf(plannedParent)
				}
				if !parentCovers {
					return staleWithFresh(&PlanStaleError{
						Rel: operation.Rel, Direction: operation.Direction, Resource: "parent_cpuset",
						Current: parent.CPUs.String(), Target: operation.Target.CPUs.String(),
						Err: fmt.Errorf("fresh parent %q cpuset does not cover grow target", operation.ParentRel),
					})
				}
			}
			priorGrowTargetByRel[operation.Rel] = operation.Target.CPUs.Clone()
		}
	}
	return nil
}

func sortedOperationDomains(operations map[DomainID][]PlanOperation) []DomainID {
	domains := make([]DomainID, 0, len(operations))
	for domain := range operations {
		domains = append(domains, domain)
	}
	sort.Slice(domains, func(i, j int) bool { return domains[i] < domains[j] })
	return domains
}

func (r *coordinatorRound) executeFixedPointRound(ctx context.Context, defaultMems string, res *ConvergenceResult) (RoundOutcome, error) {
	if r.round >= r.maxRounds {
		return RoundOutcome{}, fmt.Errorf("%w: limit=%d used=%d", ErrRoundBudgetExceeded, r.maxRounds, r.round)
	}
	if err := r.budget.ConsumeRound(); err != nil {
		return RoundOutcome{}, err
	}
	r.round++
	r.deferredCleanupRels = make(map[string]struct{})
	journalBefore := len(res.Journal)
	var progressBase, progressSnapshot *CompleteSnapshot
	var changedRels []string
	staleOutcome := func(err error) RoundOutcome {
		return RoundOutcome{
			Status:      RoundStatusStale,
			Snapshot:    progressSnapshot,
			Blocker:     err,
			Journal:     append([]AppliedPlanOperation(nil), res.Journal[journalBefore:]...),
			ChangedRels: append([]string(nil), changedRels...),
			Cost:        r.budget.Usage(),
		}
	}
	appliedBefore := res.Applied
	drainSnapshot, err := r.nextSnapshot(ctx)
	if err != nil {
		return staleOutcome(err), err
	}
	drain, err := r.buildPlan(ctx, PhaseDrain, drainSnapshot)
	if err != nil {
		var structural *StructuralV1NonEmptyDeadlock
		if errors.As(err, &structural) {
			return RoundOutcome{
				Status:   RoundStatusBlocked,
				Snapshot: drainSnapshot,
				Blocker:  err,
				Cost:     r.budget.Usage(),
			}, nil
		}
		return staleOutcome(err), err
	}
	progressBase = drain.Base
	fresh, released, err := r.executeDrainBatches(ctx, drain, res)
	if fresh != nil {
		progressSnapshot = fresh
		changedRels = verifiedDrainProgressRels(progressBase, fresh, drain.TargetByRel)
	}
	if err != nil {
		return staleOutcome(err), err
	}
	r.witnesses = r.witnesses[:0]
	for source, destinations := range released {
		for destination, cpus := range destinations {
			if cpus.IsEmpty() {
				continue
			}
			witness := NewReleaseWitness(drain.ConvergenceID, source, destination, cpus, fresh)
			if witness.CPUs.IsEmpty() {
				continue
			}
			r.witnesses = append(r.witnesses, witness)
		}
	}
	expand, err := r.buildPlan(ctx, PhaseExpand, fresh)
	if err != nil {
		return staleOutcome(err), err
	}
	if err := r.executePlan(ctx, expand, res); err != nil {
		return staleOutcome(err), err
	}
	final, err := r.nextSnapshot(ctx)
	if err != nil {
		return staleOutcome(err), err
	}
	r.recomputeBlocked(final)
	status := RoundStatusProgress
	if res.Applied == appliedBefore {
		status = RoundStatusBlocked
	}
	journal := append([]AppliedPlanOperation(nil), res.Journal[journalBefore:]...)
	return RoundOutcome{
		Status:      status,
		Snapshot:    final,
		Witnesses:   append([]ReleaseWitness(nil), r.witnesses...),
		Journal:     journal,
		ChangedRels: append([]string(nil), changedRels...),
		Progress: ProgressMeasure{
			DrainChangedRels: len(changedRels),
			VerifiedWrites:   len(journal),
		},
		Cost: r.budget.Usage(),
	}, nil
}

func (r *coordinatorRound) executeDrainBatches(
	ctx context.Context,
	plan PhasePlan,
	res *ConvergenceResult,
) (*CompleteSnapshot, map[DomainID]map[DomainID]machine.CPUSet, error) {
	fresh := plan.Base
	released := make(map[DomainID]map[DomainID]machine.CPUSet)
	if len(plan.Operations) == 0 {
		next, err := r.nextSnapshot(ctx)
		return next, released, err
	}
	for len(plan.Operations) > 0 {
		batch, err := drainFrontier(plan)
		if err != nil {
			return fresh, released, err
		}
		r.planID = batch.PlanID
		accumulateDrainTransfers(released, plan.TransferGraph, plan.DrainBatch)
		if err := r.executePlan(ctx, batch, res); err != nil {
			if recovered, snapshotErr := r.nextSnapshot(ctx); snapshotErr == nil {
				fresh = recovered
			}
			return fresh, released, err
		}
		next, err := r.nextSnapshot(ctx)
		if err != nil {
			return fresh, released, err
		}
		fresh = next
		plan, err = rebaseDrainPlan(plan, fresh, r.dag, r.budget)
		if err != nil {
			return fresh, released, err
		}
		if r.objective == ConvergenceObjectiveParentSafe {
			required, _, splitErr := SplitPlanForAdmission(&plan, AdmissionSafetyInput{
				ProtectedPendingCPUSet: r.admissionSafetyCPUSet(),
				DeferredCPUSetByRel:    r.deferredByRel,
			})
			if splitErr != nil {
				return fresh, released, splitErr
			}
			plan = *required
		}
	}
	return fresh, released, nil
}

func drainFrontier(plan PhasePlan) (PhasePlan, error) {
	if len(plan.Operations) == 0 {
		return plan, nil
	}
	remaining := plan.CostUpperBound.Operations
	if remaining <= 0 || len(plan.Operations) > remaining {
		return PhasePlan{}, fmt.Errorf("%w: drain frontier limit=%d operations=%d",
			ErrPlanOperationBudgetExceeded, remaining, len(plan.Operations))
	}
	depthByRel := buildSnapshotDepthByRel(plan.Base, nil)
	first := plan.Operations[0]
	depth := depthByRel[first.Rel]
	end := 1
	remaining--
	for end < len(plan.Operations) {
		if remaining == 0 {
			return PhasePlan{}, fmt.Errorf("%w: drain frontier exhausted after=%d", ErrPlanOperationBudgetExceeded, end)
		}
		remaining--
		operation := plan.Operations[end]
		if operation.Direction != first.Direction || depthByRel[operation.Rel] != depth {
			break
		}
		end++
	}
	plan.Operations = append([]PlanOperation(nil), plan.Operations[:end]...)
	plan.CostUpperBound.Operations = len(plan.Operations)
	plan.PlanID = canonicalExecutionPlanID(plan)
	for i := range plan.Operations {
		plan.Operations[i].PlanID = plan.PlanID
	}
	return plan, nil
}

func rebaseDrainPlan(plan PhasePlan, fresh *CompleteSnapshot, dag *TopoDAG, budget *BudgetTracker) (PhasePlan, error) {
	if fresh == nil {
		return PhasePlan{}, errors.New("cannot rebase drain plan without fresh snapshot")
	}
	if budget == nil {
		return PhasePlan{}, errors.New("cannot rebase drain plan without convergence budget")
	}
	targets := make(map[string]CPUSetTarget, len(fresh.Entries))
	for rel, entry := range fresh.Entries {
		target, ok := plan.TargetByRel[rel]
		if !ok {
			target = CPUSetTarget{CPUs: entry.CPUs.Clone(), Mems: entry.Mems}
		}
		targets[rel] = target
	}
	for rel := range plan.TargetByRel {
		if _, ok := fresh.Entries[rel]; !ok {
			return PhasePlan{}, &PlanStaleError{
				Rel: rel, Direction: WriteShrink, Resource: "snapshot",
				Current: "missing", Target: plan.TargetByRel[rel].CPUs.String(),
			}
		}
	}
	depthByRel := buildSnapshotDepthByRel(fresh, nil)
	domainByRel, parentByRel := buildPlannerRelations(fresh, dag, depthByRel, nil)
	postProcessPhaseOperationTargets(plan.Kind, plan.AllowEmptyTarget, plan.Capabilities, targets, fresh)
	if err := propagatePhaseTargetEnvelope(targets, parentByRel, depthByRel); err != nil {
		return PhasePlan{}, err
	}
	if !plan.AllowEmptyTarget {
		if err := propagateControlledPhaseTargetEnvelope(targets, dag); err != nil {
			return PhasePlan{}, err
		}
		clampReclaimNUMABucketPhaseMems(targets, dag, nil)
	}
	operationCount, err := countPlanOperations(PhaseDrain, plan.Capabilities, targets, fresh, dag, nil)
	if err != nil {
		return PhasePlan{}, err
	}
	if err := budget.ConsumePlanOperations(operationCount); err != nil {
		return PhasePlan{}, err
	}
	plan.Base = fresh
	plan.TargetByRel = targets
	plan.Operations = buildPlanOperations(
		plan.Kind, plan.AllowEmptyTarget, plan.Capabilities,
		targets, fresh, depthByRel, domainByRel, parentByRel, dag, operationCount, nil,
	)
	plan.CostUpperBound.Operations = len(plan.Operations)
	plan.PlanID = canonicalExecutionPlanID(plan)
	for i := range plan.Operations {
		plan.Operations[i].PlanID = plan.PlanID
	}
	return plan, nil
}

func accumulateDrainTransfers(
	released map[DomainID]map[DomainID]machine.CPUSet,
	graph map[DomainID]map[DomainID]machine.CPUSet,
	batch map[DomainID]machine.CPUSet,
) {
	for source, destinations := range graph {
		for destination, cpus := range destinations {
			drained := cpus.Intersection(batch[source])
			if drained.IsEmpty() {
				continue
			}
			if released[source] == nil {
				released[source] = make(map[DomainID]machine.CPUSet)
			}
			released[source][destination] = released[source][destination].Union(drained)
		}
	}
}

func verifiedDrainProgressRels(
	before, after *CompleteSnapshot,
	targets map[string]CPUSetTarget,
) []string {
	if before == nil || after == nil {
		return nil
	}
	changed := make([]string, 0)
	for rel, previous := range before.Entries {
		current, ok := after.Entries[rel]
		target, targeted := targets[rel]
		if !ok || !targeted {
			continue
		}
		cpusProgressed := target.CPUs.IsSubsetOf(current.CPUs) &&
			current.CPUs.IsSubsetOf(previous.CPUs) &&
			!current.CPUs.Equals(previous.CPUs)
		memsProgressed := current.Mems == target.Mems && previous.Mems != target.Mems
		if cpusProgressed || memsProgressed {
			changed = append(changed, rel)
		}
	}
	sort.Strings(changed)
	return changed
}

func (r *coordinatorRound) recomputeBlocked(snapshot *CompleteSnapshot) {
	r.blocked = make(map[DomainID]machine.CPUSet)
	desired := r.desiredDomainUnion()
	for destination, target := range desired {
		for source, observed := range snapshot.DomainUnion {
			if source != destination {
				r.blocked[destination] = r.blocked[destination].Union(observed.Intersection(target))
			}
		}
	}
}

func cgroupRootPathCPUSet() string {
	return cgcommon.GetCgroupRootPath(cgcommon.CgroupSubsysCPUSet)
}
