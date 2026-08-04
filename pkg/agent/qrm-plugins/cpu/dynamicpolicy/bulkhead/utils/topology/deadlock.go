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

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

const defaultDeadlockProbeBudget = 4096

type ProbeCompleteness string

const (
	ProbeComplete      ProbeCompleteness = "complete"
	ProbeIndeterminate ProbeCompleteness = "indeterminate"
)

type DrainAtom struct {
	Source      DomainID
	Destination DomainID
	CPUs        machine.CPUSet
}

type DrainAtomClass string

const (
	DrainAtomClassV1Empty    DrainAtomClass = "v1_empty"
	DrainAtomClassProtected  DrainAtomClass = "protected"
	DrainAtomClassReleasable DrainAtomClass = "releasable"
	DrainAtomClassHeld       DrainAtomClass = "held"
)

type DeadlockAnalysis struct {
	Completeness   ProbeCompleteness
	Atoms          []DrainAtom
	AtomClasses    []DrainAtomClass
	SafeSeed       *DrainAtom
	SafeGrowAnchor machine.CPUSet
	EmptyBlockers  map[string]machine.CPUSet
	Protected      machine.CPUSet
	NonFinalSpare  machine.CPUSet
	ProbeStats     DeadlockProbeStats
}

type DeadlockProbeStats struct {
	Atoms                      int
	AtomIndex                  int
	AtomSource                 DomainID
	AtomDestination            DomainID
	SnapshotEntries            int
	SnapshotChildEdges         int
	ProtectedRels              int
	ProtectedPendingCPUs       int
	ProbeOperations            int
	ProbeLimit                 int
	ContextOperations          int
	ContextPhase               string
	BaseOperations             int
	ProtectedOperations        int
	RelIndexOperations         int
	ChildIndexOps              int
	ChildMembershipsScanned    int
	FrontierIndexOps           int
	FrontierMembershipsScanned int
	AncestorClosureOps         int
	AtomOperations             int
	SnapshotID                 SnapshotID
}

type StructuralV1NonEmptyDeadlock struct {
	Analysis DeadlockAnalysis
}

func (e *StructuralV1NonEmptyDeadlock) Error() string {
	return fmt.Sprintf("structural cgroup v1 non-empty deadlock: atoms=%d blockers=%d",
		len(e.Analysis.Atoms), len(e.Analysis.EmptyBlockers))
}

func analyzeV1Deadlock(in PhasePlanInput) (DeadlockAnalysis, error) {
	analysis := DeadlockAnalysis{
		Completeness:  ProbeComplete,
		EmptyBlockers: make(map[string]machine.CPUSet),
		ProbeStats: DeadlockProbeStats{
			AtomIndex: -1,
		},
	}
	if in.AllowEmptyTarget || in.Kind != PhaseDrain || in.DAG == nil || in.Snapshot == nil {
		return analysis, nil
	}
	analysis.ProbeStats.SnapshotEntries = len(in.Snapshot.Entries)
	analysis.ProbeStats.SnapshotChildEdges = snapshotChildEdgeCount(in.Snapshot)
	analysis.ProbeStats.ProtectedRels = len(in.ProtectedByRel)
	analysis.ProbeStats.ProtectedPendingCPUs = in.ProtectedPending.Size()
	analysis.ProbeStats.SnapshotID = in.Snapshot.ID
	desiredByDomain := desiredDomainUnions(in.DAG, in.DesiredByRel)
	domains := sortedDomains(in.Snapshot.DomainUnion, desiredByDomain)
	graph := buildTransferGraph(domains, in.Snapshot.DomainUnion, desiredByDomain, nil)
	if !isNarrowPrimaryReclaimCycle(graph) {
		return analysis, nil
	}
	analysis.SafeGrowAnchor = verifiedUnownedFinalCPUs(
		in.AllowedCPUs, in.Snapshot.DomainUnion, desiredByDomain,
	)
	if !analysis.SafeGrowAnchor.IsEmpty() {
		return analysis, nil
	}
	for _, source := range []DomainID{DomainPrimary, DomainReclaim} {
		destinations := make([]DomainID, 0, len(graph[source]))
		for destination := range graph[source] {
			destinations = append(destinations, destination)
		}
		sort.Slice(destinations, func(i, j int) bool { return destinations[i] < destinations[j] })
		for _, destination := range destinations {
			for _, cpu := range graph[source][destination].ToSliceInt() {
				analysis.Atoms = append(analysis.Atoms, DrainAtom{
					Source: source, Destination: destination, CPUs: machine.NewCPUSet(cpu),
				})
			}
		}
	}
	analysis.ProbeStats.Atoms = len(analysis.Atoms)
	if in.Budget != nil {
		in.Budget.EnsureDeadlockProbeCapacity(deadlockProbeCapacityUpperBound(
			analysis.ProbeStats.SnapshotEntries,
			analysis.ProbeStats.SnapshotChildEdges,
			analysis.ProbeStats.Atoms,
		))
		analysis.ProbeStats.ProbeLimit = in.Budget.DeadlockProbeLimit()
	}
	ctx := in.Context
	if ctx == nil {
		ctx = context.Background()
	}
	limit := in.DeadlockProbeBudget
	if limit == 0 && in.Budget == nil {
		limit = defaultDeadlockProbeBudget
	}
	probeCost := 0
	protectedByDomain := protectedCPUSetByDomain(in.ProtectedByRel, in.ProtectedPending, in.DAG)
	depthByRel := buildSnapshotDepthByRel(in.Snapshot, nil)
	domainByRel, parentByRel := buildPlannerRelations(in.Snapshot, in.DAG, depthByRel, nil)
	projectionContext, err := buildDrainProjectionContext(ctx, in.Snapshot, in.ProtectedByRel, in.Budget)
	if err != nil {
		if errors.Is(err, ErrDeadlockProbeBudgetExceeded) {
			analysis.Completeness = ProbeIndeterminate
			if projectionContext != nil {
				copyProjectionContextStats(&analysis.ProbeStats, projectionContext)
			}
			analysis.ProbeStats.ProbeOperations = deadlockProbeOperations(in.Budget, probeCost)
			return analysis, wrapDeadlockProbeError(err, analysis.ProbeStats)
		}
		return DeadlockAnalysis{}, err
	}
	probeCost = projectionContext.cost
	copyProjectionContextStats(&analysis.ProbeStats, projectionContext)
	analysis.ProbeStats.ProbeOperations = deadlockProbeOperations(in.Budget, probeCost)
	leavingByDomain := make(map[DomainID]machine.CPUSet, len(domains))
	for _, domain := range domains {
		leavingByDomain[domain] = in.Snapshot.DomainUnion[domain].Difference(desiredByDomain[domain])
	}
	transferCPUs := machine.NewCPUSet()
	for _, atom := range analysis.Atoms {
		transferCPUs = transferCPUs.Union(atom.CPUs)
	}
	projectionInput := DrainProjectionInput{
		PlanInput: in, LeavingByDomain: leavingByDomain,
		DomainByRel: domainByRel, ParentByRel: parentByRel, DepthByRel: depthByRel,
		Context: projectionContext, TransferCPUs: transferCPUs,
		ProbeContext: ctx, ProbeBudget: in.Budget,
	}
	if err := prepareIncrementalDrainProjectionContext(projectionInput, projectionContext); err != nil {
		if errors.Is(err, ErrDeadlockProbeBudgetExceeded) {
			analysis.Completeness = ProbeIndeterminate
			copyProjectionContextStats(&analysis.ProbeStats, projectionContext)
			analysis.ProbeStats.ProbeOperations = deadlockProbeOperations(in.Budget, projectionContext.cost)
			return analysis, wrapDeadlockProbeError(err, analysis.ProbeStats)
		}
		return DeadlockAnalysis{}, err
	}
	probeCost = projectionContext.cost
	copyProjectionContextStats(&analysis.ProbeStats, projectionContext)
	analysis.ProbeStats.ProbeOperations = deadlockProbeOperations(in.Budget, probeCost)
	for i := range analysis.Atoms {
		atom := analysis.Atoms[i]
		analysis.ProbeStats.AtomIndex = i
		analysis.ProbeStats.AtomSource = atom.Source
		analysis.ProbeStats.AtomDestination = atom.Destination
		analysis.ProbeStats.ProbeOperations = deadlockProbeOperations(in.Budget, probeCost)
		if limit > 0 && probeCost >= limit {
			analysis.Completeness = ProbeIndeterminate
			err := fmt.Errorf("%w: limit=%d used=%d",
				ErrDeadlockProbeBudgetExceeded, limit, probeCost)
			return analysis, wrapDeadlockProbeError(err, analysis.ProbeStats)
		}
		if !atom.CPUs.Intersection(protectedByDomain[atom.Source]).IsEmpty() {
			analysis.Protected = analysis.Protected.Union(atom.CPUs)
			analysis.AtomClasses = append(analysis.AtomClasses, DrainAtomClassProtected)
			continue
		}
		drainBatch := map[DomainID]machine.CPUSet{
			DomainPrimary: machine.NewCPUSet(),
			DomainReclaim: machine.NewCPUSet(),
			atom.Source:   atom.CPUs,
		}
		atomInput := projectionInput
		atomInput.DrainBatch = drainBatch
		projection, err := projectSingleAtomIncremental(atomInput, atom)
		if err != nil {
			if errors.Is(err, ErrDeadlockProbeBudgetExceeded) {
				analysis.Completeness = ProbeIndeterminate
				analysis.ProbeStats.ProbeOperations = deadlockProbeOperations(in.Budget, probeCost)
				return analysis, wrapDeadlockProbeError(err, analysis.ProbeStats)
			}
			return DeadlockAnalysis{}, err
		}
		projectionCost := projection.Cost.Total()
		if limit > 0 && projectionCost > limit-probeCost {
			analysis.Completeness = ProbeIndeterminate
			err := fmt.Errorf("%w: limit=%d used=%d requested=%d",
				ErrDeadlockProbeBudgetExceeded, limit, probeCost, projectionCost)
			analysis.ProbeStats.ProbeOperations = deadlockProbeOperations(in.Budget, probeCost)
			return analysis, wrapDeadlockProbeError(err, analysis.ProbeStats)
		}
		probeCost += projectionCost
		analysis.ProbeStats.AtomOperations += projectionCost
		analysis.ProbeStats.ProbeOperations = deadlockProbeOperations(in.Budget, probeCost)
		for rel, cpus := range projection.EmptyBlockers {
			analysis.EmptyBlockers[rel] = analysis.EmptyBlockers[rel].Union(cpus)
		}
		if !projection.DomainUnion[atom.Source].Contains(atom.CPUs.ToSliceInt()[0]) {
			analysis.AtomClasses = append(analysis.AtomClasses, DrainAtomClassReleasable)
			seed := atom
			analysis.SafeSeed = &seed
			return analysis, nil
		}
		emptyBlocked := machine.NewCPUSet()
		for _, cpus := range projection.EmptyBlockers {
			emptyBlocked = emptyBlocked.Union(cpus)
		}
		if atom.CPUs.IsSubsetOf(emptyBlocked) {
			analysis.AtomClasses = append(analysis.AtomClasses, DrainAtomClassV1Empty)
		} else {
			analysis.AtomClasses = append(analysis.AtomClasses, DrainAtomClassHeld)
		}
	}
	desiredAll := desiredByDomain[DomainPrimary].Union(desiredByDomain[DomainReclaim])
	analysis.NonFinalSpare = in.AllowedCPUs.Difference(desiredAll)
	return analysis, nil
}

func snapshotChildEdgeCount(snapshot *CompleteSnapshot) int {
	if snapshot == nil {
		return 0
	}
	count := 0
	for _, children := range snapshot.Children {
		count += len(children)
	}
	return count
}

func deadlockProbeOperations(budget *BudgetTracker, fallback int) int {
	if budget == nil {
		return fallback
	}
	return budget.Usage().DeadlockProbeOperations
}

func deadlockProbeCapacityUpperBound(entries, childEdges, atoms int) int {
	snapshotWork := saturatingAdd(entries, childEdges)
	contextWork := saturatingMultiply(snapshotWork, 3)
	perAtomWork := saturatingAdd(snapshotWork, 1)
	return saturatingAdd(
		defaultDeadlockProbeBudget,
		saturatingAdd(contextWork, saturatingMultiply(atoms, perAtomWork)),
	)
}

func copyProjectionContextStats(stats *DeadlockProbeStats, projectionContext *drainProjectionContext) {
	if stats == nil || projectionContext == nil {
		return
	}
	stats.ContextOperations = projectionContext.cost
	stats.ContextPhase = projectionContext.phase
	stats.BaseOperations = projectionContext.baseCost
	stats.ProtectedOperations = projectionContext.protectedOperations
	stats.RelIndexOperations = projectionContext.relIndexOperations
	stats.ChildIndexOps = projectionContext.childIndexOperations
	stats.ChildMembershipsScanned = projectionContext.childMembershipsScanned
	stats.FrontierIndexOps = projectionContext.frontierIndexOperations
	stats.FrontierMembershipsScanned = projectionContext.frontierMembershipsScanned
	stats.AncestorClosureOps = projectionContext.ancestorClosureOperations
}

func wrapDeadlockProbeError(err error, stats DeadlockProbeStats) error {
	return fmt.Errorf(
		"%w: atoms=%d atom_index=%d atom_source=%s atom_destination=%s "+
			"snapshot_entries=%d snapshot_child_edges=%d protected_rels=%d "+
			"protected_pending_cpus=%d probe_operations=%d probe_limit=%d context_operations=%d "+
			"context_phase=%s base_operations=%d protected_operations=%d "+
			"rel_index_operations=%d child_index_operations=%d "+
			"child_memberships_scanned=%d "+
			"frontier_index_operations=%d frontier_memberships_scanned=%d "+
			"ancestor_closure_operations=%d "+
			"atom_operations=%d snapshot_id=%x",
		err,
		stats.Atoms,
		stats.AtomIndex,
		stats.AtomSource,
		stats.AtomDestination,
		stats.SnapshotEntries,
		stats.SnapshotChildEdges,
		stats.ProtectedRels,
		stats.ProtectedPendingCPUs,
		stats.ProbeOperations,
		stats.ProbeLimit,
		stats.ContextOperations,
		stats.ContextPhase,
		stats.BaseOperations,
		stats.ProtectedOperations,
		stats.RelIndexOperations,
		stats.ChildIndexOps,
		stats.ChildMembershipsScanned,
		stats.FrontierIndexOps,
		stats.FrontierMembershipsScanned,
		stats.AncestorClosureOps,
		stats.AtomOperations,
		stats.SnapshotID,
	)
}

func verifiedUnownedFinalCPUs(
	allowed machine.CPUSet,
	observed, desired map[DomainID]machine.CPUSet,
) machine.CPUSet {
	owned := machine.NewCPUSet()
	for _, cpus := range observed {
		owned = owned.Union(cpus)
	}
	unowned := allowed.Difference(owned)
	anchors := machine.NewCPUSet()
	for _, cpu := range unowned.ToSliceInt() {
		owners := 0
		for _, cpus := range desired {
			if cpus.Contains(cpu) {
				owners++
			}
		}
		if owners == 1 {
			anchors = anchors.Union(machine.NewCPUSet(cpu))
		}
	}
	return anchors
}

func isNarrowPrimaryReclaimCycle(graph map[DomainID]map[DomainID]machine.CPUSet) bool {
	if len(graph) != 2 {
		return false
	}
	primaryToReclaim := graph[DomainPrimary][DomainReclaim]
	reclaimToPrimary := graph[DomainReclaim][DomainPrimary]
	if primaryToReclaim.IsEmpty() || reclaimToPrimary.IsEmpty() {
		return false
	}
	for source, destinations := range graph {
		for destination := range destinations {
			if !((source == DomainPrimary && destination == DomainReclaim) ||
				(source == DomainReclaim && destination == DomainPrimary)) {
				return false
			}
		}
	}
	return true
}

func structuralV1Deadlock(analysis DeadlockAnalysis) bool {
	if analysis.Completeness != ProbeComplete ||
		len(analysis.Atoms) == 0 ||
		len(analysis.AtomClasses) != len(analysis.Atoms) {
		return false
	}
	for _, class := range analysis.AtomClasses {
		if class != DrainAtomClassV1Empty {
			return false
		}
	}
	return true
}
