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
	}
	if in.AllowEmptyTarget || in.Kind != PhaseDrain || in.DAG == nil || in.Snapshot == nil {
		return analysis, nil
	}
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
	leavingByDomain := make(map[DomainID]machine.CPUSet, len(domains))
	for _, domain := range domains {
		leavingByDomain[domain] = in.Snapshot.DomainUnion[domain].Difference(desiredByDomain[domain])
	}
	for i := range analysis.Atoms {
		if limit > 0 && probeCost >= limit {
			analysis.Completeness = ProbeIndeterminate
			return analysis, fmt.Errorf("%w: limit=%d used=%d",
				ErrDeadlockProbeBudgetExceeded, limit, probeCost)
		}
		atom := analysis.Atoms[i]
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
		projection, err := projectDrainTargets(DrainProjectionInput{
			PlanInput: in, DrainBatch: drainBatch, LeavingByDomain: leavingByDomain,
			DomainByRel: domainByRel, ParentByRel: parentByRel, DepthByRel: depthByRel,
			ProbeContext: ctx, ProbeBudget: in.Budget,
		})
		if err != nil {
			if errors.Is(err, ErrDeadlockProbeBudgetExceeded) {
				analysis.Completeness = ProbeIndeterminate
				return analysis, err
			}
			return DeadlockAnalysis{}, err
		}
		projectionCost := projection.Cost.Total()
		if limit > 0 && projectionCost > limit-probeCost {
			analysis.Completeness = ProbeIndeterminate
			return analysis, fmt.Errorf("%w: limit=%d used=%d requested=%d",
				ErrDeadlockProbeBudgetExceeded, limit, probeCost, projectionCost)
		}
		probeCost += projectionCost
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
