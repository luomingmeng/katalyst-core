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

package topology

import (
	"fmt"
	"sort"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type admissionRequiredOperationClosure struct {
	DrainOperations  []PlanOperation
	ExpandOperations []PlanOperation
	FinalSnapshot    *CompleteSnapshot
	FinalReport      ParentSafetyReport
}

func proveAdmissionRequiredClosure(
	plan PhasePlan,
	requiredByRel map[string]machine.CPUSet,
) (admissionRequiredOperationClosure, error) {
	closure := admissionRequiredOperationClosure{
		DrainOperations: append([]PlanOperation(nil), plan.Operations...),
	}
	if plan.Base == nil {
		return closure, fmt.Errorf("prove admission required closure: missing base snapshot")
	}
	projected := cloneAdmissionSnapshot(plan.Base)
	for _, operation := range closure.DrainOperations {
		if err := projectAdmissionOperation(projected, operation); err != nil {
			return closure, fmt.Errorf("project admission drain %q: %w", operation.Rel, err)
		}
	}

	canonical := finalAdmissionTarget(&plan)
	depthByRel := buildSnapshotDepthByRel(projected, nil)
	requiredClosureRels := make(map[string]struct{})
	addRequiredRelAndAncestors := func(rel string) {
		for current := rel; current != ""; current = parentRelFromSnapshot(projected, current) {
			requiredClosureRels[current] = struct{}{}
		}
	}
	for rel := range requiredByRel {
		addRequiredRelAndAncestors(rel)
	}
	incomingCPUsByDestination := make(map[DomainID]machine.CPUSet, len(plan.TransferGraph))
	for _, destinations := range plan.TransferGraph {
		for destination, cpus := range destinations {
			incomingCPUsByDestination[destination] =
				incomingCPUsByDestination[destination].Union(cpus)
		}
	}
	for rel, target := range canonical {
		current, ok := projected.Entries[rel]
		if !ok {
			continue
		}
		addedCPUs := target.CPUs.Difference(current.CPUs)
		domain := projected.DomainByRel[rel]
		if !addedCPUs.Intersection(incomingCPUsByDestination[domain]).IsEmpty() {
			addRequiredRelAndAncestors(rel)
		}
	}
	if len(requiredClosureRels) == 0 {
		for _, operation := range plan.Operations {
			requiredClosureRels[operation.Rel] = struct{}{}
		}
	}
	rels := make([]string, 0, len(requiredClosureRels))
	for rel := range requiredClosureRels {
		if _, ok := canonical[rel]; !ok {
			continue
		}
		rels = append(rels, rel)
	}
	sort.Slice(rels, func(i, j int) bool {
		if depthByRel[rels[i]] != depthByRel[rels[j]] {
			return depthByRel[rels[i]] < depthByRel[rels[j]]
		}
		return rels[i] < rels[j]
	})
	for _, rel := range rels {
		target := canonical[rel]
		current, ok := projected.Entries[rel]
		if !ok {
			return closure, fmt.Errorf("prove admission required closure: missing canonical rel %q", rel)
		}
		if current.CPUs.Equals(target.CPUs) && current.Mems == target.Mems {
			continue
		}
		if !current.CPUs.IsSubsetOf(target.CPUs) {
			return closure, fmt.Errorf(
				"prove admission required closure: canonical target for %q still requires drain: current=%s target=%s",
				rel, current.CPUs.String(), target.CPUs.String())
		}
		operation := PlanOperation{
			Rel:              rel,
			ExpectedIdentity: current.Identity,
			ExpectedCurrent:  CPUSetTarget{CPUs: current.CPUs.Clone(), Mems: current.Mems},
			Target:           cloneCPUSetTarget(target),
			Direction:        WriteGrow,
			WriteMems:        current.Mems != target.Mems,
			Requirement:      OperationAdmissionSafetyRepair,
		}
		operation.ParentRel = parentRelFromSnapshot(projected, rel)
		if parent, found := projected.Entries[operation.ParentRel]; found {
			operation.ExpectedParentIdentity = parent.Identity
		}
		closure.ExpandOperations = append(closure.ExpandOperations, operation)
		if err := projectAdmissionOperation(projected, operation); err != nil {
			return closure, fmt.Errorf("project admission expansion %q: %w", rel, err)
		}
	}
	recomputeAdmissionDomainUnion(projected)
	closure.FinalSnapshot = projected
	closure.FinalReport = admissionClosureSafetyReport(projected, requiredByRel)
	if !closure.FinalReport.Safe {
		return closure, fmt.Errorf(
			"required admission closure does not prove parent safety: deficit=%v unsafe=%v overlap=%s",
			closure.FinalReport.RequiredFloorDeficit,
			closure.FinalReport.UnsafeRequiredRels,
			closure.FinalReport.PrimaryReclaimOverlap.String())
	}
	return closure, nil
}

func cloneAdmissionSnapshot(snapshot *CompleteSnapshot) *CompleteSnapshot {
	return CloneCompleteSnapshot(snapshot)
}

func projectAdmissionOperation(snapshot *CompleteSnapshot, operation PlanOperation) error {
	current, ok := snapshot.Entries[operation.Rel]
	if !ok {
		return fmt.Errorf("missing rel")
	}
	if operation.Direction == WriteGrow && !current.CPUs.IsSubsetOf(operation.Target.CPUs) {
		return fmt.Errorf("grow target %s is not a superset of %s",
			operation.Target.CPUs.String(), current.CPUs.String())
	}
	if operation.Direction == WriteShrink && !operation.Target.CPUs.IsSubsetOf(current.CPUs) {
		return fmt.Errorf("shrink target %s is not a subset of %s",
			operation.Target.CPUs.String(), current.CPUs.String())
	}
	current.CPUs = operation.Target.CPUs.Clone()
	current.ConfiguredCPUs = operation.Target.CPUs.Clone()
	if operation.WriteMems {
		current.Mems = operation.Target.Mems
		current.ConfiguredMems = operation.Target.Mems
	}
	snapshot.Entries[operation.Rel] = current
	return nil
}

func parentRelFromSnapshot(snapshot *CompleteSnapshot, childRel string) string {
	for parentRel, children := range snapshot.Children {
		for _, child := range children {
			rel := child.Name
			if parentRel != "" {
				rel = parentRel + "/" + child.Name
			}
			if rel == childRel {
				return parentRel
			}
		}
	}
	return ""
}

func recomputeAdmissionDomainUnion(snapshot *CompleteSnapshot) {
	if len(snapshot.DomainByRel) == 0 {
		return
	}
	snapshot.DomainUnion = make(map[DomainID]machine.CPUSet)
	for rel, entry := range snapshot.Entries {
		domain := snapshot.DomainByRel[rel]
		if domain == "" {
			continue
		}
		snapshot.DomainUnion[domain] = snapshot.DomainUnion[domain].Union(entry.CPUs)
	}
	snapshot.ID = fingerprintSnapshot(snapshot)
}

func admissionClosureSafetyReport(
	snapshot *CompleteSnapshot,
	requiredByRel map[string]machine.CPUSet,
) ParentSafetyReport {
	report := ParentSafetyReport{RequiredFloorDeficit: make(map[string]machine.CPUSet)}
	for rel, required := range requiredByRel {
		entry, ok := snapshot.Entries[rel]
		if !ok {
			report.RequiredFloorDeficit[rel] = required.Clone()
			continue
		}
		if deficit := required.Difference(entry.CPUs); !deficit.IsEmpty() {
			report.RequiredFloorDeficit[rel] = deficit
		}
	}
	report.PrimaryReclaimOverlap =
		snapshot.DomainUnion[DomainPrimary].Intersection(snapshot.DomainUnion[DomainReclaim])
	for parentRel, children := range snapshot.Children {
		parent, ok := snapshot.Entries[parentRel]
		if !ok {
			continue
		}
		for _, child := range children {
			childRel := child.Name
			if parentRel != "" {
				childRel = parentRel + "/" + child.Name
			}
			childEntry, ok := snapshot.Entries[childRel]
			if ok && !childEntry.CPUs.IsSubsetOf(parent.CPUs) {
				report.UnsafeRequiredRels = append(report.UnsafeRequiredRels, RelConvergence{
					Rel: childRel, Observed: childEntry.CPUs.Clone(),
					Target: parent.CPUs.Clone(), Reason: "parent_not_containing_child",
				})
			}
		}
	}
	report.Safe = len(report.RequiredFloorDeficit) == 0 &&
		report.PrimaryReclaimOverlap.IsEmpty() &&
		len(report.UnsafeRequiredRels) == 0
	return report
}
