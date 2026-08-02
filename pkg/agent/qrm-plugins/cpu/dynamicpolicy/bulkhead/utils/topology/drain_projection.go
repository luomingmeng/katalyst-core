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
	"fmt"
	"path/filepath"
	"strings"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type DrainProjectionCost struct {
	Rels          int
	Children      int
	ProtectedRels int
}

func (c DrainProjectionCost) Total() int {
	return saturatingAdd(saturatingAdd(c.Rels, c.Children), c.ProtectedRels)
}

type DrainProjection struct {
	TargetByRel   map[string]CPUSetTarget
	DomainUnion   map[DomainID]machine.CPUSet
	EmptyBlockers map[string]machine.CPUSet
	Cost          DrainProjectionCost
}

type DrainProjectionInput struct {
	PlanInput       PhasePlanInput
	DrainBatch      map[DomainID]machine.CPUSet
	LeavingByDomain map[DomainID]machine.CPUSet
	DomainByRel     map[string]DomainID
	ParentByRel     map[string]string
	DepthByRel      map[string]int
	ProbeContext    context.Context
	ProbeBudget     *BudgetTracker
}

// projectDrainTargets is the canonical, side-effect-free bottom-up drain
// calculation shared by planning and deadlock analysis.
func projectDrainTargets(input DrainProjectionInput) (DrainProjection, error) {
	in := input.PlanInput
	chargeProbe := func() error {
		if input.ProbeBudget == nil {
			return nil
		}
		ctx := input.ProbeContext
		if ctx == nil {
			ctx = context.Background()
		}
		return input.ProbeBudget.ConsumeDeadlockProbeOperations(ctx, 1)
	}
	projection := DrainProjection{
		TargetByRel:   make(map[string]CPUSetTarget, len(in.Snapshot.Entries)),
		DomainUnion:   make(map[DomainID]machine.CPUSet),
		EmptyBlockers: make(map[string]machine.CPUSet),
	}
	rels := sortedSnapshotRels(in.Snapshot, input.DepthByRel)
	bucketUpperByRel := make(map[string]machine.CPUSet)
	for _, rel := range rels {
		if node := in.DAG.index[rel]; node != nil {
			if node.Role == TopoNodeRoleReclaimNUMABucket &&
				!node.Constraint.CPUUpperBound.IsEmpty() {
				bucketUpperByRel[rel] = node.Constraint.CPUUpperBound
			} else if upper, ok := bucketUpperByRel[input.ParentByRel[rel]]; ok {
				bucketUpperByRel[rel] = upper
			}
			continue
		}
		if upper, ok := bucketUpperByRel[input.ParentByRel[rel]]; ok {
			bucketUpperByRel[rel] = upper
		}
	}
	for i := len(rels) - 1; i >= 0; i-- {
		if err := chargeProbe(); err != nil {
			return DrainProjection{}, err
		}
		rel := rels[i]
		projection.Cost.Rels++
		entry := in.Snapshot.Entries[rel]
		required := machine.NewCPUSet()
		node := in.DAG.index[rel]
		if node != nil {
			required = required.Union(in.DesiredByRel[rel])
			if node.Role == TopoNodeRolePrimary {
				required = required.Union(in.ProtectedPending)
			}
		} else if explicit, ok := in.DynamicByRel[rel]; ok {
			required = required.Union(explicit)
		}
		for protectedRel, cpus := range in.ProtectedByRel {
			if err := chargeProbe(); err != nil {
				return DrainProjection{}, err
			}
			projection.Cost.ProtectedRels++
			if protectedRel == rel || strings.HasPrefix(protectedRel, rel+"/") {
				required = required.Union(cpus)
			}
		}
		for _, child := range in.Snapshot.Children[rel] {
			if err := chargeProbe(); err != nil {
				return DrainProjection{}, err
			}
			projection.Cost.Children++
			if childTarget, ok := projection.TargetByRel[filepath.Join(rel, child.Name)]; ok {
				required = required.Union(childTarget.CPUs)
			}
		}

		domain := input.DomainByRel[rel]
		required = required.Intersection(entry.CPUs)
		var safeTarget machine.CPUSet
		if node != nil && node.Role == TopoNodeRoleReclaimNUMABucket {
			desired := in.DesiredByRel[rel]
			relLeaving := entry.CPUs.Difference(desired).Difference(required)
			relDrain := relLeaving.Intersection(input.DrainBatch[domain])
			if input.LeavingByDomain[domain].IsEmpty() {
				relDrain = relLeaving
			}
			safeTarget = entry.CPUs.Difference(relDrain).Union(required)
		} else {
			safeTarget = entry.CPUs.Difference(input.DrainBatch[domain]).Union(required)
			if input.LeavingByDomain[domain].IsEmpty() {
				safeTarget = required
			}
		}
		final := finalCPUSetForRel(rel, in.DAG, input.ParentByRel, in.DynamicByRel, in.DesiredByRel)
		if node == nil && input.LeavingByDomain[domain].IsEmpty() && safeTarget.IsEmpty() {
			safeTarget = safeTarget.Union(entry.CPUs.Intersection(final))
		}
		target, err := buildPhaseTransition(PhaseDrain, RelTransition{
			Current: entry.CPUs, Final: final, SafeDrainTarget: safeTarget,
			AllowEmptyTarget: in.AllowEmptyTarget,
		})
		if err != nil {
			return DrainProjection{}, err
		}
		if !in.AllowEmptyTarget && safeTarget.IsEmpty() && !target.IsEmpty() &&
			!entry.CPUs.Difference(final).IsEmpty() {
			projection.EmptyBlockers[rel] = entry.CPUs.Difference(final)
		}
		if node == nil {
			if upper, ok := bucketUpperByRel[rel]; ok {
				if !required.IsSubsetOf(upper) {
					return DrainProjection{}, fmt.Errorf("%w: dynamic descendant=%q required CPUs=%s outside nearest bucket upper=%s",
						ErrInvalidReclaimBucketTarget, rel, required.String(), upper.String())
				}
				target = constrainDrainTargetToUpper(target, required, upper, in.AllowEmptyTarget)
			}
		}
		if node != nil && node.Role == TopoNodeRoleReclaimNUMABucket &&
			!node.Constraint.CPUUpperBound.IsEmpty() {
			target = constrainDrainTargetToUpper(
				target, required, node.Constraint.CPUUpperBound, in.AllowEmptyTarget,
			)
		}
		mems := entry.Mems
		if node != nil {
			// A controlled node's mems, like its CPUs, are phase targets; dynamic descendants retain their live values.
			if desiredMems := in.DesiredMemsByRel[rel]; desiredMems != "" {
				mems = desiredMems
			} else if node.Mems != "" {
				mems = node.Mems
			}
		}
		projection.TargetByRel[rel] = CPUSetTarget{CPUs: target, Mems: mems}
		projection.DomainUnion[domain] = projection.DomainUnion[domain].Union(target)
	}
	return projection, nil
}

func constrainDrainTargetToUpper(
	target, required, upper machine.CPUSet,
	allowEmptyTarget bool,
) machine.CPUSet {
	constrained := target.Intersection(upper).Union(required)
	if !allowEmptyTarget && !target.IsEmpty() && constrained.IsEmpty() {
		return target
	}
	return constrained
}
