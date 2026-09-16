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
	"errors"
	"fmt"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

// ErrProjectedParentContainment reports that a projected grow would place a
// child's configured cpuset outside its parent's effective set, which the
// kernel would reject. The projection rejects it before mutating the clone so a
// compiled trace never freezes an operation the live hierarchy would refuse.
var ErrProjectedParentContainment = errors.New("projected target exceeds parent effective cpuset")

// projectedHierarchy applies ordered plan operations to a cloned snapshot using
// the exact cgroup-version semantics of the backend the trace targets. It never
// touches a live driver: every mutation lands on the clone, so the fixed-point
// compiler can model a complete Drain->Expand trace and prove its end state
// without disturbing captured evidence or the real hierarchy.
//
// Parent/child relationships are resolved by stable cgroup identity through the
// snapshot's Children map, not by path strings, so inheritance follows the same
// edges the driver observed.
type projectedHierarchy struct {
	snapshot      *CompleteSnapshot
	capabilities  HierarchyCapabilities
	parentByRel   map[string]string
	childrenByRel map[string][]string
	phases        []CompiledPhase
}

// newProjectedHierarchy clones base and indexes its parent/child edges by
// identity so projection can re-derive effective state after every operation.
func newProjectedHierarchy(
	base *CompleteSnapshot,
	capabilities HierarchyCapabilities,
) (*projectedHierarchy, error) {
	if base == nil {
		return nil, fmt.Errorf("projected hierarchy requires a base snapshot")
	}
	snapshot := CloneCompleteSnapshot(base)
	snapshot.Capabilities = capabilities

	identityToRel := make(map[CgroupIdentity]string, len(snapshot.Entries))
	for rel, entry := range snapshot.Entries {
		identityToRel[entry.Identity] = rel
	}
	parentByRel := make(map[string]string, len(snapshot.Entries))
	childrenByRel := make(map[string][]string, len(snapshot.Children))
	for parentRel, refs := range snapshot.Children {
		for _, ref := range refs {
			childRel, ok := identityToRel[ref.Identity]
			if !ok {
				continue
			}
			parentByRel[childRel] = parentRel
			childrenByRel[parentRel] = append(childrenByRel[parentRel], childRel)
		}
	}
	return &projectedHierarchy{
		snapshot:      snapshot,
		capabilities:  capabilities,
		parentByRel:   parentByRel,
		childrenByRel: childrenByRel,
	}, nil
}

// applyOperation projects one plan operation onto the clone under the backend's
// cgroup-version semantics, re-derives the effective subtree it changed, and
// refreshes the snapshot evidence. It rejects operations the live kernel would
// refuse (empty v1 configured, out-of-parent grows) before mutating any state.
func (h *projectedHierarchy) applyOperation(operation PlanOperation) error {
	entry, ok := h.snapshot.Entries[operation.Rel]
	if !ok {
		return fmt.Errorf("projected hierarchy has no entry for rel %q", operation.Rel)
	}

	// Memory writes are independent of cpuset writes: they only reconfigure the
	// mems dimension and let projection re-derive inheritance.
	if operation.WriteMems {
		entry.ConfiguredMems = operation.Target.Mems
		h.snapshot.Entries[operation.Rel] = entry
		if err := h.recomputeEffectiveSubtree(operation.Rel); err != nil {
			return err
		}
		return h.recomputeEvidence()
	}

	target := operation.Target.CPUs
	if target.IsEmpty() {
		// An empty configured cpuset means "inherit the parent" only where the
		// backend supports it; v1 has no such representation and must be refused.
		if !h.capabilities.EmptyConfiguredCPUSet {
			return fmt.Errorf("%w", ErrEmptyCPUSetUnsupported)
		}
		entry.ConfiguredCPUs = machine.NewCPUSet()
	} else {
		if parentRel := h.parentByRel[operation.Rel]; parentRel != "" {
			parentEffective := h.snapshot.Entries[parentRel].CPUs
			if !target.IsSubsetOf(parentEffective) {
				return fmt.Errorf("%w: rel %q target %s exceeds parent %q effective %s",
					ErrProjectedParentContainment, operation.Rel,
					target.String(), parentRel, parentEffective.String())
			}
		}
		entry.ConfiguredCPUs = target.Clone()
	}
	h.snapshot.Entries[operation.Rel] = entry

	if err := h.recomputeEffectiveSubtree(operation.Rel); err != nil {
		return err
	}
	return h.recomputeEvidence()
}

// recomputeEffectiveSubtree re-derives effective CPU and memory state for rel
// and every descendant, top-down, so a parent grow or shrink re-projects every
// inheriting child before the child itself is evaluated.
func (h *projectedHierarchy) recomputeEffectiveSubtree(rel string) error {
	if _, ok := h.snapshot.Entries[rel]; !ok {
		return fmt.Errorf("projected hierarchy has no entry for rel %q", rel)
	}
	h.projectEntry(rel)
	for _, child := range h.childrenByRel[rel] {
		if err := h.recomputeEffectiveSubtree(child); err != nil {
			return err
		}
	}
	return nil
}

// projectEntry derives one entry's effective CPUs and mems from its configured
// state and its parent's current effective state, applying the backend's
// inheritance and kernel-containment semantics.
func (h *projectedHierarchy) projectEntry(rel string) {
	entry := h.snapshot.Entries[rel]
	parent, hasParent := h.parentEntry(rel)

	switch {
	case h.capabilities.EmptyConfiguredCPUSet && entry.ConfiguredCPUs.IsEmpty():
		// Empty configured means inherit the parent's effective set.
		if hasParent {
			entry.CPUs = parent.CPUs.Clone()
		} else {
			entry.CPUs = machine.NewCPUSet()
		}
	case hasParent:
		// A configured child is clamped into its parent by kernel containment.
		entry.CPUs = entry.ConfiguredCPUs.Intersection(parent.CPUs)
	default:
		entry.CPUs = entry.ConfiguredCPUs.Clone()
	}

	switch {
	case h.capabilities.EmptyConfiguredCPUSet && entry.ConfiguredMems == "":
		if hasParent {
			entry.Mems = parent.Mems
		} else {
			entry.Mems = ""
		}
	default:
		entry.Mems = entry.ConfiguredMems
	}

	h.snapshot.Entries[rel] = entry
}

// parentEntry returns the identity-resolved parent entry and whether one exists.
func (h *projectedHierarchy) parentEntry(rel string) (EntryState, bool) {
	parentRel := h.parentByRel[rel]
	if parentRel == "" {
		return EntryState{}, false
	}
	parent, ok := h.snapshot.Entries[parentRel]
	return parent, ok
}

// recomputeEvidence rebuilds the domain unions from the projected effective sets
// and refreshes the snapshot fingerprint so downstream proofs bind to the exact
// projected end state rather than the pre-operation snapshot.
func (h *projectedHierarchy) recomputeEvidence() error {
	unions := make(map[DomainID]machine.CPUSet, len(h.snapshot.DomainUnion))
	for rel, entry := range h.snapshot.Entries {
		domain, ok := h.snapshot.DomainByRel[rel]
		if !ok {
			continue
		}
		existing, ok := unions[domain]
		if !ok {
			existing = machine.NewCPUSet()
		}
		unions[domain] = existing.Union(entry.CPUs)
	}
	h.snapshot.DomainUnion = unions
	h.snapshot.ID = fingerprintSnapshot(h.snapshot)
	return nil
}
