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
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type FrozenBoundaryVersion uint8

const FrozenBoundaryVersionV1 FrozenBoundaryVersion = 1

// FrozenBoundary is the compiler-owned semantic read boundary for one frozen
// trace. ScanBoundary.ExpandedRels remains diagnostic and is not part of this
// execution contract.
type FrozenBoundary struct {
	Version             FrozenBoundaryVersion
	Roots               []string
	ControlledRels      []string
	ShrinkChildrenByRel map[string][]ChildRef
	RelevantCPUHolders  []string
	RelevantCPUs        machine.CPUSet
}

type FrozenBoundaryEvaluation struct {
	Snapshot *CompleteSnapshot
}

func compileFrozenBoundaryV1(
	snapshot *CompleteSnapshot,
	input FrozenCoordinatorEvaluationInput,
	phases []CompiledPhase,
) (FrozenBoundary, error) {
	if snapshot == nil {
		return FrozenBoundary{}, fmt.Errorf("compile frozen boundary requires initial snapshot")
	}
	controlled := make(map[string]struct{}, len(input.DAGSpecs))
	for _, spec := range input.DAGSpecs {
		controlled[spec.Rel] = struct{}{}
	}
	relevant := input.ProtectedPending.Clone()
	shrinkRels := make(map[string]struct{})
	for _, values := range []map[string]machine.CPUSet{
		input.RequiredByRel,
		input.PendingRequiredByRel,
	} {
		for _, cpus := range values {
			relevant = relevant.Union(cpus)
		}
	}
	for _, phase := range phases {
		for _, operation := range phase.Operations {
			if operation.Direction == WriteShrink {
				shrinkRels[operation.Rel] = struct{}{}
			}
			transition := operation.ExpectedCurrent.CPUs.Difference(operation.Target.CPUs).
				Union(operation.Target.CPUs.Difference(operation.ExpectedCurrent.CPUs))
			relevant = relevant.Union(transition)
		}
	}

	boundary := FrozenBoundary{
		Version:             FrozenBoundaryVersionV1,
		Roots:               normalizeRels(snapshot.ScanBoundary.Roots),
		ControlledRels:      sortedStringKeys(controlled),
		ShrinkChildrenByRel: make(map[string][]ChildRef, len(shrinkRels)),
		RelevantCPUs:        relevant,
	}
	for _, rel := range boundary.ControlledRels {
		if _, ok := snapshot.Entries[rel]; !ok {
			return FrozenBoundary{}, fmt.Errorf("controlled rel %q is absent from initial snapshot", rel)
		}
	}
	for rel := range shrinkRels {
		if _, ok := snapshot.Entries[rel]; !ok {
			return FrozenBoundary{}, fmt.Errorf("shrink rel %q is absent from initial snapshot", rel)
		}
		boundary.ShrinkChildrenByRel[rel] = cloneSortedChildRefs(snapshot.Children[rel])
	}
	for rel, entry := range snapshot.Entries {
		if _, ok := controlled[rel]; ok {
			continue
		}
		if !entry.CPUs.Intersection(relevant).IsEmpty() {
			boundary.RelevantCPUHolders = append(boundary.RelevantCPUHolders, rel)
		}
	}
	sort.Strings(boundary.RelevantCPUHolders)
	if err := validateFrozenBoundary(boundary, snapshot); err != nil {
		return FrozenBoundary{}, err
	}
	return boundary, nil
}

func validateFrozenBoundary(boundary FrozenBoundary, snapshot *CompleteSnapshot) error {
	if boundary.Version != FrozenBoundaryVersionV1 {
		return fmt.Errorf("unsupported frozen boundary version %d", boundary.Version)
	}
	if snapshot == nil {
		return fmt.Errorf("frozen boundary requires snapshot evidence")
	}
	if len(boundary.Roots) == 0 {
		return fmt.Errorf("frozen boundary requires roots")
	}
	for _, rel := range boundary.ControlledRels {
		if _, ok := snapshot.Entries[rel]; !ok {
			return fmt.Errorf("frozen boundary controlled rel %q has no snapshot entry", rel)
		}
	}
	for rel, children := range boundary.ShrinkChildrenByRel {
		if _, ok := snapshot.Entries[rel]; !ok {
			return fmt.Errorf("frozen boundary shrink rel %q has no snapshot entry", rel)
		}
		if !equalChildRefs(
			cloneSortedChildRefs(children),
			cloneSortedChildRefs(snapshot.Children[rel]),
		) {
			return fmt.Errorf("frozen boundary shrink children for %q do not match snapshot", rel)
		}
	}
	for _, rel := range boundary.RelevantCPUHolders {
		entry, ok := snapshot.Entries[rel]
		if !ok {
			return fmt.Errorf("frozen boundary relevant CPU holder %q has no snapshot entry", rel)
		}
		if entry.CPUs.Intersection(boundary.RelevantCPUs).IsEmpty() {
			return fmt.Errorf("frozen boundary holder %q does not hold relevant CPUs", rel)
		}
	}
	return nil
}

func cloneFrozenBoundary(in FrozenBoundary) FrozenBoundary {
	out := in
	out.Roots = append([]string(nil), in.Roots...)
	out.ControlledRels = append([]string(nil), in.ControlledRels...)
	out.ShrinkChildrenByRel = make(map[string][]ChildRef, len(in.ShrinkChildrenByRel))
	for rel, children := range in.ShrinkChildrenByRel {
		out.ShrinkChildrenByRel[rel] = append([]ChildRef(nil), children...)
	}
	out.RelevantCPUHolders = append([]string(nil), in.RelevantCPUHolders...)
	out.RelevantCPUs = in.RelevantCPUs.Clone()
	return out
}

// EvaluateFrozenBoundary captures a fresh root-complete snapshot and compares
// only the compiler-owned execution boundary. Dynamic descendants outside that
// boundary remain observable in the returned snapshot but do not invalidate a
// trace merely because they churn.
func EvaluateFrozenBoundary(
	ctx context.Context,
	driver HierarchyDriver,
	dag *TopoDAG,
	budget *BudgetTracker,
	boundary FrozenBoundary,
	expected *CompleteSnapshot,
) (FrozenBoundaryEvaluation, error) {
	if err := validateFrozenBoundary(boundary, expected); err != nil {
		return FrozenBoundaryEvaluation{}, err
	}
	fresh, err := BuildCompleteSnapshot(
		ctx,
		driver,
		dag,
		SnapshotRequest{Purpose: ScanForPlan, AffectedRels: boundary.Roots},
		budget,
	)
	if err != nil {
		return FrozenBoundaryEvaluation{}, err
	}
	if err := evaluateFrozenBoundarySnapshot(boundary, expected, fresh); err != nil {
		return FrozenBoundaryEvaluation{Snapshot: fresh}, err
	}
	return FrozenBoundaryEvaluation{
		Snapshot: projectFrozenBoundarySnapshot(boundary, expected, fresh),
	}, nil
}

func projectFrozenBoundarySnapshot(
	boundary FrozenBoundary,
	expected, current *CompleteSnapshot,
) *CompleteSnapshot {
	projected := CloneCompleteSnapshot(expected)
	for _, rel := range boundary.ControlledRels {
		projected.Entries[rel] = cloneEntryState(current.Entries[rel])
	}
	for rel := range boundary.ShrinkChildrenByRel {
		projected.Children[rel] = append([]ChildRef(nil), current.Children[rel]...)
		for _, child := range current.Children[rel] {
			childRel := filepath.Join(rel, child.Name)
			if entry, ok := current.Entries[childRel]; ok {
				projected.Entries[childRel] = cloneEntryState(entry)
			}
		}
	}
	for _, rel := range boundary.ControlledRels {
		if _, shrink := boundary.ShrinkChildrenByRel[rel]; shrink {
			continue
		}
		projectGrowChildren(projected, current, rel)
	}
	for _, rel := range boundary.RelevantCPUHolders {
		projected.Entries[rel] = cloneEntryState(current.Entries[rel])
	}
	projected.DomainUnion = make(map[DomainID]machine.CPUSet)
	for rel, entry := range projected.Entries {
		domain := projected.DomainByRel[rel]
		projected.DomainUnion[domain] = projected.DomainUnion[domain].Union(entry.CPUs)
	}
	projected.ID = fingerprintSnapshot(projected)
	return projected
}

func projectGrowChildren(projected, current *CompleteSnapshot, rel string) {
	currentChildren := make(map[ChildRef]struct{}, len(current.Children[rel]))
	for _, child := range current.Children[rel] {
		currentChildren[child] = struct{}{}
	}
	retained := make([]ChildRef, 0, len(projected.Children[rel]))
	for _, child := range projected.Children[rel] {
		if _, ok := currentChildren[child]; ok {
			retained = append(retained, child)
			continue
		}
		deleteProjectedSubtree(projected, filepath.Join(rel, child.Name))
	}
	projected.Children[rel] = retained
}

func deleteProjectedSubtree(snapshot *CompleteSnapshot, rel string) {
	prefix := rel + "/"
	for candidate := range snapshot.Entries {
		if candidate == rel || strings.HasPrefix(candidate, prefix) {
			delete(snapshot.Entries, candidate)
			delete(snapshot.Children, candidate)
			delete(snapshot.DomainByRel, candidate)
			delete(snapshot.UnavailableChildren, candidate)
		}
	}
}

func cloneEntryState(entry EntryState) EntryState {
	entry.CPUs = entry.CPUs.Clone()
	entry.ConfiguredCPUs = entry.ConfiguredCPUs.Clone()
	return entry
}

func evaluateFrozenBoundarySnapshot(
	boundary FrozenBoundary,
	expected, current *CompleteSnapshot,
) error {
	if err := validateFrozenBoundary(boundary, expected); err != nil {
		return err
	}
	if current == nil {
		return frozenBoundaryStale("boundary", "<nil>", "snapshot", fmt.Errorf("fresh snapshot is nil"))
	}
	controlled := make(map[string]struct{}, len(boundary.ControlledRels))
	for _, rel := range boundary.ControlledRels {
		controlled[rel] = struct{}{}
		want, wantOK := expected.Entries[rel]
		got, gotOK := current.Entries[rel]
		if !wantOK || !gotOK {
			return frozenBoundaryStale(
				rel, fmt.Sprintf("exists=%t", gotOK), fmt.Sprintf("exists=%t", wantOK),
				fmt.Errorf("controlled relation presence changed"))
		}
		if !entryPhysicalStateEqual(got, want) {
			return frozenBoundaryStale(
				rel, frozenEntryStateString(got), frozenEntryStateString(want),
				fmt.Errorf("controlled relation state changed"))
		}
	}
	for rel, children := range boundary.ShrinkChildrenByRel {
		gotChildren := cloneSortedChildRefs(current.Children[rel])
		wantChildren := cloneSortedChildRefs(children)
		if !equalChildRefs(gotChildren, wantChildren) {
			return frozenBoundaryStale(
				rel, fmt.Sprint(gotChildren), fmt.Sprint(wantChildren),
				fmt.Errorf("shrink relation direct children changed"))
		}
		wantCPUs, wantMems, err := frozenDirectChildUnion(expected, rel, wantChildren)
		if err != nil {
			return err
		}
		gotCPUs, gotMems, err := frozenDirectChildUnion(current, rel, gotChildren)
		if err != nil {
			return err
		}
		if !gotCPUs.Equals(wantCPUs) {
			return frozenBoundaryStale(
				rel, gotCPUs.String(), wantCPUs.String(),
				fmt.Errorf("shrink direct child CPU union changed"))
		}
		if !gotMems.Equals(wantMems) {
			return frozenBoundaryStale(
				rel, gotMems.String(), wantMems.String(),
				fmt.Errorf("shrink direct child mems union changed"))
		}
	}

	expectedHolders := append([]string(nil), boundary.RelevantCPUHolders...)
	currentHolders := make([]string, 0, len(expectedHolders))
	for rel, entry := range current.Entries {
		if _, isControlled := controlled[rel]; isControlled {
			continue
		}
		if !entry.CPUs.Intersection(boundary.RelevantCPUs).IsEmpty() {
			currentHolders = append(currentHolders, rel)
		}
	}
	sort.Strings(currentHolders)
	if !equalStringSlices(currentHolders, expectedHolders) {
		return frozenBoundaryStale(
			"dynamic", fmt.Sprint(currentHolders), fmt.Sprint(expectedHolders),
			fmt.Errorf("relevant CPU holder set changed"))
	}
	for _, rel := range expectedHolders {
		want, wantOK := expected.Entries[rel]
		got, gotOK := current.Entries[rel]
		if !wantOK || !gotOK {
			return frozenBoundaryStale(
				rel, fmt.Sprintf("exists=%t", gotOK), fmt.Sprintf("exists=%t", wantOK),
				fmt.Errorf("relevant CPU holder presence changed"))
		}
		if !entryPhysicalStateEqual(got, want) {
			return frozenBoundaryStale(
				rel, frozenEntryStateString(got), frozenEntryStateString(want),
				fmt.Errorf("relevant CPU holder state changed"))
		}
	}
	return nil
}

func frozenDirectChildUnion(
	snapshot *CompleteSnapshot,
	parentRel string,
	children []ChildRef,
) (machine.CPUSet, machine.CPUSet, error) {
	cpus := machine.NewCPUSet()
	mems := machine.NewCPUSet()
	for _, child := range children {
		childRel := filepath.Join(parentRel, child.Name)
		entry, ok := snapshot.Entries[childRel]
		if !ok {
			if _, unavailable := snapshot.UnavailableChildren[childRel]; unavailable {
				continue
			}
			return machine.CPUSet{}, machine.CPUSet{}, frozenBoundaryStale(
				parentRel, childRel, "direct child entry",
				fmt.Errorf("shrink direct child has no snapshot evidence"))
		}
		cpus = cpus.Union(entry.CPUs)
		if entry.Mems == "" {
			continue
		}
		childMems, err := machine.Parse(entry.Mems)
		if err != nil {
			return machine.CPUSet{}, machine.CPUSet{}, fmt.Errorf(
				"parse shrink direct child %q cpuset.mems=%q: %w",
				childRel, entry.Mems, err)
		}
		mems = mems.Union(childMems)
	}
	return cpus, mems, nil
}

func entryPhysicalStateEqual(left, right EntryState) bool {
	return left.Identity == right.Identity &&
		left.ConfiguredCPUs.Equals(right.ConfiguredCPUs) &&
		left.CPUs.Equals(right.CPUs) &&
		cpusetListValuesEqual(left.ConfiguredMems, right.ConfiguredMems) &&
		cpusetListValuesEqual(left.Mems, right.Mems)
}

func frozenEntryStateString(entry EntryState) string {
	return fmt.Sprintf(
		"identity=%v configured_cpus=%s effective_cpus=%s configured_mems=%s effective_mems=%s",
		entry.Identity, entry.ConfiguredCPUs.String(), entry.CPUs.String(),
		entry.ConfiguredMems, entry.Mems)
}

func frozenBoundaryStale(rel, current, target string, cause error) error {
	return &PlanStaleError{
		Rel:       rel,
		Direction: WritePublish,
		Resource:  "frozen_boundary",
		Current:   current,
		Target:    target,
		Err:       cause,
	}
}

func cloneSortedChildRefs(in []ChildRef) []ChildRef {
	out := append([]ChildRef(nil), in...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		if out[i].Identity.Device != out[j].Identity.Device {
			return out[i].Identity.Device < out[j].Identity.Device
		}
		return out[i].Identity.Inode < out[j].Identity.Inode
	})
	return out
}

func equalChildRefs(left, right []ChildRef) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func equalStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func frozenBoundariesEqual(left, right FrozenBoundary) bool {
	return left.Version == right.Version &&
		reflect.DeepEqual(left.Roots, right.Roots) &&
		reflect.DeepEqual(left.ControlledRels, right.ControlledRels) &&
		reflect.DeepEqual(left.ShrinkChildrenByRel, right.ShrinkChildrenByRel) &&
		reflect.DeepEqual(left.RelevantCPUHolders, right.RelevantCPUHolders) &&
		left.RelevantCPUs.Equals(right.RelevantCPUs)
}

func writeFrozenBoundaryHash(
	hash interface{ Write([]byte) (int, error) },
	boundary FrozenBoundary,
) {
	writeHashString(hash, "frozen-boundary")
	writeHashUint64(hash, uint64(boundary.Version))
	writeStringSliceHash(hash, boundary.Roots)
	writeStringSliceHash(hash, boundary.ControlledRels)
	shrinkRels := sortedStringKeys(boundary.ShrinkChildrenByRel)
	writeHashUint64(hash, uint64(len(shrinkRels)))
	for _, rel := range shrinkRels {
		writeHashString(hash, rel)
		children := boundary.ShrinkChildrenByRel[rel]
		writeHashUint64(hash, uint64(len(children)))
		for _, child := range children {
			writeHashString(hash, child.Name)
			writeHashUint64(hash, child.Identity.Device)
			writeHashUint64(hash, child.Identity.Inode)
		}
	}
	writeStringSliceHash(hash, boundary.RelevantCPUHolders)
	writeHashString(hash, boundary.RelevantCPUs.String())
}

func writeStringSliceHash(
	hash interface{ Write([]byte) (int, error) },
	values []string,
) {
	writeHashUint64(hash, uint64(len(values)))
	for _, value := range values {
		writeHashString(hash, value)
	}
}
