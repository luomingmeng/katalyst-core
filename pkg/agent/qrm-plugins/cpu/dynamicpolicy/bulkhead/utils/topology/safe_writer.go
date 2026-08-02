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

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type AppliedPlanOperation struct {
	PlanID    string
	Rel       string
	Direction WriteDirection
	Target    CPUSetTarget
	Observed  CPUSetTarget
}

type safeCPSetWriter struct {
	driver HierarchyDriver
	budget *BudgetTracker
	res    *ConvergenceResult
}

type stableLiveChildren struct {
	cpus machine.CPUSet
	mems machine.CPUSet
	refs []ChildRef
}

func newSafeCPUSetWriter(driver HierarchyDriver, budget *BudgetTracker, res *ConvergenceResult) safeCPSetWriter {
	return safeCPSetWriter{driver: driver, budget: budget, res: res}
}

func (w safeCPSetWriter) execute(ctx context.Context, plan PhasePlan) error {
	if plan.PlanID == "" || canonicalExecutionPlanID(plan) != plan.PlanID {
		return fmt.Errorf("phase writer requires canonical PlanID")
	}
	for _, operation := range plan.Operations {
		if operation.PlanID != plan.PlanID {
			return fmt.Errorf("phase writer rejected operation %q owned by PlanID %q, current %q",
				operation.Rel, operation.PlanID, plan.PlanID)
		}
		if operation.WriteMems && !operation.OwnsMems {
			return fmt.Errorf("phase writer rejected mems write for unowned rel %q", operation.Rel)
		}
	}
	if w.driver == nil {
		return fmt.Errorf("phase writer requires hierarchy driver")
	}
	if w.budget == nil {
		return fmt.Errorf("phase writer requires convergence budget")
	}
	stableChildUnion, err := w.scanStableShrinkChildren(ctx, plan.Operations, plan.Capabilities)
	if err != nil {
		return err
	}
	ioOperations := 0
	for _, operation := range plan.Operations {
		ioOperations++ // local operation read/identity/direction precondition
		if operation.ParentRel != "" {
			ioOperations++ // live immediate-parent identity/containment precondition
		}
		if operation.WriteMems {
			ioOperations++
		}
		if operation.Direction == WriteShrink && operation.WriteMems {
			children := stableChildUnion[operation.Rel]
			ioOperations += 2 + len(children.refs) // Re-prove live child mems before writing.
		}
		ioOperations++ // CPU write
		ioOperations++ // post-write read/journal
	}
	driver, err := newReservedBudgetedHierarchyDriver(w.driver, w.budget, ioOperations)
	if err != nil {
		return err
	}
	w.driver = driver
	for _, operation := range plan.Operations {
		if operation.Direction == WriteShrink && operation.WriteMems {
			children, err := scanLiveChildrenOnce(ctx, w.driver, operation)
			if err != nil {
				return err
			}
			stableChildUnion[operation.Rel] = children
		}
		if err := w.precheckOperation(ctx, operation, stableChildUnion, plan.Capabilities); err != nil {
			return err
		}
		if w.res != nil {
			w.res.Attempted++
		}
		if operation.WriteMems {
			if err := w.driver.WriteMems(ctx, operation.Rel, operation.ExpectedIdentity, operation.Target.Mems); err != nil {
				if w.res != nil {
					w.res.Failed++
				}
				return w.classifyWriteError(
					err, plan.Kind, HierarchyOperationWriteMems, operation, "cpuset.mems",
					operation.ExpectedCurrent.Mems, operation.Target.Mems)
			}
		}
		if err := w.driver.WriteCPUs(ctx, operation.Rel, operation.ExpectedIdentity, operation.Target.CPUs); err != nil {
			if w.res != nil {
				w.res.Failed++
			}
			if w.driver.Classify(err, HierarchyOperationWriteCPUs) != HierarchyErrorStale {
				return phaseWriteError(
					err, plan.Kind, operation, "cpuset.cpus",
					operation.ExpectedCurrent.CPUs.String(), operation.Target.CPUs.String(),
				)
			}
			current := operation.ExpectedCurrent.CPUs.String()
			if operation.WriteMems {
				if applied, readErr := w.readAppliedObservation(ctx, operation); readErr == nil {
					current = applied.Observed.CPUs.String()
					if w.res != nil {
						w.res.Journal = append(w.res.Journal, applied)
					}
				}
			}
			return &PlanStaleError{
				Rel: operation.Rel, Direction: operation.Direction, Resource: "cpuset.cpus",
				Current: current, Target: operation.Target.CPUs.String(), Err: err,
			}
		}
		applied, err := w.readAfterWrite(ctx, operation)
		if w.res != nil && applied.PlanID != "" {
			w.res.Journal = append(w.res.Journal, applied)
		}
		if err != nil {
			if w.res != nil {
				w.res.Failed++
			}
			return err
		}
		if w.res != nil {
			w.res.Applied++
		}
	}
	return nil
}

func (w safeCPSetWriter) classifyWriteError(
	err error,
	phase PhaseKind,
	hierarchyOperation HierarchyOperation,
	operation PlanOperation,
	resource, current, target string,
) error {
	if w.driver.Classify(err, hierarchyOperation) != HierarchyErrorStale {
		return phaseWriteError(err, phase, operation, resource, current, target)
	}
	return &PlanStaleError{
		Rel: operation.Rel, Direction: operation.Direction, Resource: resource,
		Current: current, Target: target, Err: err,
	}
}

func phaseWriteError(
	err error,
	phase PhaseKind,
	operation PlanOperation,
	resource, current, target string,
) error {
	return fmt.Errorf("phase write failed: phase=%s rel=%q direction=%s resource=%s current=%s target=%s: %w",
		phase, operation.Rel, operation.Direction, resource, current, target, err)
}

func (w safeCPSetWriter) scanStableShrinkChildren(
	ctx context.Context,
	operations []PlanOperation,
	capabilities HierarchyCapabilities,
) (map[string]stableLiveChildren, error) {
	driver := w.driver
	if wrapped, ok := driver.(*budgetedHierarchyDriver); !ok || wrapped.budget != w.budget {
		driver = NewBudgetedHierarchyDriver(driver, w.budget)
	}
	stableChildren := make(map[string]stableLiveChildren)
	for _, operation := range operations {
		if operation.Direction != WriteShrink {
			continue
		}
		skipCPUCheck := isConfiguredInheritanceClear(operation, capabilities)
		checkMems := operation.WriteMems
		if skipCPUCheck && !checkMems {
			continue
		}
		children, err := scanStableLiveChildren(ctx, driver, operation.Rel, checkMems)
		if err != nil {
			return nil, err
		}
		if !skipCPUCheck && !children.cpus.IsSubsetOf(operation.Target.CPUs) {
			return nil, &PlanStaleError{
				Rel: operation.Rel, Direction: operation.Direction, Resource: "child_union",
				Current: children.cpus.String(), Target: operation.Target.CPUs.String(),
				Err: fmt.Errorf("live child union is outside shrink target"),
			}
		}
		stableChildren[operation.Rel] = children
	}
	return stableChildren, nil
}

func scanStableLiveChildren(
	ctx context.Context,
	driver HierarchyDriver,
	rel string,
	readMems bool,
) (stableLiveChildren, error) {
	for {
		before, err := driver.ListChildren(ctx, rel)
		if err != nil {
			if driver.Classify(err, HierarchyOperationList) == HierarchyErrorStale {
				continue
			}
			return stableLiveChildren{}, err
		}
		childUnion := machine.NewCPUSet()
		childMemsUnion := machine.NewCPUSet()
		stale := false
		for _, child := range before {
			childRel := filepath.Join(rel, child.Name)
			entry, readErr := driver.ReadEntry(ctx, childRel)
			if readErr != nil {
				if driver.Classify(readErr, HierarchyOperationRead) == HierarchyErrorStale {
					stale = true
					break
				}
				return stableLiveChildren{}, readErr
			}
			if entry.Identity != child.Identity {
				stale = true
				break
			}
			if readMems {
				childMems, parseErr := machine.Parse(entry.Mems)
				if parseErr != nil {
					// Unparseable live child mems cannot prove a safe parent shrink, so fail-closed.
					return stableLiveChildren{}, fmt.Errorf("parse live child %q cpuset.mems=%q: %w",
						childRel, entry.Mems, parseErr)
				}
				childMemsUnion = childMemsUnion.Union(childMems)
			}
			childUnion = childUnion.Union(entry.CPUs)
		}
		if stale {
			continue
		}
		after, err := driver.ListChildren(ctx, rel)
		if err != nil {
			if driver.Classify(err, HierarchyOperationList) == HierarchyErrorStale {
				continue
			}
			return stableLiveChildren{}, err
		}
		if ChildrenFingerprint(before) != ChildrenFingerprint(after) {
			continue
		}
		return stableLiveChildren{cpus: childUnion, mems: childMemsUnion, refs: before}, nil
	}
}

func scanLiveChildrenOnce(
	ctx context.Context,
	driver HierarchyDriver,
	operation PlanOperation,
) (stableLiveChildren, error) {
	before, err := driver.ListChildren(ctx, operation.Rel)
	if err != nil {
		return stableLiveChildren{}, err
	}
	children := stableLiveChildren{
		cpus: machine.NewCPUSet(),
		mems: machine.NewCPUSet(),
		refs: before,
	}
	for _, child := range before {
		childRel := filepath.Join(operation.Rel, child.Name)
		entry, readErr := driver.ReadEntry(ctx, childRel)
		if readErr != nil {
			return stableLiveChildren{}, readErr
		}
		if entry.Identity != child.Identity {
			return stableLiveChildren{}, &PlanStaleError{
				Rel: operation.Rel, Direction: operation.Direction, Resource: "child_identity",
				Current: fmt.Sprint(entry.Identity), Target: fmt.Sprint(child.Identity),
				Err: fmt.Errorf("live child identity changed"),
			}
		}
		childMems, parseErr := machine.Parse(entry.Mems)
		if parseErr != nil {
			return stableLiveChildren{}, fmt.Errorf("parse live child %q cpuset.mems=%q: %w",
				childRel, entry.Mems, parseErr)
		}
		children.cpus = children.cpus.Union(entry.CPUs)
		children.mems = children.mems.Union(childMems)
	}
	after, err := driver.ListChildren(ctx, operation.Rel)
	if err != nil {
		return stableLiveChildren{}, err
	}
	if ChildrenFingerprint(before) != ChildrenFingerprint(after) {
		return stableLiveChildren{}, &PlanStaleError{
			Rel: operation.Rel, Direction: operation.Direction, Resource: "children",
			Current: ChildrenFingerprint(after), Target: ChildrenFingerprint(before),
			Err: fmt.Errorf("live children changed before shrink"),
		}
	}
	return children, nil
}

func (w safeCPSetWriter) precheckOperation(
	ctx context.Context,
	operation PlanOperation,
	stableChildUnion map[string]stableLiveChildren,
	capabilities HierarchyCapabilities,
) error {
	if operation.Direction != WriteShrink && operation.Direction != WriteGrow {
		return fmt.Errorf("unsupported plan operation direction %q", operation.Direction)
	}
	current, err := w.driver.ReadEntry(ctx, operation.Rel)
	if err != nil {
		return w.classifyHierarchyReadError(err, operation)
	}
	if current.Identity != operation.ExpectedIdentity {
		return fmt.Errorf("%w: planned operation rel=%q expected=%v current=%v",
			ErrCgroupIdentityChanged, operation.Rel, operation.ExpectedIdentity, current.Identity)
	}
	if err := validateLiveOperationDirection(operation, current, capabilities); err != nil {
		return err
	}
	if err := w.precheckOperationChildren(operation, stableChildUnion, capabilities); err != nil {
		return err
	}
	if operation.ParentRel != "" {
		parent, err := w.driver.ReadEntry(ctx, operation.ParentRel)
		if err != nil {
			return w.classifyHierarchyReadError(err, operation)
		}
		if parent.Identity != operation.ExpectedParentIdentity {
			return fmt.Errorf("%w: planned operation parent=%q expected=%v current=%v",
				ErrCgroupIdentityChanged, operation.ParentRel, operation.ExpectedParentIdentity, parent.Identity)
		}
		if operation.Direction == WriteGrow && !operation.Target.CPUs.IsSubsetOf(parent.CPUs) {
			return &PlanStaleError{
				Rel:       operation.Rel,
				Direction: operation.Direction,
				Resource:  "parent_cpuset.cpus",
				Current:   parent.CPUs.String(),
				Target:    operation.Target.CPUs.String(),
				Err: fmt.Errorf("planned grow target outside live parent %q",
					operation.ParentRel),
			}
		}
		if operation.Direction == WriteGrow && operation.WriteMems {
			parentMems, parentErr := machine.Parse(parent.Mems)
			targetMems, targetErr := machine.Parse(operation.Target.Mems)
			if parentErr != nil || targetErr != nil || !targetMems.IsSubsetOf(parentMems) {
				return &PlanStaleError{
					Rel: operation.Rel, Direction: operation.Direction, Resource: "parent_cpuset.mems",
					Current: parent.Mems, Target: operation.Target.Mems,
					Err: fmt.Errorf("planned mems grow target outside live parent %q: parent_parse=%v target_parse=%v",
						operation.ParentRel, parentErr, targetErr),
				}
			}
		}
	}
	return nil
}

func (w safeCPSetWriter) precheckOperationChildren(
	operation PlanOperation,
	stableChildUnion map[string]stableLiveChildren,
	capabilities HierarchyCapabilities,
) error {
	skipCPUCheck := isConfiguredInheritanceClear(operation, capabilities)
	if skipCPUCheck && !operation.WriteMems {
		// In v2, clearing configured only switches to inheritance without shrinking effective state; therefore,
		// a CPU-only clear skips CPU child containment checks, while concurrent mems writes still validate child mems.
		return nil
	}
	if operation.Direction == WriteShrink {
		children, ok := stableChildUnion[operation.Rel]
		if !ok {
			return fmt.Errorf("missing stable live child union for shrink rel=%q", operation.Rel)
		}
		if !skipCPUCheck && !children.cpus.IsSubsetOf(operation.Target.CPUs) {
			return &PlanStaleError{
				Rel: operation.Rel, Direction: operation.Direction, Resource: "child_union",
				Current: children.cpus.String(), Target: operation.Target.CPUs.String(),
				Err: fmt.Errorf("live child union is outside shrink target"),
			}
		}
		if operation.WriteMems {
			targetMems, parseErr := machine.Parse(operation.Target.Mems)
			if parseErr != nil || !children.mems.IsSubsetOf(targetMems) {
				return &PlanStaleError{
					Rel: operation.Rel, Direction: operation.Direction, Resource: "child_union_cpuset.mems",
					Current: children.mems.String(), Target: operation.Target.Mems,
					Err: fmt.Errorf("live child mems union is outside shrink target: target_parse=%v", parseErr),
				}
			}
		}
		return nil
	}
	return nil
}

func (w safeCPSetWriter) readAfterWrite(ctx context.Context, operation PlanOperation) (AppliedPlanOperation, error) {
	applied, err := w.readAppliedObservation(ctx, operation)
	if err != nil {
		return AppliedPlanOperation{}, err
	}
	current := applied.Observed
	if !current.CPUs.Equals(operation.Target.CPUs) {
		return applied, &PlanStaleError{
			Rel: operation.Rel, Direction: operation.Direction, Resource: "post_write_cpuset.cpus",
			Current: current.CPUs.String(), Target: operation.Target.CPUs.String(),
			Err: fmt.Errorf("post-write observation differs from target"),
		}
	}
	if operation.OwnsMems {
		currentMems, currentMemsErr := machine.Parse(current.Mems)
		targetMems, targetMemsErr := machine.Parse(operation.Target.Mems)
		if currentMemsErr != nil || targetMemsErr != nil || !currentMems.Equals(targetMems) {
			return applied, &PlanStaleError{
				Rel: operation.Rel, Direction: operation.Direction, Resource: "post_write_cpuset.mems",
				Current: current.Mems, Target: operation.Target.Mems,
				Err: fmt.Errorf("post-write mems observation differs from target: current_parse=%v target_parse=%v",
					currentMemsErr, targetMemsErr),
			}
		}
	}
	return applied, nil
}

func (w safeCPSetWriter) readAppliedObservation(ctx context.Context, operation PlanOperation) (AppliedPlanOperation, error) {
	current, err := w.driver.ReadEntry(ctx, operation.Rel)
	if err != nil {
		return AppliedPlanOperation{}, w.classifyHierarchyReadError(err, operation)
	}
	if current.Identity != operation.ExpectedIdentity {
		return AppliedPlanOperation{}, fmt.Errorf("%w: post-write rel=%q expected=%v current=%v",
			ErrCgroupIdentityChanged, operation.Rel, operation.ExpectedIdentity, current.Identity)
	}
	applied := AppliedPlanOperation{
		PlanID:    operation.PlanID,
		Rel:       operation.Rel,
		Direction: operation.Direction,
		Target:    operation.Target,
		Observed: CPUSetTarget{
			CPUs: observedCPUsForTargetProof(current, operation.Target.CPUs, w.driver.Capabilities()),
			Mems: current.Mems,
		},
	}
	return applied, nil
}

func (w safeCPSetWriter) classifyHierarchyReadError(err error, operation PlanOperation) error {
	if w.driver.Classify(err, HierarchyOperationRead) != HierarchyErrorStale {
		return err
	}
	return &PlanStaleError{
		Rel:       operation.Rel,
		Direction: operation.Direction,
		Resource:  "hierarchy",
		Current:   operation.ExpectedCurrent.CPUs.String(),
		Target:    operation.Target.CPUs.String(),
		Err:       err,
	}
}

func validateLiveOperationDirection(
	operation PlanOperation,
	current EntryState,
	capabilities HierarchyCapabilities,
) error {
	stale := func(resource, currentValue, targetValue string, err error) error {
		return &PlanStaleError{
			Rel: operation.Rel, Direction: operation.Direction, Resource: resource,
			Current: currentValue, Target: targetValue, Err: err,
		}
	}
	_, cpuChanged, _ := classifySetDirection(
		operation.ExpectedCurrent.CPUs, operation.Target.CPUs,
	)
	if !cpuChanged {
		plannedDirection, _, directionErr := combinedWriteDirection(
			operation.Rel,
			operation.ExpectedCurrent,
			operation.Target,
			operation.OwnsMems,
			false,
		)
		if directionErr != nil || plannedDirection != operation.Direction {
			if directionErr == nil {
				directionErr = fmt.Errorf("declared=%s calculated=%s", operation.Direction, plannedDirection)
			}
			return stale("write_direction", string(operation.Direction), string(plannedDirection),
				fmt.Errorf("operation direction disagrees with CPU/mems transition: %w", directionErr))
		}
	}
	currentCPUs := observedCPUsForTargetProof(current, operation.Target.CPUs, capabilities)
	memsDirectedClear := isConfiguredInheritanceClear(operation, capabilities) && operation.WriteMems
	if !memsDirectedClear {
		switch operation.Direction {
		case WriteGrow:
			if !operation.ExpectedCurrent.CPUs.IsSubsetOf(currentCPUs) ||
				!currentCPUs.IsSubsetOf(operation.Target.CPUs) {
				return stale("cpuset.cpus", currentCPUs.String(), operation.Target.CPUs.String(), nil)
			}
		case WriteShrink:
			if !operation.Target.CPUs.IsSubsetOf(currentCPUs) ||
				!currentCPUs.IsSubsetOf(operation.ExpectedCurrent.CPUs) {
				return stale("cpuset.cpus", currentCPUs.String(), operation.Target.CPUs.String(), nil)
			}
		default:
			return fmt.Errorf("unsupported plan operation direction %q", operation.Direction)
		}
	}

	if !operation.OwnsMems {
		return nil
	}
	currentMems, currentErr := machine.Parse(current.Mems)
	expectedMems, expectedErr := machine.Parse(operation.ExpectedCurrent.Mems)
	targetMems, targetErr := machine.Parse(operation.Target.Mems)
	if currentErr != nil || expectedErr != nil || targetErr != nil {
		return stale("cpuset.mems", current.Mems, operation.Target.Mems,
			fmt.Errorf("parse live/expected/target mems: current=%v expected=%v target=%v",
				currentErr, expectedErr, targetErr))
	}
	if expectedMems.Equals(targetMems) {
		if !currentMems.Equals(targetMems) {
			return stale("cpuset.mems", current.Mems, operation.Target.Mems, nil)
		}
		return nil
	}
	// Live mems must advance monotonically in the planner's final merged direction, never infer it independently.
	switch operation.Direction {
	case WriteGrow:
		if !expectedMems.IsSubsetOf(currentMems) || !currentMems.IsSubsetOf(targetMems) {
			return stale("cpuset.mems", current.Mems, operation.Target.Mems, nil)
		}
	case WriteShrink:
		if !targetMems.IsSubsetOf(currentMems) || !currentMems.IsSubsetOf(expectedMems) {
			return stale("cpuset.mems", current.Mems, operation.Target.Mems, nil)
		}
	default:
		return fmt.Errorf("unsupported plan operation direction %q", operation.Direction)
	}
	return nil
}
