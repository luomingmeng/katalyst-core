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
	"path/filepath"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

const maxResetEnforceDepth = 8

type resetCoordinatorWriter struct {
	driver      HierarchyDriver
	budget      *BudgetTracker
	defaultMems string
	res         *ConvergenceResult
}

func newResetCoordinatorWriter(driver HierarchyDriver, budget *BudgetTracker, defaultMems string, res *ConvergenceResult) resetCoordinatorWriter {
	return resetCoordinatorWriter{driver: driver, budget: budget, defaultMems: defaultMems, res: res}
}

func (w resetCoordinatorWriter) execute(
	ctx context.Context,
	dag *TopoDAG,
	targets map[string]machine.CPUSet,
	allowEmptyTarget bool,
	expected map[string]machine.CPUSet,
	boundarySets ...map[string]struct{},
) error {
	if w.driver == nil {
		return fmt.Errorf("reset writer requires hierarchy driver")
	}
	if w.budget == nil {
		return fmt.Errorf("reset writer requires convergence budget")
	}
	controlled := map[string]*TopoNode{}
	for _, n := range dag.Nodes() {
		controlled[n.Rel] = n
	}
	var boundaries map[string]struct{}
	if len(boundarySets) > 0 {
		boundaries = boundarySets[0]
	}
	var firstErr error
	_ = dag.ForEachExpand(func(n *TopoNode) error {
		target := targets[n.Rel]
		if target.IsEmpty() && !allowEmptyTarget {
			if w.res != nil {
				w.res.Skipped++
			}
			return nil
		}
		localErr := w.writeResetNode(ctx, n, target)
		var propagateErr error
		w.propagateResetTarget(ctx, n.Rel, target, controlled, expected, boundaries, &propagateErr, 0)
		if n.Role == TopoNodeRoleReclaimNUMABucket && localErr != nil {
			if err := w.writeResetNode(ctx, n, target); err == nil {
				localErr = nil
			}
		}
		if firstErr == nil && localErr != nil {
			firstErr = localErr
		}
		if firstErr == nil && propagateErr != nil {
			firstErr = propagateErr
		}
		return nil
	})
	return firstErr
}

func (w resetCoordinatorWriter) writeResetNode(ctx context.Context, node *TopoNode, target machine.CPUSet) error {
	if node == nil {
		return nil
	}
	parentRel := ""
	if parent := parentNodeOf(node); parent != nil {
		parentRel = parent.Rel
	}
	return w.writeResetRel(ctx, node.Rel, parentRel, target, memsForNode(node, w.defaultMems))
}

func (w resetCoordinatorWriter) writeResetRel(ctx context.Context, rel, parentRel string, target machine.CPUSet, mems string) error {
	if w.res != nil {
		w.res.Attempted++
	}
	operations := 2 // ReadEntry(rel) + WriteCPUs(rel).
	if parentRel != "" {
		operations += 2 // ReadEntry(parentRel) before and after.
	}
	if mems != "" {
		operations++ // WriteMems(rel).
	}
	driver, err := newReservedBudgetedHierarchyDriver(ctx, w.driver, w.budget, operations)
	if err != nil {
		return err
	}
	current, err := driver.ReadEntry(ctx, rel)
	if err != nil {
		if w.res != nil {
			w.res.Failed++
		}
		return err
	}
	if parentRel != "" {
		parentBefore, err := driver.ReadEntry(ctx, parentRel)
		if err != nil {
			if w.res != nil {
				w.res.Failed++
			}
			return err
		}
		parentAfter, err := driver.ReadEntry(ctx, parentRel)
		if err != nil {
			if w.res != nil {
				w.res.Failed++
			}
			return err
		}
		if parentAfter.Identity != parentBefore.Identity {
			if w.res != nil {
				w.res.Failed++
			}
			return fmt.Errorf("%w: reset parent=%q expected=%v current=%v",
				ErrCgroupIdentityChanged, parentRel, parentBefore.Identity, parentAfter.Identity)
		}
	}
	if mems != "" {
		if err := driver.WriteMems(ctx, rel, current.Identity, mems); err != nil {
			if w.res != nil {
				w.res.Failed++
			}
			return err
		}
	}
	if err := driver.WriteCPUs(ctx, rel, current.Identity, target); err != nil {
		if w.res != nil {
			w.res.Failed++
		}
		return err
	}
	if w.res != nil {
		w.res.Applied++
	}
	return nil
}

func (w resetCoordinatorWriter) propagateResetTarget(
	ctx context.Context,
	parentRel string,
	parentTarget machine.CPUSet,
	controlled map[string]*TopoNode,
	expected map[string]machine.CPUSet,
	boundaries map[string]struct{},
	firstErr *error,
	depth int,
) {
	if depth > maxResetEnforceDepth {
		if w.res != nil {
			w.res.Skipped++
		}
		return
	}
	driver, err := newReservedBudgetedHierarchyDriver(ctx, w.driver, w.budget, 1)
	if err != nil {
		if *firstErr == nil {
			*firstErr = err
		}
		return
	}
	children, err := driver.ListChildren(ctx, parentRel)
	if err != nil {
		if *firstErr == nil {
			*firstErr = err
		}
		return
	}
	for _, child := range children {
		childRel := filepath.Join(parentRel, child.Name)
		if withinTraversalBoundary(childRel, boundaries) {
			continue
		}
		if _, ok := controlled[childRel]; ok {
			continue
		}
		target, hasExpected := expected[childRel]
		if !hasExpected {
			target = parentTarget
		}
		mems := ""
		if parentNode := controlled[parentRel]; parentNode != nil && parentNode.Role == TopoNodeRoleReclaimNUMABucket {
			mems = memsForNode(parentNode, w.defaultMems)
		}
		if err := w.writeResetRel(ctx, childRel, parentRel, target, mems); err != nil {
			if w.shouldSkipDynamicUnavailableController(err, hasExpected) {
				w.markDynamicResetSkip()
				continue
			}
			if *firstErr == nil {
				*firstErr = err
			}
			continue
		}
		w.propagateResetTarget(ctx, childRel, target, controlled, expected, boundaries, firstErr, depth+1)
	}
}

func withinTraversalBoundary(rel string, boundaries map[string]struct{}) bool {
	rel = filepath.Clean(rel)
	for boundary := range boundaries {
		boundary = filepath.Clean(boundary)
		if rel == boundary || (boundary != "." && len(rel) > len(boundary) &&
			rel[:len(boundary)] == boundary && rel[len(boundary)] == filepath.Separator) {
			return true
		}
	}
	return false
}

func (w resetCoordinatorWriter) shouldSkipDynamicUnavailableController(err error, hasExpected bool) bool {
	return !hasExpected && errors.Is(err, ErrCgroupControllerUnavailable)
}

func (w resetCoordinatorWriter) markDynamicResetSkip() {
	if w.res == nil {
		return
	}
	w.res.Skipped++
	if w.res.Failed > 0 {
		w.res.Failed--
	}
}
