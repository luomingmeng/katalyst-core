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
	"strings"
	"sync"

	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type AdmissionBudgetTicket struct {
	mu               sync.Mutex
	canonicalTargets map[string]CPUSetTarget
	closure          admissionRequiredOperationClosure
	reserved         AdmissionReservationCost
	consumedForward  PhysicalWriteCost
	consumedRollback PhysicalWriteCost
	authorized       map[string]int
	released         bool
}

func (b *BudgetTracker) ReserveAdmissionBudget(
	plan PhasePlan,
	requiredByRel map[string]machine.CPUSet,
	limit int,
) (*AdmissionBudgetTicket, error) {
	if b == nil {
		return nil, fmt.Errorf("admission reservation requires budget tracker")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	closure, err := proveAdmissionRequiredClosure(plan, requiredByRel)
	if err != nil {
		return nil, err
	}
	cost := admissionRequiredClosurePhysicalWriteCost(closure)
	total := saturatingAdd(cost.Forward.Total(), cost.Rollback.Total())
	if limit > 0 && total > limit {
		return nil, fmt.Errorf("%w before safety closure: limit=%d required=%d",
			ErrAdmissionReservationExceeded, limit, total)
	}
	general.InfoS("admission required closure reserved",
		"requiredCPUSetByRel", stringifyCPUSetMap(requiredByRel),
		"observedCPUSetByRel", snapshotCPUSetByRel(plan.Base),
		"requiredFloorDeficit", stringifyCPUSetMap(closure.FinalReport.RequiredFloorDeficit),
		"drainOperationCount", len(closure.DrainOperations),
		"expandOperationCount", len(closure.ExpandOperations),
		"reservedCPUSetWrites", cost.Forward.CPUSetWrites+cost.Rollback.CPUSetWrites,
		"reservedMemsWrites", cost.Forward.MemsWrites+cost.Rollback.MemsWrites)
	return &AdmissionBudgetTicket{
		canonicalTargets: frozenCanonicalAdmissionTarget(plan),
		closure:          closure,
		reserved:         cost,
		authorized:       admissionClosureOperationCounts(closure),
	}, nil
}

func stringifyCPUSetMap(in map[string]machine.CPUSet) map[string]string {
	out := make(map[string]string, len(in))
	for rel, cpus := range in {
		out[rel] = cpus.String()
	}
	return out
}

func snapshotCPUSetByRel(snapshot *CompleteSnapshot) map[string]string {
	if snapshot == nil {
		return nil
	}
	out := make(map[string]string, len(snapshot.Entries))
	for rel, entry := range snapshot.Entries {
		out[rel] = entry.CPUs.String()
	}
	return out
}

func (t *AdmissionBudgetTicket) consume(plan PhasePlan) error {
	if t == nil {
		return fmt.Errorf("admission reservation ticket is nil")
	}
	if !cpuSetTargetsEqual(t.canonicalTargets, frozenCanonicalAdmissionTarget(plan)) {
		return fmt.Errorf("%w: canonical admission target changed after ticket lock",
			ErrAdmissionReservationExceeded)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.released {
		return fmt.Errorf("%w: ticket already released", ErrAdmissionReservationExceeded)
	}
	requested := make(map[string]int)
	for _, operation := range plan.Operations {
		signature := admissionOperationSignature(operation)
		requested[signature]++
		if requested[signature] > t.authorized[signature] {
			return fmt.Errorf(
				"%w: operation %q is outside proved closure: requested=%q authorizedForRel=%q",
				ErrAdmissionReservationExceeded, operation.Rel, signature,
				authorizedAdmissionOperationSignaturesForRel(t.authorized, operation.Rel))
		}
	}
	required := admissionPlanPhysicalWriteCost(plan)
	remaining := subtractPhysicalWriteCost(t.reserved.Forward, t.consumedForward)
	if !physicalWriteCostFits(required, remaining) {
		return fmt.Errorf("%w: reserved=%d remaining=%d requested=%d",
			ErrAdmissionReservationExceeded, t.reserved.Forward.Total(),
			remaining.Total(), required.Total())
	}
	return nil
}

func (t *AdmissionBudgetTicket) consumeOperation(operation PlanOperation) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.released {
		return fmt.Errorf("%w: ticket already released", ErrAdmissionReservationExceeded)
	}
	signature := admissionOperationSignature(operation)
	if t.authorized[signature] <= 0 {
		return fmt.Errorf(
			"%w: operation %q is outside remaining proved closure: requested=%q authorizedForRel=%q",
			ErrAdmissionReservationExceeded, operation.Rel, signature,
			authorizedAdmissionOperationSignaturesForRel(t.authorized, operation.Rel))
	}
	t.authorized[signature]--
	return nil
}

func admissionClosureOperationCounts(closure admissionRequiredOperationClosure) map[string]int {
	out := make(map[string]int)
	operations := append([]PlanOperation(nil), closure.DrainOperations...)
	operations = append(operations, closure.ExpandOperations...)
	for _, operation := range operations {
		out[admissionOperationSignature(operation)]++
	}
	return out
}

func admissionOperationSignature(operation PlanOperation) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s|%t",
		operation.Rel, operation.Direction,
		operation.ExpectedCurrent.CPUs.String(), operation.ExpectedCurrent.Mems,
		operation.Target.CPUs.String(), operation.Target.Mems, operation.WriteMems)
}

func authorizedAdmissionOperationSignaturesForRel(
	authorized map[string]int,
	rel string,
) []string {
	prefix := rel + "|"
	out := make([]string, 0)
	for signature, remaining := range authorized {
		if remaining > 0 && strings.HasPrefix(signature, prefix) {
			out = append(out, fmt.Sprintf("%s#%d", signature, remaining))
		}
	}
	sort.Strings(out)
	return out
}

func admissionRequiredClosurePhysicalWriteCost(
	closure admissionRequiredOperationClosure,
) AdmissionReservationCost {
	forward := PhysicalWriteCost{}
	operations := append([]PlanOperation(nil), closure.DrainOperations...)
	operations = append(operations, closure.ExpandOperations...)
	for _, operation := range operations {
		forward = addPhysicalWriteCost(forward, physicalWriteCost(
			operation.ExpectedCurrent, operation.Target, operation.WriteMems))
	}
	return AdmissionReservationCost{Forward: forward, Rollback: forward}
}

func admissionPlanPhysicalWriteCost(plan PhasePlan) PhysicalWriteCost {
	total := PhysicalWriteCost{}
	for _, operation := range plan.Operations {
		total = addPhysicalWriteCost(total, physicalWriteCost(
			operation.ExpectedCurrent, operation.Target, operation.WriteMems))
	}
	return total
}

func physicalWriteCost(from, to CPUSetTarget, writeMems bool) PhysicalWriteCost {
	cost := PhysicalWriteCost{}
	if !from.CPUs.Equals(to.CPUs) {
		cost.CPUSetWrites = 1
	}
	if writeMems && from.Mems != to.Mems {
		cost.MemsWrites = 1
	}
	return cost
}

func addPhysicalWriteCost(left, right PhysicalWriteCost) PhysicalWriteCost {
	return PhysicalWriteCost{
		CPUSetWrites: saturatingAdd(left.CPUSetWrites, right.CPUSetWrites),
		MemsWrites:   saturatingAdd(left.MemsWrites, right.MemsWrites),
	}
}

func subtractPhysicalWriteCost(left, right PhysicalWriteCost) PhysicalWriteCost {
	return PhysicalWriteCost{
		CPUSetWrites: left.CPUSetWrites - right.CPUSetWrites,
		MemsWrites:   left.MemsWrites - right.MemsWrites,
	}
}

func physicalWriteCostFits(required, available PhysicalWriteCost) bool {
	return required.CPUSetWrites <= available.CPUSetWrites &&
		required.MemsWrites <= available.MemsWrites
}

func (t *AdmissionBudgetTicket) consumeForward(cost PhysicalWriteCost) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.released {
		return fmt.Errorf("%w: ticket already released", ErrAdmissionReservationExceeded)
	}
	remaining := subtractPhysicalWriteCost(t.reserved.Forward, t.consumedForward)
	if !physicalWriteCostFits(cost, remaining) {
		return fmt.Errorf("%w: forward remaining=%+v requested=%+v",
			ErrAdmissionReservationExceeded, remaining, cost)
	}
	t.consumedForward = addPhysicalWriteCost(t.consumedForward, cost)
	general.InfoS("admission required closure forward write consumed",
		"consumedCPUSetWrites", t.consumedForward.CPUSetWrites,
		"consumedMemsWrites", t.consumedForward.MemsWrites,
		"reservedCPUSetWrites", t.reserved.Forward.CPUSetWrites,
		"reservedMemsWrites", t.reserved.Forward.MemsWrites)
	return nil
}

func (t *AdmissionBudgetTicket) consumeRollback(cost PhysicalWriteCost) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.released {
		return fmt.Errorf("%w: ticket already released", ErrAdmissionReservationExceeded)
	}
	remaining := subtractPhysicalWriteCost(t.reserved.Rollback, t.consumedRollback)
	if !physicalWriteCostFits(cost, remaining) {
		return fmt.Errorf("%w: rollback remaining=%+v requested=%+v",
			ErrAdmissionReservationExceeded, remaining, cost)
	}
	t.consumedRollback = addPhysicalWriteCost(t.consumedRollback, cost)
	general.InfoS("admission required closure rollback write consumed",
		"consumedCPUSetWrites", t.consumedRollback.CPUSetWrites,
		"consumedMemsWrites", t.consumedRollback.MemsWrites,
		"reservedCPUSetWrites", t.reserved.Rollback.CPUSetWrites,
		"reservedMemsWrites", t.reserved.Rollback.MemsWrites)
	return nil
}

func (t *AdmissionBudgetTicket) forwardExhausted() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.consumedForward == t.reserved.Forward
}

func (t *AdmissionBudgetTicket) hasPhysicalConsumption() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.consumedForward.Total() > 0 || t.consumedRollback.Total() > 0
}

func (t *AdmissionBudgetTicket) ReleaseUnused() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.released = true
}

func cloneCPUSetTarget(in CPUSetTarget) CPUSetTarget {
	return CPUSetTarget{CPUs: in.CPUs.Clone(), Mems: in.Mems}
}

func frozenCanonicalAdmissionTarget(plan PhasePlan) map[string]CPUSetTarget {
	out := make(map[string]CPUSetTarget, len(plan.CanonicalTargetByRel))
	for rel, target := range plan.CanonicalTargetByRel {
		out[rel] = cloneCPUSetTarget(target)
	}
	return out
}

func cpuSetTargetsEqual(left, right map[string]CPUSetTarget) bool {
	if len(left) != len(right) {
		return false
	}
	for rel, leftTarget := range left {
		rightTarget, ok := right[rel]
		if !ok || leftTarget.Mems != rightTarget.Mems || !leftTarget.CPUs.Equals(rightTarget.CPUs) {
			return false
		}
	}
	return true
}
