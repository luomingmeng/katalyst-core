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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestPhysicalWriteCostUsesActualCPUAndMemsChanges(t *testing.T) {
	tests := []struct {
		name      string
		from, to  CPUSetTarget
		writeMems bool
		want      PhysicalWriteCost
	}{
		{
			name: "cpus only",
			from: CPUSetTarget{CPUs: machine.MustParse("0-1"), Mems: "0"},
			to:   CPUSetTarget{CPUs: machine.MustParse("0"), Mems: "0"},
			want: PhysicalWriteCost{CPUSetWrites: 1},
		},
		{
			name:      "mems only",
			from:      CPUSetTarget{CPUs: machine.MustParse("0"), Mems: "0-1"},
			to:        CPUSetTarget{CPUs: machine.MustParse("0"), Mems: "0"},
			writeMems: true,
			want:      PhysicalWriteCost{MemsWrites: 1},
		},
		{
			name:      "both",
			from:      CPUSetTarget{CPUs: machine.MustParse("0-1"), Mems: "0-1"},
			to:        CPUSetTarget{CPUs: machine.MustParse("0"), Mems: "0"},
			writeMems: true,
			want:      PhysicalWriteCost{CPUSetWrites: 1, MemsWrites: 1},
		},
		{
			name:      "neither",
			from:      CPUSetTarget{CPUs: machine.MustParse("0"), Mems: "0"},
			to:        CPUSetTarget{CPUs: machine.MustParse("0"), Mems: "0"},
			writeMems: true,
			want:      PhysicalWriteCost{},
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := physicalWriteCost(tc.from, tc.to, tc.writeMems); got != tc.want {
				t.Fatalf("physicalWriteCost() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestAdmissionReservationChargesPhysicalWrites(t *testing.T) {
	tests := []struct {
		name          string
		currentCPUs   machine.CPUSet
		targetCPUs    machine.CPUSet
		currentMems   string
		targetMems    string
		wantWriteCPUs int
		wantWriteMems int
		wantBudget    int
	}{
		{
			name:          "cpus",
			currentCPUs:   machine.NewCPUSet(0, 1),
			targetCPUs:    machine.NewCPUSet(0),
			currentMems:   "0",
			targetMems:    "0",
			wantWriteCPUs: 1,
			wantBudget:    2,
		},
		{
			name:          "mems",
			currentCPUs:   machine.NewCPUSet(0),
			targetCPUs:    machine.NewCPUSet(0),
			currentMems:   "0-1",
			targetMems:    "0",
			wantWriteMems: 1,
			wantBudget:    2,
		},
		{
			name:          "both",
			currentCPUs:   machine.NewCPUSet(0, 1),
			targetCPUs:    machine.NewCPUSet(0),
			currentMems:   "0-1",
			targetMems:    "0",
			wantWriteCPUs: 1,
			wantWriteMems: 1,
			wantBudget:    4,
		},
		{
			name:        "noop",
			currentCPUs: machine.NewCPUSet(0),
			targetCPUs:  machine.NewCPUSet(0),
			currentMems: "0",
			targetMems:  "0",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			plan, driver := admissionReservationPlanForTest(t, tc.currentCPUs, tc.targetCPUs, tc.currentMems, tc.targetMems)
			round := admissionReservationRoundForTest(driver, tc.wantBudget, plan.PlanID)

			if err := round.reserveAdmissionClosure(plan); err != nil {
				t.Fatalf("reserveAdmissionClosure() error = %v", err)
			}
			if err := round.executePlan(context.Background(), plan, &ConvergenceResult{}); err != nil {
				t.Fatalf("executePlan() error = %v", err)
			}
			if driver.writeCPUs != tc.wantWriteCPUs || driver.writeMems != tc.wantWriteMems {
				t.Fatalf("physical writes = CPUs:%d mems:%d, want CPUs:%d mems:%d",
					driver.writeCPUs, driver.writeMems, tc.wantWriteCPUs, tc.wantWriteMems)
			}
		})
	}
}

func TestAdmissionReservationRejectsBeforeAnyPhysicalWrite(t *testing.T) {
	tests := []struct {
		name        string
		currentCPUs machine.CPUSet
		targetCPUs  machine.CPUSet
		currentMems string
		targetMems  string
	}{
		{
			name:        "cpus-only budget one",
			currentCPUs: machine.NewCPUSet(0, 1),
			targetCPUs:  machine.NewCPUSet(0),
			currentMems: "0",
			targetMems:  "0",
		},
		{
			name:        "mems-only budget one",
			currentCPUs: machine.NewCPUSet(0),
			targetCPUs:  machine.NewCPUSet(0),
			currentMems: "0-1",
			targetMems:  "0",
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			plan, driver := admissionReservationPlanForTest(
				t,
				tc.currentCPUs,
				tc.targetCPUs,
				tc.currentMems,
				tc.targetMems,
			)
			round := admissionReservationRoundForTest(driver, 1, plan.PlanID)
			result := &ConvergenceResult{}

			err := round.reserveAdmissionClosure(plan)
			if !errors.Is(err, ErrAdmissionReservationExceeded) {
				t.Errorf("executePlan() error = %v, want ErrAdmissionReservationExceeded", err)
			}
			if driver.writeCPUs != 0 || driver.writeMems != 0 {
				t.Errorf("physical writes after rejected reservation = CPUs:%d mems:%d, want CPUs:0 mems:0",
					driver.writeCPUs, driver.writeMems)
			}
			if result.ParentSafe {
				t.Errorf("ParentSafe = true after rejected reservation, want false")
			}
		})
	}

	plan, driver := admissionReservationPlanForTest(
		t,
		machine.NewCPUSet(0, 1),
		machine.NewCPUSet(0),
		"0-1",
		"0",
	)
	required, deferred, err := SplitPlanForAdmission(&plan, AdmissionSafetyInput{
		ProtectedPendingCPUSet: machine.NewCPUSet(1),
	})
	if err != nil {
		t.Fatalf("SplitPlanForAdmission() error = %v", err)
	}
	if len(required.Operations) != 1 || len(deferred.Operations) != 0 {
		t.Fatalf("split operations = required:%d deferred:%d, want required:1 deferred:0",
			len(required.Operations), len(deferred.Operations))
	}

	round := admissionReservationRoundForTest(driver, 3, required.PlanID)
	result := &ConvergenceResult{}

	err = round.reserveAdmissionClosure(*required)
	if !errors.Is(err, ErrAdmissionReservationExceeded) {
		t.Errorf("executePlan() error = %v, want ErrAdmissionReservationExceeded", err)
	}
	if driver.writeCPUs != 0 || driver.writeMems != 0 {
		t.Errorf("physical writes after rejected reservation = CPUs:%d mems:%d, want CPUs:0 mems:0",
			driver.writeCPUs, driver.writeMems)
	}
	if result.ParentSafe {
		t.Errorf("ParentSafe = true after rejected reservation, want false")
	}
}

func TestAdmissionReservationLocksCanonicalClosureBeforeIntermediateDrain(t *testing.T) {
	canonical := machine.NewCPUSet(0, 1, 2, 3)
	intermediate := machine.NewCPUSet(0, 1)
	plan, driver := admissionReservationPlanForTest(
		t,
		canonical,
		intermediate,
		"0",
		"0",
	)
	plan.TargetByRel = map[string]CPUSetTarget{
		"root": {CPUs: canonical.Clone(), Mems: "0"},
	}
	plan.PlanID = canonicalExecutionPlanID(plan)
	plan.Operations[0].PlanID = plan.PlanID
	round := admissionReservationRoundForTest(driver, 3, plan.PlanID)

	err := round.reserveAdmissionClosure(plan)
	if !errors.Is(err, ErrAdmissionReservationExceeded) {
		t.Fatalf("executePlan() error = %v, want ErrAdmissionReservationExceeded", err)
	}
	if driver.writeCPUs != 0 || driver.writeMems != 0 {
		t.Fatalf("physical writes after rejected canonical closure = CPUs:%d mems:%d, want zero",
			driver.writeCPUs, driver.writeMems)
	}
}

func TestAdmissionReservationRejectsUnprovableRequiredClosureBeforeWrite(t *testing.T) {
	plan, driver := admissionReservationPlanForTest(
		t,
		machine.NewCPUSet(0, 1),
		machine.NewCPUSet(0),
		"0",
		"0",
	)
	round := admissionReservationRoundForTest(driver, 8, plan.PlanID)
	round.requiredByRel = map[string]machine.CPUSet{
		"root": machine.MustParse("0-2"),
	}

	err := round.reserveAdmissionClosure(plan)
	require.Error(t, err)
	require.ErrorContains(t, err, "required admission closure does not prove parent safety")
	require.Zero(t, driver.writeCPUs)
	require.Zero(t, driver.writeMems)
}

func TestAdmissionReservationIsCumulativeAcrossPlans(t *testing.T) {
	first, driver := admissionReservationPlanForTest(
		t,
		machine.NewCPUSet(0, 1),
		machine.NewCPUSet(0),
		"0-1",
		"0-1",
	)
	round := admissionReservationRoundForTest(driver, 2, first.PlanID)
	result := &ConvergenceResult{}

	if err := round.reserveAdmissionClosure(first); err != nil {
		t.Fatalf("reserveAdmissionClosure() error = %v", err)
	}
	if err := round.executePlan(context.Background(), first, result); err != nil {
		t.Fatalf("first executePlan() error = %v", err)
	}

	second, _ := admissionReservationPlanForTest(
		t,
		machine.NewCPUSet(0),
		machine.NewCPUSet(0),
		"0-1",
		"0",
	)
	second.Base.Entries["root"] = driver.entry
	second.Operations[0].ExpectedIdentity = driver.entry.Identity
	second.Operations[0].ExpectedCurrent = CPUSetTarget{
		CPUs: driver.entry.CPUs.Clone(),
		Mems: driver.entry.Mems,
	}
	second.PlanID = canonicalExecutionPlanID(second)
	second.Operations[0].PlanID = second.PlanID
	round.planID = second.PlanID

	err := round.executePlan(context.Background(), second, result)
	if !errors.Is(err, ErrAdmissionReservationExceeded) {
		t.Fatalf("second executePlan() error = %v, want ErrAdmissionReservationExceeded", err)
	}
	if driver.writeCPUs != 1 {
		t.Fatalf("physical CPU writes = %d, want 1", driver.writeCPUs)
	}
	if driver.writeMems != 0 {
		t.Fatalf("physical mems writes = %d, want 0", driver.writeMems)
	}
}

func TestAdmissionReservationAllowsRuntimeTargetRebaseUnderStableCanonicalTarget(t *testing.T) {
	plan, _ := admissionReservationPlanForTest(
		t,
		machine.NewCPUSet(0, 1),
		machine.NewCPUSet(0, 1),
		"0",
		"0",
	)
	plan.CanonicalTargetByRel = map[string]CPUSetTarget{
		"root": {CPUs: machine.NewCPUSet(0, 1), Mems: "0"},
	}
	plan.TargetByRel = map[string]CPUSetTarget{
		"root":    {CPUs: machine.NewCPUSet(0, 1), Mems: "0"},
		"runtime": {CPUs: machine.NewCPUSet(0), Mems: "0"},
	}
	ticket, err := NewBudgetTracker(ConvergenceBudget{}).
		ReserveAdmissionBudget(plan, nil, 0)
	require.NoError(t, err)

	rebased := plan
	rebased.TargetByRel = map[string]CPUSetTarget{
		"root":    {CPUs: machine.NewCPUSet(0, 1), Mems: "0"},
		"runtime": {CPUs: machine.NewCPUSet(0, 1), Mems: "0"},
	}
	require.NoError(t, ticket.consume(rebased))
}

func TestAdmissionReservationReportsRequestedAndAuthorizedSignatures(t *testing.T) {
	plan, _ := admissionReservationPlanForTest(
		t,
		machine.NewCPUSet(0, 1),
		machine.NewCPUSet(0),
		"0",
		"0",
	)
	ticket, err := NewBudgetTracker(ConvergenceBudget{}).
		ReserveAdmissionBudget(plan, nil, 0)
	require.NoError(t, err)

	plan.Operations[0].Target.CPUs = machine.NewCPUSet(1)
	err = ticket.consume(plan)
	require.ErrorContains(t, err, "requested=")
	require.ErrorContains(t, err, "authorizedForRel=")
}

func TestAdmissionReservationRollsBackMemsWhenCPUWriteFails(t *testing.T) {
	plan, driver := admissionReservationPlanForTest(
		t,
		machine.NewCPUSet(0, 1),
		machine.NewCPUSet(0),
		"0-1",
		"0",
	)
	driver.failWriteCPUs = true
	round := admissionReservationRoundForTest(driver, 4, plan.PlanID)

	require.NoError(t, round.reserveAdmissionClosure(plan))
	err := round.executePlan(context.Background(), plan, &ConvergenceResult{})
	if err == nil {
		t.Fatal("executePlan() error = nil, want CPU write failure")
	}
	if driver.entry.Mems != "0-1" {
		t.Fatalf("mems after failed CPU write = %q, want rollback to %q", driver.entry.Mems, "0-1")
	}
	if driver.writeMems != 2 {
		t.Fatalf("physical mems writes = %d, want forward plus rollback", driver.writeMems)
	}
}

func TestAdmissionReservationRollsBackBothWritesWhenFreshReadBackFails(t *testing.T) {
	plan, driver := admissionReservationPlanForTest(
		t,
		machine.NewCPUSet(0, 1),
		machine.NewCPUSet(0),
		"0-1",
		"0",
	)
	driver.failReadAt = 2
	round := admissionReservationRoundForTest(driver, 4, plan.PlanID)

	require.NoError(t, round.reserveAdmissionClosure(plan))
	err := round.executePlan(context.Background(), plan, &ConvergenceResult{})
	if err == nil {
		t.Fatal("executePlan() error = nil, want fresh read-back failure")
	}
	if !driver.entry.CPUs.Equals(machine.NewCPUSet(0, 1)) || driver.entry.Mems != "0-1" {
		t.Fatalf("entry after failed read-back = cpus:%s mems:%q, want cpus:0-1 mems:%q",
			driver.entry.CPUs.String(), driver.entry.Mems, "0-1")
	}
	if driver.writeCPUs != 2 || driver.writeMems != 2 {
		t.Fatalf("physical writes = CPUs:%d mems:%d, want CPUs:2 mems:2",
			driver.writeCPUs, driver.writeMems)
	}
}

func TestAdmissionReservationRejectsUnexpectedPhysicalDriftBeforeWrite(t *testing.T) {
	plan, driver := admissionReservationPlanForTest(
		t,
		machine.NewCPUSet(0, 1),
		machine.NewCPUSet(0),
		"0",
		"0",
	)
	round := admissionReservationRoundForTest(driver, 2, plan.PlanID)
	require.NoError(t, round.reserveAdmissionClosure(plan))
	driver.entry.CPUs = machine.MustParse("0-2")

	err := round.executePlan(context.Background(), plan, &ConvergenceResult{})
	var stale *PlanStaleError
	require.ErrorAs(t, err, &stale)
	require.Zero(t, driver.writeCPUs)
	require.Zero(t, round.admissionTicket.consumedForward.CPUSetWrites)

	driver.entry.CPUs = machine.NewCPUSet(0, 1)
	err = round.executePlan(context.Background(), plan, &ConvergenceResult{})
	require.NoError(t, err)
	require.Equal(t, 1, driver.writeCPUs)
}

func TestAdmissionReservationRejectsFalseSuccessAndRollsBack(t *testing.T) {
	plan, driver := admissionReservationPlanForTest(
		t,
		machine.NewCPUSet(0, 1),
		machine.NewCPUSet(0),
		"0",
		"0",
	)
	driver.ignoreCPUWrite = true
	round := admissionReservationRoundForTest(driver, 2, plan.PlanID)

	require.NoError(t, round.reserveAdmissionClosure(plan))
	err := round.executePlan(context.Background(), plan, &ConvergenceResult{})
	if err == nil {
		t.Fatal("executePlan() error = nil, want fresh read-back mismatch")
	}
	require.True(t, driver.entry.CPUs.Equals(machine.NewCPUSet(0, 1)))
	require.Equal(t, PhysicalWriteCost{CPUSetWrites: 1}, round.admissionTicket.consumedForward)
	require.Equal(t, PhysicalWriteCost{CPUSetWrites: 1}, round.admissionTicket.consumedRollback)
}

func TestAdmissionReservationReportsRollbackWriteFailure(t *testing.T) {
	plan, driver := admissionReservationPlanForTest(
		t,
		machine.NewCPUSet(0, 1),
		machine.NewCPUSet(0),
		"0-1",
		"0",
	)
	driver.failWriteCPUs = true
	driver.failWriteMemsAt = 2
	round := admissionReservationRoundForTest(driver, 4, plan.PlanID)

	require.NoError(t, round.reserveAdmissionClosure(plan))
	err := round.executePlan(context.Background(), plan, &ConvergenceResult{})
	require.Error(t, err)
	require.ErrorContains(t, err, "injected CPU write failure")
	require.ErrorContains(t, err, "rollback cpuset.mems")
	require.Equal(t, PhysicalWriteCost{CPUSetWrites: 1, MemsWrites: 1},
		round.admissionTicket.consumedForward)
	require.Equal(t, PhysicalWriteCost{MemsWrites: 1},
		round.admissionTicket.consumedRollback)
}

func admissionReservationPlanForTest(
	t *testing.T,
	currentCPUs, targetCPUs machine.CPUSet,
	currentMems, targetMems string,
) (PhasePlan, *admissionReservationDriver) {
	t.Helper()

	identity := CgroupIdentity{Device: 1, Inode: 1}
	driver := &admissionReservationDriver{entry: EntryState{
		Rel: "root", Identity: identity,
		CPUs: currentCPUs.Clone(), ConfiguredCPUs: currentCPUs.Clone(),
		Mems: currentMems, ConfiguredMems: currentMems,
	}}
	plan := PhasePlan{
		ConvergenceID: "admission-reservation",
		Kind:          PhaseExpand,
		Base: &CompleteSnapshot{
			Entries:  map[string]EntryState{"root": driver.entry},
			Children: map[string][]ChildRef{"root": nil},
			DomainUnion: map[DomainID]machine.CPUSet{
				DomainPrimary: currentCPUs.Clone(),
			},
		},
		Capabilities: driver.Capabilities(),
	}
	if currentCPUs.Equals(targetCPUs) && currentMems == targetMems {
		plan.PlanID = canonicalExecutionPlanID(plan)
		return plan, driver
	}

	direction, _, err := combinedWriteDirection(
		"root",
		CPUSetTarget{CPUs: currentCPUs, Mems: currentMems},
		CPUSetTarget{CPUs: targetCPUs, Mems: targetMems},
		true,
		false,
	)
	if err != nil {
		t.Fatalf("combinedWriteDirection() error = %v", err)
	}
	plan.Operations = []PlanOperation{{
		Rel:              "root",
		ExpectedIdentity: identity,
		ExpectedChildren: ChildrenFingerprint(nil),
		ExpectedCurrent:  CPUSetTarget{CPUs: currentCPUs.Clone(), Mems: currentMems},
		Target:           CPUSetTarget{CPUs: targetCPUs.Clone(), Mems: targetMems},
		Direction:        direction,
		OwnsMems:         true,
		WriteMems:        currentMems != targetMems,
	}}
	plan.PlanID = canonicalExecutionPlanID(plan)
	plan.Operations[0].PlanID = plan.PlanID
	return plan, driver
}

func admissionReservationRoundForTest(
	driver HierarchyDriver,
	maxRequiredWrites int,
	planID string,
) *coordinatorRound {
	return &coordinatorRound{
		objective:       ConvergenceObjectiveParentSafe,
		admissionBudget: &AdmissionConvergenceBudget{MaxRequiredWrites: maxRequiredWrites},
		driver:          driver,
		budget:          NewBudgetTracker(ConvergenceBudget{}),
		planID:          planID,
	}
}

type admissionReservationDriver struct {
	entry                 EntryState
	writeCPUs, writeMems  int
	failWriteCPUs         bool
	readCount, failReadAt int
	failWriteMemsAt       int
	ignoreCPUWrite        bool
}

func (d *admissionReservationDriver) Close() error { return nil }

func (d *admissionReservationDriver) Roots(context.Context) ([]RootRef, error) {
	return []RootRef{{Rel: d.entry.Rel, Identity: d.entry.Identity}}, nil
}

func (d *admissionReservationDriver) StatIdentity(_ context.Context, rel string) (CgroupIdentity, error) {
	if rel != d.entry.Rel {
		return CgroupIdentity{}, fmt.Errorf("unknown rel %q", rel)
	}
	return d.entry.Identity, nil
}

func (d *admissionReservationDriver) ReadEntry(_ context.Context, rel string) (EntryState, error) {
	if rel != d.entry.Rel {
		return EntryState{}, fmt.Errorf("unknown rel %q", rel)
	}
	d.readCount++
	if d.failReadAt > 0 && d.readCount == d.failReadAt {
		return EntryState{}, errors.New("injected read-back failure")
	}
	entry := d.entry
	entry.CPUs = entry.CPUs.Clone()
	entry.ConfiguredCPUs = entry.ConfiguredCPUs.Clone()
	return entry, nil
}

func (d *admissionReservationDriver) ListChildren(context.Context, string) ([]ChildRef, error) {
	return nil, nil
}

func (d *admissionReservationDriver) WriteCPUs(
	_ context.Context,
	rel string,
	expected CgroupIdentity,
	cpus machine.CPUSet,
) error {
	if rel != d.entry.Rel || expected != d.entry.Identity {
		return ErrCgroupIdentityChanged
	}
	d.writeCPUs++
	if d.failWriteCPUs {
		return errors.New("injected CPU write failure")
	}
	if !d.ignoreCPUWrite {
		d.entry.CPUs = cpus.Clone()
	}
	d.entry.ConfiguredCPUs = cpus.Clone()
	return nil
}

func (d *admissionReservationDriver) WriteMems(
	_ context.Context,
	rel string,
	expected CgroupIdentity,
	mems string,
) error {
	if rel != d.entry.Rel || expected != d.entry.Identity {
		return ErrCgroupIdentityChanged
	}
	d.writeMems++
	if d.failWriteMemsAt > 0 && d.writeMems == d.failWriteMemsAt {
		return errors.New("injected mems write failure")
	}
	d.entry.Mems = mems
	d.entry.ConfiguredMems = mems
	return nil
}

func (d *admissionReservationDriver) Classify(err error, _ HierarchyOperation) HierarchyErrorClass {
	if err == nil {
		return HierarchyErrorNone
	}
	return HierarchyErrorInvalid
}

func (d *admissionReservationDriver) Capabilities() HierarchyCapabilities {
	return HierarchyCapabilities{
		StableIdentity:          true,
		KernelParentContainment: true,
	}
}
