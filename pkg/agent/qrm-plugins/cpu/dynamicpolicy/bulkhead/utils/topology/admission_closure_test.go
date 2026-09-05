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
	"testing"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestProveAdmissionRequiredClosureIncludesDrainExpandAndAncestor(t *testing.T) {
	rootID := CgroupIdentity{Device: 1, Inode: 1}
	sourceID := CgroupIdentity{Device: 1, Inode: 2}
	reclaimID := CgroupIdentity{Device: 1, Inode: 3}
	snapshot := planSnapshot(map[string]EntryState{
		"root": {
			Rel: "root", Identity: rootID, CPUs: machine.MustParse("0-3"), Mems: "0",
		},
		"root/source": {
			Rel: "root/source", Identity: sourceID, CPUs: machine.MustParse("2-3"), Mems: "0",
		},
		"root/reclaim": {
			Rel: "root/reclaim", Identity: reclaimID, CPUs: machine.MustParse("0-1"), Mems: "0",
		},
	}, map[DomainID]machine.CPUSet{
		DomainPrimary: machine.MustParse("2-3"),
		DomainReclaim: machine.MustParse("0-1"),
	})
	snapshot.DomainByRel = map[string]DomainID{
		"root/source":  DomainPrimary,
		"root/reclaim": DomainReclaim,
	}
	snapshot.Children = map[string][]ChildRef{
		"root": {
			{Name: "source", Identity: sourceID},
			{Name: "reclaim", Identity: reclaimID},
		},
	}
	plan := PhasePlan{
		Base: snapshot,
		Kind: PhaseDrain,
		TargetByRel: map[string]CPUSetTarget{
			"root":         {CPUs: machine.MustParse("0-5"), Mems: "0"},
			"root/source":  {CPUs: machine.NewCPUSet(), Mems: "0"},
			"root/reclaim": {CPUs: machine.MustParse("0-5"), Mems: "0"},
		},
		Operations: []PlanOperation{{
			Rel:             "root/source",
			ParentRel:       "root",
			ExpectedCurrent: CPUSetTarget{CPUs: machine.MustParse("2-3"), Mems: "0"},
			Target:          CPUSetTarget{CPUs: machine.NewCPUSet(), Mems: "0"},
			Direction:       WriteShrink,
		}},
	}

	closure, err := proveAdmissionRequiredClosure(
		plan,
		map[string]machine.CPUSet{"root/reclaim": machine.MustParse("0-5")},
	)
	if err != nil {
		t.Fatalf("proveAdmissionRequiredClosure() error = %v", err)
	}
	if len(closure.DrainOperations) != 1 {
		t.Fatalf("drain operations = %+v, want source drain", closure.DrainOperations)
	}
	if len(closure.ExpandOperations) != 2 ||
		closure.ExpandOperations[0].Rel != "root" ||
		closure.ExpandOperations[1].Rel != "root/reclaim" ||
		!closure.ExpandOperations[1].Target.CPUs.Equals(machine.MustParse("0-5")) {
		t.Fatalf("expand operations = %+v, want ancestor grow then reclaim expansion to 0-5",
			closure.ExpandOperations)
	}
	if !closure.FinalReport.Safe {
		t.Fatalf("final report = %+v, want ParentSafe", closure.FinalReport)
	}
}

func TestProveAdmissionRequiredClosureRejectsMissingRequiredTarget(t *testing.T) {
	snapshot := planSnapshot(map[string]EntryState{
		"reclaim": {
			Rel: "reclaim", CPUs: machine.MustParse("0-1"), Mems: "0",
		},
	}, map[DomainID]machine.CPUSet{DomainReclaim: machine.MustParse("0-1")})
	plan := PhasePlan{
		Base: snapshot,
		Kind: PhaseDrain,
		TargetByRel: map[string]CPUSetTarget{
			"reclaim": {CPUs: machine.MustParse("0-1"), Mems: "0"},
		},
	}

	_, err := proveAdmissionRequiredClosure(
		plan,
		map[string]machine.CPUSet{"reclaim": machine.MustParse("0-3")},
	)
	if err == nil {
		t.Fatal("proveAdmissionRequiredClosure() error = nil, want unprovable required floor")
	}
}

func TestProveAdmissionRequiredClosureIncludesTransferDestinationGrow(t *testing.T) {
	primaryID := CgroupIdentity{Device: 1, Inode: 1}
	reclaimID := CgroupIdentity{Device: 1, Inode: 2}
	snapshot := planSnapshot(map[string]EntryState{
		"kubepods": {
			Rel: "kubepods", Identity: primaryID, CPUs: machine.MustParse("0-1"), Mems: "0",
		},
		"kubepods/besteffort": {
			Rel: "kubepods/besteffort", Identity: CgroupIdentity{Device: 1, Inode: 3},
			CPUs: machine.MustParse("0-1"), Mems: "0",
		},
		"sandboxes/reclaimed-0": {
			Rel: "sandboxes/reclaimed-0", Identity: reclaimID, CPUs: machine.MustParse("2-3"), Mems: "0",
		},
	}, map[DomainID]machine.CPUSet{
		DomainPrimary: machine.MustParse("0-1"),
		DomainReclaim: machine.MustParse("2-3"),
	})
	snapshot.DomainByRel = map[string]DomainID{
		"kubepods":              DomainPrimary,
		"kubepods/besteffort":   DomainPrimary,
		"sandboxes/reclaimed-0": DomainReclaim,
	}
	snapshot.Children = map[string][]ChildRef{
		"kubepods": {{Name: "besteffort", Identity: CgroupIdentity{Device: 1, Inode: 3}}},
	}
	plan := PhasePlan{
		Base: snapshot,
		Kind: PhaseDrain,
		TargetByRel: map[string]CPUSetTarget{
			"kubepods":            {CPUs: machine.MustParse("0-2"), Mems: "0"},
			"kubepods/besteffort": {CPUs: machine.MustParse("0-2"), Mems: "0"},
		},
		CanonicalTargetByRel: map[string]CPUSetTarget{
			"kubepods":              {CPUs: machine.MustParse("0-2"), Mems: "0"},
			"sandboxes/reclaimed-0": {CPUs: machine.NewCPUSet(3), Mems: "0"},
		},
		Operations: []PlanOperation{{
			Rel:             "sandboxes/reclaimed-0",
			ExpectedCurrent: CPUSetTarget{CPUs: machine.MustParse("2-3"), Mems: "0"},
			Target:          CPUSetTarget{CPUs: machine.NewCPUSet(3), Mems: "0"},
			Direction:       WriteShrink,
		}},
		TransferGraph: map[DomainID]map[DomainID]machine.CPUSet{
			DomainReclaim: {DomainPrimary: machine.NewCPUSet(2)},
		},
	}

	closure, err := proveAdmissionRequiredClosure(
		plan,
		map[string]machine.CPUSet{"sandboxes/reclaimed-0": machine.NewCPUSet(3)},
	)
	if err != nil {
		t.Fatalf("proveAdmissionRequiredClosure() error = %v", err)
	}
	if len(closure.ExpandOperations) != 2 ||
		closure.ExpandOperations[0].Rel != "kubepods" ||
		closure.ExpandOperations[1].Rel != "kubepods/besteffort" ||
		!closure.ExpandOperations[1].Target.CPUs.Equals(machine.MustParse("0-2")) {
		t.Fatalf("expand operations = %+v, want transfer destination grow for kubepods and besteffort",
			closure.ExpandOperations)
	}
}
