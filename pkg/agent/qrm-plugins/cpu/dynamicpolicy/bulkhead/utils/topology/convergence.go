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

import "github.com/kubewharf/katalyst-core/pkg/util/machine"

type ConvergenceReport struct {
	FullyConverged        bool
	NonConvergedTargets   []RelConvergence
	PendingToPrimary      machine.CPUSet
	PendingToReclaim      machine.CPUSet
	CleanupPendingPrimary machine.CPUSet
	CleanupPendingReclaim machine.CPUSet
}

type RelConvergence struct {
	Rel          string
	Observed     machine.CPUSet
	Target       machine.CPUSet
	ObservedMems string
	TargetMems   string
	Reason       string
}

const (
	convergenceReasonTargetMismatch = "target_mismatch"
	convergenceReasonReadError      = "read_error"
)

func buildConvergenceReport(
	snapshot *CompleteSnapshot,
	dag *TopoDAG,
	targetByRel map[string]machine.CPUSet,
	targetMemsByRel map[string]string,
	desired map[DomainID]machine.CPUSet,
	allowedCPUs machine.CPUSet,
	capabilities HierarchyCapabilities,
	allowEmptyTarget bool,
) (ConvergenceReport, error) {
	gate, err := NewDomainGate("convergence-report", snapshot, desired, allowedCPUs, nil)
	if err != nil {
		return ConvergenceReport{}, err
	}
	// Successful writes alone do not prove convergence: runtime descendants may
	// appear between writes. Compare the current hierarchy with targets to expose
	// incomplete convergence.
	report := ConvergenceReport{
		PendingToPrimary:      gate.pending[DomainPrimary].Clone(),
		PendingToReclaim:      gate.pending[DomainReclaim].Clone(),
		CleanupPendingPrimary: gate.cleanupPending[DomainPrimary].Clone(),
		CleanupPendingReclaim: gate.cleanupPending[DomainReclaim].Clone(),
	}
	for _, node := range dag.Nodes() {
		target := targetByRel[node.Rel]
		if target.IsEmpty() && !allowEmptyTarget {
			continue
		}
		entry, ok := snapshot.Entries[node.Rel]
		if !ok {
			report.NonConvergedTargets = append(report.NonConvergedTargets, RelConvergence{
				Rel:    node.Rel,
				Target: target.Clone(),
				Reason: convergenceReasonReadError,
			})
			continue
		}
		targetMems := targetMemsByRel[node.Rel]
		memsMatch := true
		if targetMems != "" {
			observedSet, observedErr := machine.Parse(entry.Mems)
			targetSet, targetErr := machine.Parse(targetMems)
			memsMatch = observedErr == nil && targetErr == nil && observedSet.Equals(targetSet)
		}
		observedCPUs := observedCPUsForTargetProof(entry, target, capabilities)
		if !observedCPUs.Equals(target) || !memsMatch {
			report.NonConvergedTargets = append(report.NonConvergedTargets, RelConvergence{
				Rel:          node.Rel,
				Observed:     observedCPUs.Clone(),
				Target:       target.Clone(),
				ObservedMems: entry.Mems,
				TargetMems:   targetMems,
				Reason:       convergenceReasonTargetMismatch,
			})
		}
	}
	report.FullyConverged = len(report.NonConvergedTargets) == 0 &&
		report.PendingToPrimary.IsEmpty() &&
		report.PendingToReclaim.IsEmpty() &&
		report.CleanupPendingPrimary.IsEmpty() &&
		report.CleanupPendingReclaim.IsEmpty()
	return report, nil
}
