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
	"os"
	"sort"

	cgroupclient "github.com/kubewharf/katalyst-core/pkg/util/cgroup/client"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

var ErrNotConverged = errors.New("bulkhead topology not fully converged")

type ConvergenceError struct {
	Report ConvergenceReport
}

func (e *ConvergenceError) Error() string {
	return fmt.Sprintf("%v: %+v", ErrNotConverged, e.Report)
}

func (e *ConvergenceError) Unwrap() error {
	return ErrNotConverged
}

type ConvergenceReport struct {
	FullyConverged        bool
	NonConvergedTargets   []RelConvergence
	PendingToPrimary      machine.CPUSet
	PendingToReclaim      machine.CPUSet
	CleanupPendingPrimary machine.CPUSet
	CleanupPendingReclaim machine.CPUSet
}

type RelConvergence struct {
	Rel      string
	Observed machine.CPUSet
	Target   machine.CPUSet
	Reason   string
	Detail   string
}

const (
	convergenceReasonTargetMismatch = "target_mismatch"
	convergenceReasonReadError      = "read_error"
)

func buildConvergenceReport(
	ctx context.Context,
	cg cgroupclient.CgroupClient,
	dag *TopoDAG,
	targetByRel map[string]machine.CPUSet,
	controlledCPUSetByRel map[string]machine.CPUSet,
	cpuDetails machine.CPUDetails,
	reservedCPUs machine.CPUSet,
	_ bool,
	cache *applyCache,
) (ConvergenceReport, error) {
	// The transfer cycle cache is intentionally scoped to its gate snapshot.
	// Final convergence is new evidence: re-enumerate descendants so runtime
	// cgroups created after the last gate cannot be hidden by cached children.
	cache.resetChildren()
	snapshot, err := buildDomainSnapshot(ctx, cg, dag, targetByRel, cpuDetails, reservedCPUs, cache)
	if err != nil {
		return ConvergenceReport{}, err
	}
	gate := newDomainGate(snapshot)
	// Successful writes alone do not prove convergence: runtime descendants may
	// appear between writes. Compare the current hierarchy with targets to expose
	// incomplete convergence.
	report := ConvergenceReport{
		PendingToPrimary:      gate.pendingToPrimary.Clone(),
		PendingToReclaim:      gate.pendingToReclaim.Clone(),
		CleanupPendingPrimary: gate.cleanupPendingPrimary.Clone(),
		CleanupPendingReclaim: gate.cleanupPendingReclaim.Clone(),
	}
	if len(controlledCPUSetByRel) == 0 {
		controlledCPUSetByRel = targetByRel
	}
	rels := make([]string, 0, len(controlledCPUSetByRel))
	for rel := range controlledCPUSetByRel {
		rels = append(rels, rel)
	}
	sort.Strings(rels)
	for _, rel := range rels {
		target := controlledCPUSetByRel[rel]
		observed, err := cg.ReadCPUSet(ctx, rel)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return ConvergenceReport{}, fmt.Errorf("fresh read controlled cpuset rel=%q: %w", rel, err)
			}
			report.NonConvergedTargets = append(report.NonConvergedTargets, RelConvergence{
				Rel:    rel,
				Target: target.Clone(),
				Reason: convergenceReasonReadError,
				Detail: err.Error(),
			})
			continue
		}
		if !observed.Equals(target) {
			report.NonConvergedTargets = append(report.NonConvergedTargets, RelConvergence{
				Rel:      rel,
				Observed: observed.Clone(),
				Target:   target.Clone(),
				Reason:   convergenceReasonTargetMismatch,
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
