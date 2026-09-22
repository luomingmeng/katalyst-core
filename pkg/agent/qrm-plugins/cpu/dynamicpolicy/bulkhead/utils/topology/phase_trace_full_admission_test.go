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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

const (
	fullAdmissionPendingCPUCount = 160
	fullAdmissionNodePairs       = 76
	fullAdmissionWriteCount      = fullAdmissionNodePairs * 2
	fullAdmissionDeadline        = 5 * time.Second
)

func TestExecuteValidatedFrozenTraceFullAdmission(t *testing.T) {
	t.Run("preflight execute final proof and one publish", func(t *testing.T) {
		fixture, base, trace := newFullAdmissionTrace(t)
		requireFullAdmissionShape(t, trace)
		live := fixture.driver
		initial := live.snapshot()
		ctx, cancel := context.WithTimeout(context.Background(), fullAdmissionDeadline)
		defer cancel()
		res := &ConvergenceResult{}
		finalPublishes, parentSafePublishes := 0, 0
		publishFinal := func(snapshot *CompleteSnapshot) error {
			require.Equal(t, trace.FinalSnapshot.ID, snapshot.ID)
			finalPublishes++
			return nil
		}
		publishParentSafe := func(snapshot *CompleteSnapshot, _ map[string]struct{}) error {
			require.Equal(t, trace.FinalSnapshot.ID, snapshot.ID)
			parentSafePublishes++
			return nil
		}

		outcome, err := fixture.round.executeParentSafeAdmission(
			ctx, base, res, publishFinal, publishParentSafe)

		require.NoError(t, err)
		require.NoError(t, ctx.Err(), "full admission must finish before its deadline")
		require.Equal(t, fullAdmissionWriteCount, live.PhysicalWriteCount())
		require.Equal(t, fullAdmissionWriteCount, res.Applied)
		require.Len(t, res.Journal, fullAdmissionWriteCount)
		require.Equal(t, 1, finalPublishes)
		require.Zero(t, parentSafePublishes)
		require.True(t, res.FinalSnapshotCurrent)
		require.Equal(t, trace.FinalSnapshot.ID, outcome.Snapshot.ID)
		require.Equal(t, trace.FinalSnapshot.ID, res.FinalSnapshot.ID)
		require.NotEqual(t, initial, live.snapshot())
	})

	t.Run("final proof failure performs exact inverse writes and does not publish", func(t *testing.T) {
		fixture, base, trace := newFullAdmissionTrace(t)
		requireFullAdmissionShape(t, trace)
		live := fixture.driver
		initial := live.snapshot()

		driver := &injectedTraceDriver{
			HierarchyDriver: live,
			injection:       traceFailureInjection{failFinalProof: true},
			expectedForward: fullAdmissionWriteCount,
		}
		fixture.round.driver = driver
		ctx, cancel := context.WithTimeout(context.Background(), fullAdmissionDeadline)
		defer cancel()
		res := &ConvergenceResult{}
		finalPublishes, parentSafePublishes := 0, 0
		publishFinal := func(*CompleteSnapshot) error {
			finalPublishes++
			return nil
		}
		publishParentSafe := func(*CompleteSnapshot, map[string]struct{}) error {
			parentSafePublishes++
			return nil
		}

		_, err := fixture.round.executeParentSafeAdmission(
			ctx, base, res, publishFinal, publishParentSafe)

		require.ErrorContains(t, err, "injected final proof failure")
		require.NoError(t, ctx.Err(), "rollback must finish before its deadline")
		require.True(t, driver.injected, "final-proof failure point must be reached")
		require.Equal(t, fullAdmissionWriteCount, driver.forwardSuccess,
			"final-proof failure must be injected after every forward write")
		require.Equal(t, fullAdmissionWriteCount*2, live.PhysicalWriteCount())
		require.Len(t, live.writes[fullAdmissionWriteCount:], fullAdmissionWriteCount,
			"rollback must perform exactly one inverse write per forward write")
		requireExactFullAdmissionInverseWrites(t, live.writes, initial)
		require.Equal(t, initial, live.snapshot())
		require.Zero(t, finalPublishes)
		require.Zero(t, parentSafePublishes)
		require.Nil(t, res.FinalSnapshot)
		require.False(t, res.FinalSnapshotCurrent)
		require.Empty(t, res.Journal)
		require.Zero(t, res.Applied)
	})
}

func newFullAdmissionTrace(
	t *testing.T,
) (*admissionTraceFixture, *CompleteSnapshot, *CompiledPhaseTrace) {
	t.Helper()
	fixture := newAdmissionTraceFixture(t)
	fixture.driver.capabilities = cgroupV2Policy.capabilities(true)
	fixture.round.objective = ConvergenceObjectiveParentSafe
	fixture.round.allowEmptyTarget = true
	fixture.selection = DrainSelectionPolicy{RequirePairedSwapProgress: true}

	pending := machine.MustParse("0-159")
	primaryInitial := machine.NewCPUSet(160)
	primaryTarget := primaryInitial.Union(pending)
	reclaimInitial := machine.NewCPUSet(161).Union(pending)
	reclaimTarget := machine.NewCPUSet(161)
	for cpu := 4; cpu <= 161; cpu++ {
		fixture.cpuDetails[cpu] = machine.CPUTopoInfo{
			NUMANodeID: cpu / 80,
			SocketID:   cpu / 80,
			CoreID:     cpu,
		}
	}

	for index := 0; index < fullAdmissionNodePairs; index++ {
		primaryRel := fmt.Sprintf("primary-%02d", index)
		reclaimRel := fmt.Sprintf("reclaim-%02d", index)
		fixture.addPrimary(primaryRel, primaryInitial.String(), "0-1")
		fixture.addReclaim(reclaimRel, reclaimInitial.String(), "0-1")
		fixture.targetByRel[primaryRel] = primaryTarget.Clone()
		fixture.requiredByRel[primaryRel] = primaryTarget.Clone()
		fixture.targetByRel[reclaimRel] = reclaimTarget.Clone()
	}
	for index := range fixture.specs {
		fixture.specs[index].CPUs = fixture.targetByRel[fixture.specs[index].Rel].Clone()
	}
	fixture.round.protectedPending = pending.Clone()

	base := fixture.snapshot()
	trace, err := fixture.round.compileFixedPointTrace(context.Background(), base)
	require.NoError(t, err)
	return fixture, base, trace
}

func requireFullAdmissionShape(t *testing.T, trace *CompiledPhaseTrace) {
	t.Helper()
	pending := trace.FinalSnapshot.DomainUnion[DomainPrimary].
		Difference(trace.InitialSnapshot.DomainUnion[DomainPrimary])
	require.Equal(t, fullAdmissionPendingCPUCount, pending.Size())

	shrinks, grows := 0, 0
	for _, operation := range flattenTraceOperations(trace) {
		switch operation.Direction {
		case WriteShrink:
			shrinks++
		case WriteGrow:
			grows++
		}
	}
	require.Equal(t, fullAdmissionNodePairs, shrinks)
	require.Equal(t, fullAdmissionNodePairs, grows)
	require.Equal(t, fullAdmissionWriteCount, len(expectedPhysicalWrites(trace)))
}

func requireExactFullAdmissionInverseWrites(
	t *testing.T,
	writes []fakeHierarchyWrite,
	initial fakeHierarchyState,
) {
	t.Helper()
	require.Len(t, writes, fullAdmissionWriteCount*2)
	for index := 0; index < fullAdmissionWriteCount; index++ {
		forward := writes[index]
		inverse := writes[len(writes)-1-index]
		require.Equal(t, forward.rel, inverse.rel)
		require.Equal(t, forward.identity, inverse.identity)
		require.True(t, inverse.cpus.Equals(initial[inverse.rel].configuredCPUs),
			"inverse write %d for %q restored %s, want %s",
			index, inverse.rel, inverse.cpus.String(), initial[inverse.rel].configuredCPUs.String())
	}
}
