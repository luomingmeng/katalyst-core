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

package provisionassembler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestQuantizeDisjointReclaimTargetToWholeCore_OddRoundDownToCommitted reproduces the
// control-loop transient: SMT2, odd reclaim target=3 on top of committed reserve=2.
// The lower whole-core value (2) is anchored on the committed reserve, so it rounds
// down rather than up to 4 (which would ask QRM to take a dedicated core).
func TestQuantizeDisjointReclaimTargetToWholeCore_OddRoundDownToCommitted(t *testing.T) {
	t.Parallel()

	got, decision := quantizeDisjointReclaimTargetToWholeCore(3, 2, 2)
	require.Equal(t, 2, got)
	require.Equal(t, disjointReclaimQuantizeRoundedDown, decision)
}

// TestQuantizeDisjointReclaimTargetToWholeCore_AlignedUnchanged proves an already
// whole-core-aligned target passes through.
func TestQuantizeDisjointReclaimTargetToWholeCore_AlignedUnchanged(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct{ reclaim, reserved, w int }{
		{8, 2, 2}, {4, 4, 2}, {12, 4, 4}, {0, 0, 2},
	} {
		got, decision := quantizeDisjointReclaimTargetToWholeCore(tc.reclaim, tc.reserved, tc.w)
		require.Equal(t, tc.reclaim, got)
		require.Equal(t, disjointReclaimQuantizeAligned, decision)
	}
}

// TestQuantizeDisjointReclaimTargetToWholeCore_SMT1Passthrough proves SMT1 has no
// odd/even distinction and every target is already a whole core.
func TestQuantizeDisjointReclaimTargetToWholeCore_SMT1Passthrough(t *testing.T) {
	t.Parallel()

	got, decision := quantizeDisjointReclaimTargetToWholeCore(3, 1, 1)
	require.Equal(t, 3, got)
	require.Equal(t, disjointReclaimQuantizeAligned, decision)
}

// TestQuantizeDisjointReclaimTargetToWholeCore_NeverShrinksReserve proves that
// rounding down would violate the committed reserve, so the quantization must choose
// the upper (or pass through) rather than ask QRM to give back a committed core.
func TestQuantizeDisjointReclaimTargetToWholeCore_NeverShrinksReserve(t *testing.T) {
	t.Parallel()

	// w=2, target=3, reserved=4. lower=2 < reserved -> illegal; upper=4 >= reserved.
	got, decision := quantizeDisjointReclaimTargetToWholeCore(3, 4, 2)
	require.Equal(t, 4, got)
	require.Equal(t, disjointReclaimQuantizeRoundedUp, decision)
}

// TestQuantizeDisjointReclaimTargetToWholeCore_PassthroughWhenNoLegalCandidate
// proves that when neither whole-core value can meet the committed reserve, the
// original target passes through untouched (QRM owns the reconcile).
func TestQuantizeDisjointReclaimTargetToWholeCore_PassthroughWhenNoLegalCandidate(t *testing.T) {
	t.Parallel()

	// w=4, target=5, reserved=8. lower=4 < reserved, upper=8 >= reserved -> legal.
	got, decision := quantizeDisjointReclaimTargetToWholeCore(5, 8, 4)
	require.Equal(t, 8, got)
	require.Equal(t, disjointReclaimQuantizeRoundedUp, decision)

	// w=4, target=1, reserved=8. lower=0, upper=4 both < reserved -> passthrough.
	got2, decision2 := quantizeDisjointReclaimTargetToWholeCore(1, 8, 4)
	require.Equal(t, 1, got2)
	require.Equal(t, disjointReclaimQuantizePassthrough, decision2)
}

// TestQuantizeDisjointReclaimTargetToWholeCore_SMT4Grid proves core-width
// agnosticism on SMT4 (w=4).
func TestQuantizeDisjointReclaimTargetToWholeCore_SMT4Grid(t *testing.T) {
	t.Parallel()

	// target=13, reserved=4 (one committed core). lower=12 (growth 8), upper=16
	// (growth 12); both >= reserved. Anchor distance picks lower (closer to 4).
	got, decision := quantizeDisjointReclaimTargetToWholeCore(13, 4, 4)
	require.Equal(t, 12, got)
	require.Equal(t, disjointReclaimQuantizeRoundedDown, decision)
}
