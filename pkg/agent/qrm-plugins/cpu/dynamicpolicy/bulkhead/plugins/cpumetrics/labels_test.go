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

package cpumetrics

import (
	"math/rand"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/model"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	utilmetric "github.com/kubewharf/katalyst-core/pkg/util/metric"
)

func TestAssignPoolLabels(t *testing.T) {
	t.Parallel()

	reclaim := model.CPUSetPoolIdentity{Kind: model.CPUSetPoolKindReclaim}
	share := func(name string) model.CPUSetPoolIdentity {
		return model.CPUSetPoolIdentity{Kind: model.CPUSetPoolKindShare, Name: name}
	}
	dedicated := func(uid string) model.CPUSetPoolIdentity {
		return model.CPUSetPoolIdentity{Kind: model.CPUSetPoolKindDedicated, PodUID: uid}
	}
	isolation := func(uid string) model.CPUSetPoolIdentity {
		return model.CPUSetPoolIdentity{Kind: model.CPUSetPoolKindIsolation, PodUID: uid}
	}
	wantEmptyConflicts := func() map[model.CPUSetPoolKind]machine.CPUSet {
		return map[model.CPUSetPoolKind]machine.CPUSet{
			model.CPUSetPoolKindShare:     machine.NewCPUSet(),
			model.CPUSetPoolKindDedicated: machine.NewCPUSet(),
			model.CPUSetPoolKindIsolation: machine.NewCPUSet(),
		}
	}

	t.Run("assigns fixed formatted and short UID labels", func(t *testing.T) {
		t.Parallel()

		byIdentity := map[model.CPUSetPoolIdentity]machine.CPUSet{
			reclaim:              machine.NewCPUSet(0),
			share("batch NUMA0"): machine.NewCPUSet(1),
			dedicated("ab91"):    machine.NewCPUSet(2),
			isolation("x"):       machine.NewCPUSet(3),
		}

		got := assignPoolLabels(byIdentity)

		require.Equal(t, []labeledPool{
			{identity: dedicated("ab91"), label: "dedicated-ab", cpus: machine.NewCPUSet(2)},
			{identity: isolation("x"), label: "isolation-x", cpus: machine.NewCPUSet(3)},
			{identity: reclaim, label: "reclaim", cpus: machine.NewCPUSet(0)},
			{identity: share("batch NUMA0"), label: "batch_NUMA0", cpus: machine.NewCPUSet(1)},
		}, got.pools)
		require.Equal(t, wantEmptyConflicts(), got.conflictCPUByKind)
	})

	t.Run("extends all colliding UID candidates to the shortest unique prefix", func(t *testing.T) {
		t.Parallel()

		got := assignPoolLabels(map[model.CPUSetPoolIdentity]machine.CPUSet{
			dedicated("ab91"): machine.NewCPUSet(9),
			dedicated("ab72"): machine.NewCPUSet(7),
		})

		require.Equal(t, []labeledPool{
			{identity: dedicated("ab72"), label: "dedicated-ab7", cpus: machine.NewCPUSet(7)},
			{identity: dedicated("ab91"), label: "dedicated-ab9", cpus: machine.NewCPUSet(9)},
		}, got.pools)
		require.Equal(t, wantEmptyConflicts(), got.conflictCPUByKind)
	})

	t.Run("extends UID labels through the first differing character", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name        string
			firstUID    string
			secondUID   string
			firstLabel  string
			secondLabel string
		}{
			{
				name:        "fourth character",
				firstUID:    "abc1tail",
				secondUID:   "abc2tail",
				firstLabel:  "dedicated-abc1",
				secondLabel: "dedicated-abc2",
			},
			{
				name:        "fifth character",
				firstUID:    "abcd1tail",
				secondUID:   "abcd2tail",
				firstLabel:  "dedicated-abcd1",
				secondLabel: "dedicated-abcd2",
			},
			{
				name:        "sixth character",
				firstUID:    "abcde1tail",
				secondUID:   "abcde2tail",
				firstLabel:  "dedicated-abcde1",
				secondLabel: "dedicated-abcde2",
			},
			{
				name:        "seventh character",
				firstUID:    "abcdef1tail",
				secondUID:   "abcdef2tail",
				firstLabel:  "dedicated-abcdef1",
				secondLabel: "dedicated-abcdef2",
			},
			{
				name:        "eighth character",
				firstUID:    "abcdefg1tail",
				secondUID:   "abcdefg2tail",
				firstLabel:  "dedicated-abcdefg1",
				secondLabel: "dedicated-abcdefg2",
			},
		}

		for _, tc := range tests {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				got := assignPoolLabels(map[model.CPUSetPoolIdentity]machine.CPUSet{
					dedicated(tc.firstUID):  machine.NewCPUSet(1),
					dedicated(tc.secondUID): machine.NewCPUSet(2),
				})

				require.Equal(t, []labeledPool{
					{identity: dedicated(tc.firstUID), label: tc.firstLabel, cpus: machine.NewCPUSet(1)},
					{identity: dedicated(tc.secondUID), label: tc.secondLabel, cpus: machine.NewCPUSet(2)},
				}, got.pools)
				require.Equal(t, wantEmptyConflicts(), got.conflictCPUByKind)
			})
		}
	})

	t.Run("falls back to complete UID after the first eight characters collide", func(t *testing.T) {
		t.Parallel()

		got := assignPoolLabels(map[model.CPUSetPoolIdentity]machine.CPUSet{
			dedicated("abcdefgh1"): machine.NewCPUSet(1),
			dedicated("abcdefgh2"): machine.NewCPUSet(2),
		})

		require.Equal(t, []labeledPool{
			{identity: dedicated("abcdefgh1"), label: "dedicated-abcdefgh1", cpus: machine.NewCPUSet(1)},
			{identity: dedicated("abcdefgh2"), label: "dedicated-abcdefgh2", cpus: machine.NewCPUSet(2)},
		}, got.pools)
		require.Equal(t, wantEmptyConflicts(), got.conflictCPUByKind)
	})

	t.Run("falls back to complete non-ASCII UIDs after eight runes collide", func(t *testing.T) {
		t.Parallel()

		firstUID := "甲乙丙丁戊己庚辛一"
		secondUID := "甲乙丙丁戊己庚辛二"
		got := assignPoolLabels(map[model.CPUSetPoolIdentity]machine.CPUSet{
			dedicated(firstUID):  machine.NewCPUSet(1),
			dedicated(secondUID): machine.NewCPUSet(2),
		})

		require.Equal(t, []labeledPool{
			{identity: dedicated(firstUID), label: "dedicated-" + firstUID, cpus: machine.NewCPUSet(1)},
			{identity: dedicated(secondUID), label: "dedicated-" + secondUID, cpus: machine.NewCPUSet(2)},
		}, got.pools)
		for _, pool := range got.pools {
			require.True(t, utf8.ValidString(pool.label))
		}
		require.Equal(t, wantEmptyConflicts(), got.conflictCPUByKind)
	})

	t.Run("extends a dynamic label past a reserved shared label", func(t *testing.T) {
		t.Parallel()

		got := assignPoolLabels(map[model.CPUSetPoolIdentity]machine.CPUSet{
			share("dedicated-ab"): machine.NewCPUSet(1),
			dedicated("ab91"):     machine.NewCPUSet(2),
		})

		require.Equal(t, []labeledPool{
			{identity: dedicated("ab91"), label: "dedicated-ab9", cpus: machine.NewCPUSet(2)},
			{identity: share("dedicated-ab"), label: "dedicated-ab", cpus: machine.NewCPUSet(1)},
		}, got.pools)
		require.Equal(t, wantEmptyConflicts(), got.conflictCPUByKind)
	})

	t.Run("omits every participant in an unresolvable dynamic shared collision", func(t *testing.T) {
		t.Parallel()

		got := assignPoolLabels(map[model.CPUSetPoolIdentity]machine.CPUSet{
			share("dedicated-x"): machine.NewCPUSet(1),
			dedicated("x"):       machine.NewCPUSet(2),
		})

		require.Empty(t, got.pools)
		require.Equal(t, map[model.CPUSetPoolKind]machine.CPUSet{
			model.CPUSetPoolKindShare:     machine.NewCPUSet(1),
			model.CPUSetPoolKindDedicated: machine.NewCPUSet(2),
			model.CPUSetPoolKindIsolation: machine.NewCPUSet(),
		}, got.conflictCPUByKind)
	})

	t.Run("omits all shared identities whose formatted labels collide", func(t *testing.T) {
		t.Parallel()

		got := assignPoolLabels(map[model.CPUSetPoolIdentity]machine.CPUSet{
			share("batch NUMA0"): machine.NewCPUSet(1, 3),
			share("batch_NUMA0"): machine.NewCPUSet(2, 3),
		})

		require.Empty(t, got.pools)
		require.Equal(t, map[model.CPUSetPoolKind]machine.CPUSet{
			model.CPUSetPoolKindShare:     machine.NewCPUSet(1, 2, 3),
			model.CPUSetPoolKindDedicated: machine.NewCPUSet(),
			model.CPUSetPoolKindIsolation: machine.NewCPUSet(),
		}, got.conflictCPUByKind)
	})

	t.Run("omits shared identities whose long names collide after formatter truncation", func(t *testing.T) {
		t.Parallel()

		common := strings.Repeat("s", utilmetric.MaxTagLength)
		firstName := common + "-first"
		secondName := common + "-second"
		require.Greater(t, len(firstName), utilmetric.MaxTagLength)
		require.Greater(t, len(secondName), utilmetric.MaxTagLength)
		require.Equal(t,
			utilmetric.MetricTagValueFormat(firstName),
			utilmetric.MetricTagValueFormat(secondName))

		got := assignPoolLabels(map[model.CPUSetPoolIdentity]machine.CPUSet{
			share(firstName):  machine.NewCPUSet(1, 3),
			share(secondName): machine.NewCPUSet(2, 3),
		})

		require.Empty(t, got.pools)
		require.Equal(t, map[model.CPUSetPoolKind]machine.CPUSet{
			model.CPUSetPoolKindShare:     machine.NewCPUSet(1, 2, 3),
			model.CPUSetPoolKindDedicated: machine.NewCPUSet(),
			model.CPUSetPoolKindIsolation: machine.NewCPUSet(),
		}, got.conflictCPUByKind)
	})

	t.Run("omits all dynamic identities when complete formatted labels still collide", func(t *testing.T) {
		t.Parallel()

		common := strings.Repeat("a", utilmetric.MaxTagLength)
		firstUID := common + "1"
		secondUID := common + "2"
		got := assignPoolLabels(map[model.CPUSetPoolIdentity]machine.CPUSet{
			dedicated(firstUID):  machine.NewCPUSet(4),
			dedicated(secondUID): machine.NewCPUSet(5),
		})

		require.Empty(t, got.pools)
		require.Equal(t, map[model.CPUSetPoolKind]machine.CPUSet{
			model.CPUSetPoolKindShare:     machine.NewCPUSet(),
			model.CPUSetPoolKindDedicated: machine.NewCPUSet(4, 5),
			model.CPUSetPoolKindIsolation: machine.NewCPUSet(),
		}, got.conflictCPUByKind)
	})

	t.Run("omits empty UIDs and accepts one character UIDs", func(t *testing.T) {
		t.Parallel()

		got := assignPoolLabels(map[model.CPUSetPoolIdentity]machine.CPUSet{
			dedicated(""):  machine.NewCPUSet(0),
			isolation(""):  machine.NewCPUSet(1),
			dedicated("d"): machine.NewCPUSet(2),
			isolation("i"): machine.NewCPUSet(3),
		})

		require.Equal(t, []labeledPool{
			{identity: dedicated("d"), label: "dedicated-d", cpus: machine.NewCPUSet(2)},
			{identity: isolation("i"), label: "isolation-i", cpus: machine.NewCPUSet(3)},
		}, got.pools)
		require.Equal(t, map[model.CPUSetPoolKind]machine.CPUSet{
			model.CPUSetPoolKindShare:     machine.NewCPUSet(),
			model.CPUSetPoolKindDedicated: machine.NewCPUSet(0),
			model.CPUSetPoolKindIsolation: machine.NewCPUSet(1),
		}, got.conflictCPUByKind)
	})

	t.Run("is deterministic across randomized map insertion order", func(t *testing.T) {
		t.Parallel()

		type entry struct {
			identity model.CPUSetPoolIdentity
			cpus     machine.CPUSet
		}
		entries := []entry{
			{identity: reclaim, cpus: machine.NewCPUSet(0)},
			{identity: share("batch NUMA0"), cpus: machine.NewCPUSet(1)},
			{identity: dedicated("ab91"), cpus: machine.NewCPUSet(2)},
			{identity: dedicated("ab72"), cpus: machine.NewCPUSet(3)},
			{identity: isolation("z"), cpus: machine.NewCPUSet(4)},
		}
		baselineMap := make(map[model.CPUSetPoolIdentity]machine.CPUSet, len(entries))
		for _, entry := range entries {
			baselineMap[entry.identity] = entry.cpus
		}
		want := assignPoolLabels(baselineMap)
		random := rand.New(rand.NewSource(42))

		for iteration := 0; iteration < 50; iteration++ {
			random.Shuffle(len(entries), func(i, j int) {
				entries[i], entries[j] = entries[j], entries[i]
			})
			shuffled := make(map[model.CPUSetPoolIdentity]machine.CPUSet, len(entries))
			for _, entry := range entries {
				shuffled[entry.identity] = entry.cpus
			}
			require.Equal(t, want, assignPoolLabels(shuffled))
		}
	})

	t.Run("contracts labels after a colliding Pod is removed", func(t *testing.T) {
		t.Parallel()

		withCollision := assignPoolLabels(map[model.CPUSetPoolIdentity]machine.CPUSet{
			dedicated("ab91"): machine.NewCPUSet(1),
			dedicated("ab72"): machine.NewCPUSet(2),
		})
		withoutCollision := assignPoolLabels(map[model.CPUSetPoolIdentity]machine.CPUSet{
			dedicated("ab91"): machine.NewCPUSet(1),
		})

		require.Equal(t, "dedicated-ab9", withCollision.pools[1].label)
		require.Equal(t, []labeledPool{
			{identity: dedicated("ab91"), label: "dedicated-ab", cpus: machine.NewCPUSet(1)},
		}, withoutCollision.pools)
	})
}

func TestUIDLabelCandidates(t *testing.T) {
	t.Parallel()

	uid := "甲乙丙丁戊己庚辛壬"
	got := uidLabelCandidates("dedicated-", uid)

	require.Equal(t, []string{
		"dedicated-甲乙",
		"dedicated-甲乙丙",
		"dedicated-甲乙丙丁",
		"dedicated-甲乙丙丁戊",
		"dedicated-甲乙丙丁戊己",
		"dedicated-甲乙丙丁戊己庚",
		"dedicated-甲乙丙丁戊己庚辛",
		"dedicated-" + uid,
	}, got)
	for _, candidate := range got {
		require.True(t, utf8.ValidString(candidate))
	}
}
