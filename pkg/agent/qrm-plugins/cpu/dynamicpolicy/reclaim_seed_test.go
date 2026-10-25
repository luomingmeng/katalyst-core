package dynamicpolicy

import (
	"fmt"
	"testing"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	"github.com/stretchr/testify/require"
)

func TestSelectReclaimSeedMinimalCompleteSubset(t *testing.T) {
	for _, sizes := range [][]int{{2, 3}, {3, 2, 2}, {4, 1, 2}, {1, 2, 3, 4}} {
		t.Run(fmt.Sprint(sizes), func(t *testing.T) {
			topology := &machine.CPUTopology{CPUDetails: make(machine.CPUDetails)}
			cpu := 0
			for core, size := range sizes {
				for sibling := 0; sibling < size; sibling++ {
					topology.CPUDetails[cpu] = machine.CPUTopoInfo{NUMANodeID: core % 2, CoreID: core}
					cpu++
				}
			}
			eligible := topology.CPUDetails.CPUs()
			for requested := 1; requested <= cpu; requested++ {
				best := cpu
				for mask := 1; mask < 1<<len(sizes); mask++ {
					sum := 0
					for i, size := range sizes {
						if mask&(1<<i) != 0 {
							sum += size
						}
					}
					if sum >= requested && sum < best {
						best = sum
					}
				}
				got, err := selectReclaimSeed(topology, eligible, machine.NewCPUSet(), requested)
				require.NoError(t, err)
				require.Equal(t, best, got.Size(), "request=%d sizes=%v", requested, sizes)
				require.NoError(t, assertCoreAligned(got, topology))
				for repeat := 0; repeat < 3; repeat++ {
					again, err := selectReclaimSeed(topology, eligible, machine.NewCPUSet(), requested)
					require.NoError(t, err)
					require.True(t, got.Equals(again))
				}
			}
		})
	}
}

func BenchmarkSelectReclaimSeed(b *testing.B) {
	for _, n := range []int{128, 1024} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			topology, err := machine.GenerateDummyCPUTopology(n, 2, 2)
			if err != nil {
				b.Fatal(err)
			}
			eligible := topology.CPUDetails.CPUs()
			prefer := machine.NewCPUSet()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := selectReclaimSeed(topology, eligible, prefer, 4); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
