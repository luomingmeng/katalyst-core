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

package cpu

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func Test_cpuResourceAdvisor_updateReservedForReclaim(t *testing.T) {
	t.Parallel()
	type fields struct {
		numaNum                                  int
		socketNum                                int
		numCPUs                                  int
		minReclaimedResourceForAllocate          v1.ResourceList
		numaMinReclaimedResourceRatioForAllocate v1.ResourceList
	}
	tests := []struct {
		name                   string
		fields                 fields
		wantReservedForReclaim map[int]int
	}{
		{
			name: "reserved for reclaim",
			fields: fields{
				numaNum:   2,
				socketNum: 1,
				numCPUs:   16,
				minReclaimedResourceForAllocate: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("4"),
				},
			},
			wantReservedForReclaim: map[int]int{
				0: 2,
				1: 2,
			},
		},
		{
			name: "reserved all for reclaim",
			fields: fields{
				numaNum:   2,
				socketNum: 1,
				numCPUs:   16,
				minReclaimedResourceForAllocate: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("16"),
				},
			},
			wantReservedForReclaim: map[int]int{
				0: 8,
				1: 8,
			},
		},
		{
			name: "reserved for reclaim less than numa num",
			fields: fields{
				numaNum:   8,
				socketNum: 2,
				numCPUs:   16,
				minReclaimedResourceForAllocate: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("4"),
				},
			},
			wantReservedForReclaim: map[int]int{
				0: 1,
				1: 1,
				2: 1,
				3: 1,
				4: 1,
				5: 1,
				6: 1,
				7: 1,
			},
		},
		{
			name: "reserved with numa size ratio",
			fields: fields{
				numaNum:   2,
				socketNum: 1,
				numCPUs:   64,
				minReclaimedResourceForAllocate: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("4"),
				},
				numaMinReclaimedResourceRatioForAllocate: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("0.05"),
				},
			},
			wantReservedForReclaim: map[int]int{
				0: 2,
				1: 2,
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ckDir, err := ioutil.TempDir("", "checkpoint-updateReservedForReclaim")
			require.NoError(t, err)
			defer func() { _ = os.RemoveAll(ckDir) }()

			sfDir, err := ioutil.TempDir("", "statefile")
			require.NoError(t, err)
			defer func() { _ = os.RemoveAll(sfDir) }()

			conf := generateTestConfiguration(t, ckDir, sfDir)
			conf.GetDynamicConfiguration().EnableStrategyGroup = true
			conf.GetDynamicConfiguration().MinReclaimedResourceForAllocate = tt.fields.minReclaimedResourceForAllocate
			conf.GetDynamicConfiguration().NumaMinReclaimedResourceRatioForAllocate = tt.fields.numaMinReclaimedResourceRatioForAllocate

			cpuTopology, err := machine.GenerateDummyCPUTopology(tt.fields.numCPUs, tt.fields.socketNum, tt.fields.numaNum)
			assert.NoError(t, err)

			metaServer := &metaserver.MetaServer{
				MetaAgent: &agent.MetaAgent{
					KatalystMachineInfo: &machine.KatalystMachineInfo{
						CPUTopology: cpuTopology,
					},
				},
			}

			cra := &cpuResourceAdvisor{
				conf:       conf,
				metaServer: metaServer,
			}
			cra.updateReservedForReclaim()

			assert.Equal(t, tt.wantReservedForReclaim, cra.reservedForReclaim)
		})
	}
}

func TestHardPartitionReservedReclaimCores(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		ratioReserved int
		numaCount     int
		want          int
	}{
		{
			name:          "zero ratio across two NUMAs",
			ratioReserved: 0,
			numaCount:     2,
			want:          4,
		},
		{
			name:          "hard floor wins across two NUMAs",
			ratioReserved: 3,
			numaCount:     2,
			want:          4,
		},
		{
			name:          "ratio wins across two NUMAs",
			ratioReserved: 8,
			numaCount:     2,
			want:          8,
		},
		{
			name:          "hard floor wins across four NUMAs",
			ratioReserved: 2,
			numaCount:     4,
			want:          8,
		},
		{
			name:          "no NUMA preserves ratio reserve",
			ratioReserved: 3,
			numaCount:     0,
			want:          3,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, hardPartitionReservedReclaimCores(tt.ratioReserved, tt.numaCount))
		})
	}
}

func TestCPUResourceAdvisorUpdateReservedForReclaimHardPartitionCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		ratio        float64
		wantReserved map[int]int
		wantErr      string
	}{
		{
			name:         "candidate fits every NUMA",
			ratio:        0.5,
			wantReserved: map[int]int{0: 4, 1: 4},
		},
		{
			name:    "candidate exceeds one NUMA",
			ratio:   0.75,
			wantErr: "NUMA 0 reclaim reservation exceeds capacity: required 6, available 4",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			conf := generateTestConfiguration(t, t.TempDir(), t.TempDir())
			dynamicConf := conf.GetDynamicConfiguration()
			dynamicConf.EnableRampUpReclaimHardPartition = true
			dynamicConf.InitialRampUpReclaimCPUSetRatio = tt.ratio

			cra := &cpuResourceAdvisor{
				conf: conf,
				metaServer: &metaserver.MetaServer{
					MetaAgent: &agent.MetaAgent{
						KatalystMachineInfo: &machine.KatalystMachineInfo{
							CPUTopology: &machine.CPUTopology{
								NumNUMANodes: 2,
							},
						},
					},
				},
				numaAvailable: map[int]int{0: 4, 1: 12},
			}

			err := cra.updateReservedForReclaim()
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				assert.Nil(t, cra.reservedForReclaim)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantReserved, cra.reservedForReclaim)
		})
	}
}
