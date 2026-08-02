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

package validator

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	advisorapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuadvisor"
	cpustate "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func TestCPUAdvisorValidatorRejectsIncompleteReclaimNUMAResults(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	state := cpustate.NewCPUPluginState(nil)
	validator := NewCPUAdvisorValidator(state, &machine.KatalystMachineInfo{CPUTopology: topology})

	testCases := []struct {
		name    string
		results map[int64]*advisorapi.NumaCalculationResult
		wantErr string
	}{
		{
			name:    "empty NUMA view is compatible",
			results: map[int64]*advisorapi.NumaCalculationResult{},
		},
		{
			name: "fake NUMA only is compatible",
			results: map[int64]*advisorapi.NumaCalculationResult{
				commonstate.FakedNUMAID: {Blocks: []*advisorapi.Block{{BlockId: "reclaim", Result: 2}}},
			},
		},
		{
			name: "partial physical NUMA result is compatible",
			results: map[int64]*advisorapi.NumaCalculationResult{
				1: {Blocks: []*advisorapi.Block{{BlockId: "reclaim-1", Result: 2}}},
			},
		},
		{
			name: "hybrid physical and fake NUMA result is compatible",
			results: map[int64]*advisorapi.NumaCalculationResult{
				0:                       {Blocks: []*advisorapi.Block{{BlockId: "reclaim-0", Result: 2}}},
				commonstate.FakedNUMAID: {Blocks: []*advisorapi.Block{{BlockId: "reclaim-fake", Result: 2}}},
			},
		},
		{
			name: "empty physical NUMA blocks",
			results: map[int64]*advisorapi.NumaCalculationResult{
				0: {Blocks: []*advisorapi.Block{}},
				1: {Blocks: []*advisorapi.Block{{BlockId: "reclaim-1", Result: 2}}},
			},
			wantErr: "empty reclaim blocks for NUMA: 0",
		},
		{
			name: "nil-only physical NUMA blocks",
			results: map[int64]*advisorapi.NumaCalculationResult{
				0: {Blocks: []*advisorapi.Block{nil}},
				1: {Blocks: []*advisorapi.Block{{BlockId: "reclaim-1", Result: 2}}},
			},
			wantErr: "empty reclaim blocks for NUMA: 0",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			resp := &advisorapi.ListAndWatchResponse{
				Entries: map[string]*advisorapi.CalculationEntries{
					commonstate.PoolNameReclaim: {
						Entries: map[string]*advisorapi.CalculationInfo{
							commonstate.FakedContainerName: {
								OwnerPoolName:             commonstate.PoolNameReclaim,
								CalculationResultsByNumas: tc.results,
							},
						},
					},
				},
			}

			err := validator.Validate(resp)
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr)
			}
		})
	}
}

func TestCPUAdvisorValidatorOverlapModeStillValidatesPhysicalNUMA(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	validator := NewCPUAdvisorValidator(cpustate.NewCPUPluginState(nil),
		&machine.KatalystMachineInfo{CPUTopology: topology})

	for _, tc := range []struct {
		name    string
		results map[int64]*advisorapi.NumaCalculationResult
		wantErr string
	}{
		{
			name: "unknown physical NUMA",
			results: map[int64]*advisorapi.NumaCalculationResult{
				2: {Blocks: []*advisorapi.Block{{BlockId: "reclaim-2", Result: 1}}},
			},
			wantErr: "NUMA: 2 referred by blocks isn't in topology",
		},
		{
			name: "physical NUMA capacity exceeded",
			results: map[int64]*advisorapi.NumaCalculationResult{
				0: {Blocks: []*advisorapi.Block{{BlockId: "reclaim-0", Result: 5}}},
			},
			wantErr: "exceeds NUMA capacity",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resp := &advisorapi.ListAndWatchResponse{
				AllowSharedCoresOverlapReclaimedCores: true,
				Entries: map[string]*advisorapi.CalculationEntries{
					commonstate.PoolNameReclaim: {
						Entries: map[string]*advisorapi.CalculationInfo{
							commonstate.FakedContainerName: {
								OwnerPoolName:             commonstate.PoolNameReclaim,
								CalculationResultsByNumas: tc.results,
							},
						},
					},
				},
			}
			require.ErrorContains(t, validator.Validate(resp), tc.wantErr)
		})
	}
}

func TestCPUAdvisorValidatorUsesIncomingOverlapMode(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)

	for _, tc := range []struct {
		name            string
		stateOverlap    bool
		incomingOverlap bool
		wantErr         bool
	}{
		{
			name:            "incoming disjoint rejects empty physical NUMA blocks despite stale overlap state",
			stateOverlap:    true,
			incomingOverlap: false,
			wantErr:         true,
		},
		{
			name:            "incoming overlap accepts empty physical NUMA blocks despite stale disjoint state",
			stateOverlap:    false,
			incomingOverlap: true,
			wantErr:         false,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			currentState := cpustate.NewCPUPluginState(nil)
			currentState.SetAllowSharedCoresOverlapReclaimedCores(tc.stateOverlap)
			validator := NewCPUAdvisorValidator(currentState, &machine.KatalystMachineInfo{CPUTopology: topology})
			resp := &advisorapi.ListAndWatchResponse{
				AllowSharedCoresOverlapReclaimedCores: tc.incomingOverlap,
				Entries: map[string]*advisorapi.CalculationEntries{
					commonstate.PoolNameReclaim: {
						Entries: map[string]*advisorapi.CalculationInfo{
							commonstate.FakedContainerName: {
								OwnerPoolName: commonstate.PoolNameReclaim,
								CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
									0: {Blocks: nil},
								},
							},
						},
					},
				},
			}

			err := validator.Validate(resp)
			if tc.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "empty reclaim blocks for NUMA: 0")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
