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

package dynamicpolicy

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/advisorsvc"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	cpuconsts "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/consts"
	advisorapi "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuadvisor"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/agent/utilcomponent/featuregatenegotiation/finders/feature_cpu"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	resourcepackage "github.com/kubewharf/katalyst-core/pkg/util/resource-package"
)

func TestDeriveAdvisorIsolationSourcePool(t *testing.T) {
	t.Parallel()

	entries := state.PodEntries{
		"pod1": state.ContainerEntries{
			"c": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "pod1",
					ContainerName: "c",
					QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
					OwnerPoolName: commonstate.PoolNamePrefixIsolation + "-pod1",
					Annotations:   map[string]string{},
				},
			},
		},
	}
	block := &advisorapi.BlockInfo{
		Block: advisorapi.Block{BlockId: "block-isolation", Result: 2},
		OwnerPoolEntryMap: map[string]advisorapi.BlockEntry{
			commonstate.PoolNamePrefixIsolation + "-pod1": {
				EntryName:    "pod1",
				SubEntryName: "c",
			},
		},
	}

	source, ok := deriveAdvisorIsolationSourcePool(block, entries)
	require.True(t, ok)
	require.Equal(t, commonstate.PoolNameShare, source)
}

func TestDynamicPolicy_tryCarveAdvisorBlockFromSource(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)

	tmpDir, err := ioutil.TempDir("", "checkpoint-TestDynamicPolicy_tryCarveAdvisorBlockFromSource")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, tmpDir)
	require.NoError(t, err)

	p.state.SetPodEntries(state.PodEntries{
		"pod1": state.ContainerEntries{
			"c": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "pod1",
					ContainerName: "c",
					QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
					OwnerPoolName: commonstate.PoolNamePrefixIsolation + "-pod1",
					Annotations:   map[string]string{},
				},
			},
		},
	}, false)

	blockCPUSet := advisorapi.BlockCPUSet{
		"block-share": machine.NewCPUSet(0, 1, 2, 3),
	}
	sourceBlockByPool := map[string]string{
		commonstate.PoolNameShare: "block-share",
	}
	availableCPUs := machine.NewCPUSet(4, 5)
	nodeRemainingCPUs := machine.NewCPUSet(4, 5, 6, 7)
	block := &advisorapi.BlockInfo{
		Block: advisorapi.Block{BlockId: "block-isolation", Result: 3},
		OwnerPoolEntryMap: map[string]advisorapi.BlockEntry{
			commonstate.PoolNamePrefixIsolation + "-pod1": {
				EntryName:    "pod1",
				SubEntryName: "c",
			},
		},
	}

	carved, err := p.tryCarveAdvisorBlockFromSource(
		block, sourceBlockByPool, nil, blockCPUSet, availableCPUs.Clone(), &availableCPUs, &nodeRemainingCPUs, commonstate.FakedNUMAID, 3)
	require.NoError(t, err)
	require.True(t, carved)

	require.Equal(t, 3, blockCPUSet["block-isolation"].Size())
	require.True(t, blockCPUSet["block-isolation"].IsSubsetOf(machine.NewCPUSet(0, 1, 2, 3)),
		"isolation block should be carved from the source share block first, got %s",
		blockCPUSet["block-isolation"].String())
	require.Equal(t, 1, blockCPUSet["block-share"].Size(),
		"source share block should be shrunk after carve, got %s",
		blockCPUSet["block-share"].String())
	require.True(t, availableCPUs.Equals(machine.NewCPUSet(4, 5)),
		"available cpus should not be consumed when source is sufficient, got %s",
		availableCPUs.String())
}

func TestDynamicPolicy_tryCarveAdvisorBlockFromSourceUsesConstrainedFallback(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)

	tmpDir, err := ioutil.TempDir("", "checkpoint-TestDynamicPolicy_tryCarveAdvisorBlockFromSource_constrainedFallback")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, tmpDir)
	require.NoError(t, err)
	p.state.SetPodEntries(state.PodEntries{
		"pod1": state.ContainerEntries{
			"c": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "pod1",
					ContainerName: "c",
					QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
					OwnerPoolName: commonstate.PoolNamePrefixIsolation + "-pod1",
					Annotations:   map[string]string{},
				},
			},
		},
	}, false)

	blockCPUSet := advisorapi.BlockCPUSet{
		"block-share": machine.NewCPUSet(0),
	}
	sourceBlockByPool := map[string]string{
		commonstate.PoolNameShare: "block-share",
	}
	fallbackCandidate := machine.NewCPUSet(4, 5)
	availableCPUs := machine.NewCPUSet(4, 5, 6, 7)
	nodeRemainingCPUs := machine.NewCPUSet(4, 5, 6, 7)
	block := &advisorapi.BlockInfo{
		Block: advisorapi.Block{BlockId: "block-isolation", Result: 3},
		OwnerPoolEntryMap: map[string]advisorapi.BlockEntry{
			commonstate.PoolNamePrefixIsolation + "-pod1": {
				EntryName:    "pod1",
				SubEntryName: "c",
			},
		},
	}

	carved, err := p.tryCarveAdvisorBlockFromSource(
		block, sourceBlockByPool, nil, blockCPUSet, fallbackCandidate, &availableCPUs, &nodeRemainingCPUs, commonstate.FakedNUMAID, 3)
	require.NoError(t, err)
	require.True(t, carved)
	require.True(t, blockCPUSet["block-isolation"].IsSubsetOf(machine.NewCPUSet(0, 4, 5)),
		"fallback should only use the constrained candidate, got %s",
		blockCPUSet["block-isolation"].String())
	require.True(t, availableCPUs.Contains(6) && availableCPUs.Contains(7),
		"unconstrained available CPUs must not be consumed, available=%s",
		availableCPUs.String())
}

func TestDynamicPolicy_allocateShareBlocks_carvesIsolationFromAllocatedSource(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)

	tmpDir, err := ioutil.TempDir("", "checkpoint-TestDynamicPolicy_allocateShareBlocks_carvesIsolationFromAllocatedSource")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, tmpDir)
	require.NoError(t, err)
	p.state.SetPodEntries(state.PodEntries{
		"pod1": state.ContainerEntries{
			"c": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "pod1",
					ContainerName: "c",
					QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
					OwnerPoolName: commonstate.PoolNamePrefixIsolation + "-pod1",
					Annotations:   map[string]string{},
				},
			},
		},
	}, false)

	blockCPUSet := advisorapi.BlockCPUSet{
		"block-share": machine.NewCPUSet(0, 1, 2, 3),
	}
	availableCPUs := machine.NewCPUSet(4, 5)
	nodeRemainingCPUs := machine.NewCPUSet(4, 5, 6, 7)
	sourceBlockByPool := map[string]string{
		commonstate.PoolNameShare: "block-share",
	}
	blocks := []*advisorapi.BlockInfo{
		{
			Block: advisorapi.Block{BlockId: "block-isolation", Result: 2},
			OwnerPoolEntryMap: map[string]advisorapi.BlockEntry{
				commonstate.PoolNamePrefixIsolation + "-pod1": {
					EntryName:    "pod1",
					SubEntryName: "c",
				},
			},
		},
	}

	err = p.allocateShareBlocks(
		commonstate.FakedNUMAID,
		blocks,
		blockCPUSet,
		machine.NewCPUSet(),
		&nodeRemainingCPUs,
		&availableCPUs,
		nil,
		machine.NewCPUSet(),
		nil,
		sourceBlockByPool,
	)
	require.NoError(t, err)
	require.True(t, blockCPUSet["block-isolation"].IsSubsetOf(machine.NewCPUSet(0, 1, 2, 3)),
		"isolation block should be carved from source share block, got %s",
		blockCPUSet["block-isolation"].String())
	require.Equal(t, 2, blockCPUSet["block-share"].Size())
	require.True(t, availableCPUs.Equals(machine.NewCPUSet(4, 5)))
}

func TestDynamicPolicy_allocateShareBlocks_sourceCarveFallbackRespectsNUMA(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)

	tmpDir, err := ioutil.TempDir("", "checkpoint-TestDynamicPolicy_allocateShareBlocks_sourceCarveFallbackRespectsNUMA")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, tmpDir)
	require.NoError(t, err)
	p.state.SetPodEntries(state.PodEntries{
		"pod1": state.ContainerEntries{
			"c": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "pod1",
					ContainerName: "c",
					QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
					OwnerPoolName: commonstate.PoolNamePrefixIsolation + "-pod1",
					Annotations: map[string]string{
						apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
						"numa_hint": "0",
					},
				},
			},
		},
	}, false)

	blockCPUSet := advisorapi.BlockCPUSet{
		"block-share-numa0": machine.NewCPUSet(0),
	}
	availableCPUs := machine.NewCPUSet(1, 2, 3, 4, 5, 6)
	outsideNUMAAvailableCPUs := availableCPUs.Intersection(cpuTopology.CPUDetails.CPUsInNUMANodes(1))
	nodeRemainingCPUs := availableCPUs.Clone()
	blocks := []*advisorapi.BlockInfo{
		{
			Block: advisorapi.Block{BlockId: "block-isolation", Result: 3},
			OwnerPoolEntryMap: map[string]advisorapi.BlockEntry{
				commonstate.PoolNamePrefixIsolation + "-pod1": {
					EntryName:    "pod1",
					SubEntryName: "c",
				},
			},
		},
	}

	err = p.allocateShareBlocks(
		0,
		blocks,
		blockCPUSet,
		cpuTopology.CPUDetails.CPUsInNUMANodes(0),
		&nodeRemainingCPUs,
		&availableCPUs,
		nil,
		machine.NewCPUSet(),
		nil,
		map[string]string{commonstate.PoolNameShare + commonstate.NUMAPoolInfix + "0": "block-share-numa0"},
	)
	require.NoError(t, err)
	require.True(t, blockCPUSet["block-isolation"].IsSubsetOf(cpuTopology.CPUDetails.CPUsInNUMANodes(0)),
		"NUMA-bound isolation fallback should stay within NUMA0, got %s",
		blockCPUSet["block-isolation"].String())
	require.True(t, outsideNUMAAvailableCPUs.IsSubsetOf(availableCPUs),
		"CPUs outside NUMA0 must not be consumed by fallback, available=%s",
		availableCPUs.String())
}

func TestDynamicPolicy_allocateShareBlocks_preservesRealNUMASourceResult(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)

	tmpDir, err := ioutil.TempDir("", "checkpoint-TestDynamicPolicy_allocateShareBlocks_preservesRealNUMASourceResult")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, tmpDir)
	require.NoError(t, err)
	p.state.SetPodEntries(state.PodEntries{
		"pod1": state.ContainerEntries{
			"c": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "pod1",
					ContainerName: "c",
					QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
					OwnerPoolName: commonstate.PoolNamePrefixIsolation + "-pod1",
					Annotations: map[string]string{
						apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
						"numa_hint": "0",
					},
				},
			},
		},
	}, false)

	sourcePoolName := commonstate.PoolNameShare + commonstate.NUMAPoolInfix + "0"
	blockCPUSet := advisorapi.BlockCPUSet{}
	availableCPUs := machine.NewCPUSet(0, 1, 2, 3, 8, 9)
	nodeRemainingCPUs := availableCPUs.Clone()
	blocks := []*advisorapi.BlockInfo{
		{
			Block: advisorapi.Block{BlockId: "block-share-numa0", Result: 4},
			OwnerPoolEntryMap: map[string]advisorapi.BlockEntry{
				sourcePoolName: {
					EntryName:    sourcePoolName,
					SubEntryName: commonstate.FakedContainerName,
				},
			},
		},
		{
			Block: advisorapi.Block{BlockId: "block-isolation", Result: 2},
			OwnerPoolEntryMap: map[string]advisorapi.BlockEntry{
				commonstate.PoolNamePrefixIsolation + "-pod1": {
					EntryName:    "pod1",
					SubEntryName: "c",
				},
			},
		},
	}

	err = p.allocateShareBlocks(
		0,
		blocks,
		blockCPUSet,
		cpuTopology.CPUDetails.CPUsInNUMANodes(0),
		&nodeRemainingCPUs,
		&availableCPUs,
		nil,
		machine.NewCPUSet(),
		nil,
		map[string]string{sourcePoolName: "block-share-numa0"},
	)
	require.NoError(t, err)
	require.Equal(t, 4, blockCPUSet["block-share-numa0"].Size(),
		"source block must keep its advisor result after isolation carve, got %s",
		blockCPUSet["block-share-numa0"].String())
	require.Equal(t, 2, blockCPUSet["block-isolation"].Size())
	require.True(t, blockCPUSet["block-share-numa0"].Intersection(blockCPUSet["block-isolation"]).IsEmpty())
}

func TestDynamicPolicy_allocateAdvisorSourceBlocksForCarve(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)

	tmpDir, err := ioutil.TempDir("", "checkpoint-TestDynamicPolicy_allocateAdvisorSourceBlocksForCarve")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, tmpDir)
	require.NoError(t, err)
	p.state.SetPodEntries(state.PodEntries{
		"pod1": state.ContainerEntries{
			"c": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "pod1",
					ContainerName: "c",
					QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
					OwnerPoolName: commonstate.PoolNamePrefixIsolation + "-pod1",
					Annotations:   map[string]string{},
				},
			},
		},
	}, false)

	blockCPUSet := advisorapi.NewBlockCPUSet()
	availableCPUs := machine.NewCPUSet(0, 1, 2, 3, 8, 9, 10, 11)
	nodeRemainingCPUs := availableCPUs.Clone()
	reclaimBlocks := []*advisorapi.BlockInfo{
		{
			Block: advisorapi.Block{BlockId: "block-share", Result: 4},
			OwnerPoolEntryMap: map[string]advisorapi.BlockEntry{
				commonstate.PoolNameShare: {
					EntryName:    commonstate.PoolNameShare,
					SubEntryName: commonstate.FakedContainerName,
				},
			},
		},
	}
	isolationBlocks := []*advisorapi.BlockInfo{
		{
			Block: advisorapi.Block{BlockId: "block-isolation", Result: 2},
			OwnerPoolEntryMap: map[string]advisorapi.BlockEntry{
				commonstate.PoolNamePrefixIsolation + "-pod1": {
					EntryName:    "pod1",
					SubEntryName: "c",
				},
			},
		},
	}
	sourceBlockByPool := map[string]string{
		commonstate.PoolNameShare: "block-share",
	}

	err = p.allocateAdvisorSourceBlocksForCarve(
		reclaimBlocks, isolationBlocks, blockCPUSet, &availableCPUs, &nodeRemainingCPUs, machine.NewCPUSet(), sourceBlockByPool)
	require.NoError(t, err)
	require.Equal(t, 6, blockCPUSet["block-share"].Size(),
		"source share block should be preallocated with share + isolation quantity")
	require.Equal(t, 2, availableCPUs.Size())
	require.True(t, blockCPUSet["block-share"].Union(availableCPUs).Equals(machine.NewCPUSet(0, 1, 2, 3, 8, 9, 10, 11)))
}

func TestDynamicPolicy_allocateAdvisorSourceBlocksForCarveAggregatesSharedSourceBlock(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)

	tmpDir, err := ioutil.TempDir("", "checkpoint-TestDynamicPolicy_allocateAdvisorSourceBlocksForCarve_sharedSource")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, tmpDir)
	require.NoError(t, err)
	p.state.SetPodEntries(state.PodEntries{
		"pod-a": {
			"c": &state.AllocationInfo{AllocationMeta: commonstate.AllocationMeta{
				PodUid:        "pod-a",
				ContainerName: "c",
				QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
				OwnerPoolName: commonstate.PoolNamePrefixIsolation + "-pod-a",
				Annotations: map[string]string{
					apiconsts.PodAnnotationCPUEnhancementCPUSet: "a",
				},
			}},
		},
		"pod-b": {
			"c": &state.AllocationInfo{AllocationMeta: commonstate.AllocationMeta{
				PodUid:        "pod-b",
				ContainerName: "c",
				QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
				OwnerPoolName: commonstate.PoolNamePrefixIsolation + "-pod-b",
				Annotations: map[string]string{
					apiconsts.PodAnnotationCPUEnhancementCPUSet: "b",
				},
			}},
		},
	}, false)

	const (
		sourceBlockID = "block-shared-source"
		sourcePoolA   = "a"
		sourcePoolB   = "b"
	)
	sourceBlocks := []*advisorapi.BlockInfo{{
		Block: advisorapi.Block{BlockId: sourceBlockID, Result: 4},
		OwnerPoolEntryMap: map[string]advisorapi.BlockEntry{
			sourcePoolA: {EntryName: sourcePoolA, SubEntryName: commonstate.FakedContainerName},
			sourcePoolB: {EntryName: sourcePoolB, SubEntryName: commonstate.FakedContainerName},
		},
	}}
	isolationBlocks := []*advisorapi.BlockInfo{
		{
			Block: advisorapi.Block{BlockId: "block-isolation-a", Result: 2},
			OwnerPoolEntryMap: map[string]advisorapi.BlockEntry{
				commonstate.PoolNamePrefixIsolation + "-pod-a": {EntryName: "pod-a", SubEntryName: "c"},
			},
		},
		{
			Block: advisorapi.Block{BlockId: "block-isolation-b", Result: 2},
			OwnerPoolEntryMap: map[string]advisorapi.BlockEntry{
				commonstate.PoolNamePrefixIsolation + "-pod-b": {EntryName: "pod-b", SubEntryName: "c"},
			},
		},
	}
	blockCPUSet := advisorapi.NewBlockCPUSet()
	availableCPUs := machine.NewCPUSet(0, 1, 2, 3, 8, 9, 10, 11)
	nodeRemainingCPUs := availableCPUs.Clone()

	err = p.allocateAdvisorSourceBlocksForCarve(
		sourceBlocks,
		isolationBlocks,
		blockCPUSet,
		&availableCPUs,
		&nodeRemainingCPUs,
		machine.NewCPUSet(),
		map[string]string{
			sourcePoolA: sourceBlockID,
			sourcePoolB: sourceBlockID,
		},
	)
	require.NoError(t, err)
	require.Equal(t, 8, blockCPUSet[sourceBlockID].Size(),
		"shared source block must reserve source result plus isolation demand from every mapped pool")
	require.True(t, availableCPUs.IsEmpty())
}

func TestDynamicPolicy_allocateAdvisorSourceBlocksForCarveReturnsErrorWhenInsufficient(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)

	tmpDir, err := ioutil.TempDir("", "checkpoint-TestDynamicPolicy_allocateAdvisorSourceBlocksForCarve_insufficient")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, tmpDir)
	require.NoError(t, err)
	p.state.SetPodEntries(state.PodEntries{
		"pod1": state.ContainerEntries{
			"c": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "pod1",
					ContainerName: "c",
					QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
					OwnerPoolName: commonstate.PoolNamePrefixIsolation + "-pod1",
					Annotations:   map[string]string{},
				},
			},
		},
	}, false)

	blockCPUSet := advisorapi.NewBlockCPUSet()
	availableCPUs := machine.NewCPUSet(0, 1, 2)
	nodeRemainingCPUs := availableCPUs.Clone()
	err = p.allocateAdvisorSourceBlocksForCarve(
		[]*advisorapi.BlockInfo{{
			Block: advisorapi.Block{BlockId: "block-share", Result: 2},
			OwnerPoolEntryMap: map[string]advisorapi.BlockEntry{
				commonstate.PoolNameShare: {
					EntryName:    commonstate.PoolNameShare,
					SubEntryName: commonstate.FakedContainerName,
				},
			},
		}},
		[]*advisorapi.BlockInfo{{
			Block: advisorapi.Block{BlockId: "block-isolation", Result: 2},
			OwnerPoolEntryMap: map[string]advisorapi.BlockEntry{
				commonstate.PoolNamePrefixIsolation + "-pod1": {
					EntryName:    "pod1",
					SubEntryName: "c",
				},
			},
		}},
		blockCPUSet,
		&availableCPUs,
		&nodeRemainingCPUs,
		machine.NewCPUSet(),
		map[string]string{commonstate.PoolNameShare: "block-share"})
	require.Error(t, err)
	require.NotContains(t, blockCPUSet, "block-share")
	require.True(t, availableCPUs.Equals(machine.NewCPUSet(0, 1, 2)))
}

func TestDynamicPolicy_allocateAdvisorSourceBlocksForCarveExcludesNonReclaimable(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)

	tmpDir, err := ioutil.TempDir("", "checkpoint-TestDynamicPolicy_allocateAdvisorSourceBlocksForCarve_nonReclaimable")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, tmpDir)
	require.NoError(t, err)
	p.state.SetPodEntries(state.PodEntries{
		"pod1": state.ContainerEntries{
			"c": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "pod1",
					ContainerName: "c",
					QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
					OwnerPoolName: commonstate.PoolNamePrefixIsolation + "-pod1",
					Annotations:   map[string]string{},
				},
			},
		},
	}, false)

	blockCPUSet := advisorapi.NewBlockCPUSet()
	availableCPUs := machine.NewCPUSet(0, 1, 2, 3, 4, 5)
	nodeRemainingCPUs := availableCPUs.Clone()
	nonReclaimableCPUSet := machine.NewCPUSet(0, 1)

	err = p.allocateAdvisorSourceBlocksForCarve(
		[]*advisorapi.BlockInfo{{
			Block: advisorapi.Block{BlockId: "block-share", Result: 2},
			OwnerPoolEntryMap: map[string]advisorapi.BlockEntry{
				commonstate.PoolNameShare: {
					EntryName:    commonstate.PoolNameShare,
					SubEntryName: commonstate.FakedContainerName,
				},
			},
		}},
		[]*advisorapi.BlockInfo{{
			Block: advisorapi.Block{BlockId: "block-isolation", Result: 2},
			OwnerPoolEntryMap: map[string]advisorapi.BlockEntry{
				commonstate.PoolNamePrefixIsolation + "-pod1": {
					EntryName:    "pod1",
					SubEntryName: "c",
				},
			},
		}},
		blockCPUSet,
		&availableCPUs,
		&nodeRemainingCPUs,
		nonReclaimableCPUSet,
		map[string]string{commonstate.PoolNameShare: "block-share"})
	require.NoError(t, err)
	require.True(t, blockCPUSet["block-share"].Intersection(nonReclaimableCPUSet).IsEmpty(),
		"source preallocation must exclude non-reclaimable CPUs, got %s",
		blockCPUSet["block-share"].String())
	require.True(t, availableCPUs.Intersection(nonReclaimableCPUSet).Equals(nonReclaimableCPUSet),
		"non-reclaimable CPUs should remain available for their pinned owners, available=%s",
		availableCPUs.String())
}

func TestDynamicPolicy_generateBlockCPUSet_combinedCarvesIsolationFromNormalShare(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)

	tmpDir, err := ioutil.TempDir("", "checkpoint-TestDynamicPolicy_generateBlockCPUSet_combinedCarvesIsolation")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	p, err := getTestDynamicPolicyWithInitialization(cpuTopology, tmpDir)
	require.NoError(t, err)
	p.state.SetPodEntries(state.PodEntries{
		"pod1": state.ContainerEntries{
			"c": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "pod1",
					ContainerName: "c",
					QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
					OwnerPoolName: commonstate.PoolNamePrefixIsolation + "-pod1",
					Annotations:   map[string]string{},
				},
			},
		},
	}, false)

	resp := &advisorapi.ListAndWatchResponse{
		Entries: map[string]*advisorapi.CalculationEntries{
			"pod1": {
				Entries: map[string]*advisorapi.CalculationInfo{
					"c": {
						OwnerPoolName: commonstate.PoolNamePrefixIsolation + "-pod1",
						CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
							commonstate.FakedNUMAID: {
								Blocks: []*advisorapi.Block{{BlockId: "block-isolation", Result: 2}},
							},
						},
					},
				},
			},
			commonstate.PoolNameShare: {
				Entries: map[string]*advisorapi.CalculationInfo{
					commonstate.FakedContainerName: {
						OwnerPoolName: commonstate.PoolNameShare,
						CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
							commonstate.FakedNUMAID: {
								Blocks: []*advisorapi.Block{{BlockId: "block-share", Result: 4}},
							},
						},
					},
				},
			},
			commonstate.PoolNameReclaim: {
				Entries: map[string]*advisorapi.CalculationInfo{
					commonstate.FakedContainerName: {
						OwnerPoolName: commonstate.PoolNameReclaim,
						CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
							commonstate.FakedNUMAID: {
								Blocks: []*advisorapi.Block{{BlockId: "block-reclaim", Result: 4}},
							},
						},
					},
				},
			},
		},
	}

	blockCPUSet, err := p.generateBlockCPUSet(resp, nil)
	require.NoError(t, err)

	share := blockCPUSet["block-share"]
	isolation := blockCPUSet["block-isolation"]
	reclaim := blockCPUSet["block-reclaim"]
	require.Equal(t, 4, share.Size())
	require.Equal(t, 2, isolation.Size())
	require.Equal(t, 4, reclaim.Size())
	require.True(t, share.Intersection(isolation).IsEmpty())
	require.True(t, share.Intersection(reclaim).IsEmpty())
	require.True(t, isolation.Intersection(reclaim).IsEmpty())
	require.Equal(t, 6, share.Union(isolation).Size(),
		"share + isolation should be split from a combined source candidate before reclaim")
}

func TestGenerateBlockCPUSetDisjointPlannerKeepsSourceAndIsolationTogether(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)

	oldSource := machine.NewCPUSet(0, 1, 2, 3)
	oldIsolation := machine.NewCPUSet(8, 9)
	p.state.SetPodEntries(state.PodEntries{
		commonstate.PoolNameShare: {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:           commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameShare),
				AllocationResult:         oldSource,
				OriginalAllocationResult: oldSource,
			},
		},
		"pod-isolation": {
			"main": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "pod-isolation",
					ContainerName: "main",
					QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
					OwnerPoolName: commonstate.PoolNamePrefixIsolation + "-pod-isolation",
					Annotations:   map[string]string{},
				},
				AllocationResult:         oldIsolation,
				OriginalAllocationResult: oldIsolation,
			},
		},
	}, false)

	resp := &advisorapi.ListAndWatchResponse{
		DisableDedicatedCoresOverlapReclaimedCores: true,
		Entries: map[string]*advisorapi.CalculationEntries{
			commonstate.PoolNameShare: {
				Entries: map[string]*advisorapi.CalculationInfo{
					commonstate.FakedContainerName: {
						OwnerPoolName: commonstate.PoolNameShare,
						CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
							commonstate.FakedNUMAID: {
								Blocks: []*advisorapi.Block{{BlockId: "rotated-source", Result: 4}},
							},
						},
					},
				},
			},
			"pod-isolation": {
				Entries: map[string]*advisorapi.CalculationInfo{
					"main": {
						OwnerPoolName: commonstate.PoolNamePrefixIsolation + "-pod-isolation",
						CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
							commonstate.FakedNUMAID: {
								Blocks: []*advisorapi.Block{{BlockId: "rotated-isolation", Result: 2}},
							},
						},
					},
				},
			},
		},
	}
	featureGates := map[string]*advisorsvc.FeatureGate{
		feature_cpu.NegotiationFeatureGateDedicatedReclaimDisjointPartition: {
			Name: feature_cpu.NegotiationFeatureGateDedicatedReclaimDisjointPartition,
		},
	}

	got, err := p.generateBlockCPUSet(resp, featureGates)
	require.NoError(t, err)
	require.Equal(t, oldSource, got["rotated-source"])
	require.Equal(t, oldIsolation, got["rotated-isolation"])
	require.Equal(t, oldSource.Union(oldIsolation),
		got["rotated-source"].Union(got["rotated-isolation"]))

	sourceBlock := resp.Entries[commonstate.PoolNameShare].Entries[commonstate.FakedContainerName].
		CalculationResultsByNumas[commonstate.FakedNUMAID].Blocks[0]
	sourceBlock.Result = 5
	grown, err := p.generateBlockCPUSet(resp, featureGates)
	require.NoError(t, err)
	require.True(t, oldSource.IsSubsetOf(grown["rotated-source"]))
	require.Equal(t, 1, oldSource.Difference(grown["rotated-source"]).
		Union(grown["rotated-source"].Difference(oldSource)).Size())
	require.Equal(t, oldIsolation, grown["rotated-isolation"])

	sourceBlock.Result = 3
	shrunk, err := p.generateBlockCPUSet(resp, featureGates)
	require.NoError(t, err)
	require.True(t, shrunk["rotated-source"].IsSubsetOf(oldSource))
	require.Equal(t, 1, oldSource.Difference(shrunk["rotated-source"]).
		Union(shrunk["rotated-source"].Difference(oldSource)).Size())
	require.Equal(t, oldIsolation, shrunk["rotated-isolation"])
}

func TestGenerateBlockCPUSetDisjointPlannerIndexesShareNUMASourceWithRPIsolation(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)

	const resourcePackageName = "rp-a"
	sourcePool := commonstate.PoolNameShare + commonstate.NUMAPoolInfix + "0"
	wrappedSourcePool := resourcepackage.WrapOwnerPoolName(sourcePool, resourcePackageName)
	isolationPool := commonstate.PoolNamePrefixIsolation + "-pod-numa0"
	wrappedIsolationPool := resourcepackage.WrapOwnerPoolName(isolationPool, resourcePackageName)
	numa0CPUs := cpuTopology.CPUDetails.CPUsInNUMANodes(0)
	oldSource := machine.NewCPUSet(0, 1, 2, 3)
	oldIsolation := machine.NewCPUSet(8, 9)

	machineState := p.state.GetMachineState()
	machineState[0].ResourcePackageStates = map[string]*state.ResourcePackageState{
		resourcePackageName: {PinnedCPUSet: numa0CPUs},
	}
	p.state.SetMachineState(machineState, false)
	p.state.SetPodEntries(state.PodEntries{
		wrappedSourcePool: {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:           commonstate.GenerateGenericPoolAllocationMeta(wrappedSourcePool),
				AllocationResult:         oldSource,
				OriginalAllocationResult: oldSource,
			},
		},
		"pod-numa0": {
			"main": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "pod-numa0",
					ContainerName: "main",
					QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
					OwnerPoolName: wrappedIsolationPool,
					Annotations: map[string]string{
						apiconsts.PodAnnotationMemoryEnhancementNumaBinding: apiconsts.PodAnnotationMemoryEnhancementNumaBindingEnable,
						cpuconsts.CPUStateAnnotationKeyNUMAHint:             "0",
					},
				},
				AllocationResult:         oldIsolation,
				OriginalAllocationResult: oldIsolation,
				TopologyAwareAssignments: map[int]machine.CPUSet{
					0: oldIsolation,
				},
			},
		},
	}, false)

	resp := &advisorapi.ListAndWatchResponse{
		DisableDedicatedCoresOverlapReclaimedCores: true,
		Entries: map[string]*advisorapi.CalculationEntries{
			wrappedSourcePool: {
				Entries: map[string]*advisorapi.CalculationInfo{
					commonstate.FakedContainerName: {
						OwnerPoolName: wrappedSourcePool,
						CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
							0: {Blocks: []*advisorapi.Block{{BlockId: "rotated-numa-source", Result: 4}}},
						},
					},
				},
			},
			"pod-numa0": {
				Entries: map[string]*advisorapi.CalculationInfo{
					"main": {
						OwnerPoolName: wrappedIsolationPool,
						CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
							0: {Blocks: []*advisorapi.Block{{BlockId: "rotated-numa-isolation", Result: 2}}},
						},
					},
				},
			},
		},
	}

	descriptors, err := buildAdvisorBlockDescriptors(
		resp,
		cpuTopology.CPUDetails,
		p.state.GetPodEntries(),
		map[string]machine.CPUSet{resourcePackageName: numa0CPUs},
		machine.NewCPUSet(),
	)
	require.NoError(t, err)
	keys, members, err := p.advisorSourceIsolationComponents(descriptors)
	require.NoError(t, err)
	require.Equal(t, []string{"rotated-numa-source"}, keys,
		"share-NUMA0 source descriptor must enter the source domain index")
	require.ElementsMatch(t,
		[]string{"rotated-numa-source", "rotated-numa-isolation"},
		advisorDescriptorBlockIDs(members["rotated-numa-source"]),
	)

	featureGates := map[string]*advisorsvc.FeatureGate{
		feature_cpu.NegotiationFeatureGateDedicatedReclaimDisjointPartition: {
			Name: feature_cpu.NegotiationFeatureGateDedicatedReclaimDisjointPartition,
		},
	}
	got, err := p.generateBlockCPUSet(resp, featureGates)
	require.NoError(t, err)
	require.Equal(t, oldSource, got["rotated-numa-source"])
	require.Equal(t, oldIsolation, got["rotated-numa-isolation"])
	require.Equal(t, oldSource.Union(oldIsolation),
		got["rotated-numa-source"].Union(got["rotated-numa-isolation"]))
}

func TestAdvisorSourceIsolationComponentsScopeSourceByNUMAAndResourcePackage(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)

	const isolationPool = "isolation-workload"
	p.state.SetPodEntries(state.PodEntries{
		"pod-a0": {"main": &state.AllocationInfo{AllocationMeta: commonstate.AllocationMeta{
			QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
			OwnerPoolName: resourcepackage.WrapOwnerPoolName(isolationPool, "rp-a"),
			Annotations:   map[string]string{apiconsts.PodAnnotationCPUEnhancementCPUSet: "source"},
		}}},
		"pod-a1": {"main": &state.AllocationInfo{AllocationMeta: commonstate.AllocationMeta{
			QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
			OwnerPoolName: resourcepackage.WrapOwnerPoolName(isolationPool, "rp-a"),
			Annotations:   map[string]string{apiconsts.PodAnnotationCPUEnhancementCPUSet: "source"},
		}}},
		"pod-b0": {"main": &state.AllocationInfo{AllocationMeta: commonstate.AllocationMeta{
			QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
			OwnerPoolName: resourcepackage.WrapOwnerPoolName(isolationPool, "rp-b"),
			Annotations:   map[string]string{apiconsts.PodAnnotationCPUEnhancementCPUSet: "source"},
		}}},
	}, false)

	owner := func(pool, entry, rp string) string {
		return canonicalAdvisorBlockOwner(pool, entry, "main", rp)
	}
	descriptors := []advisorBlockDescriptor{
		{BlockID: "source-a0", Class: advisorBlockClassShared, NUMAID: 0, Owners: []string{owner("source", "source-a0", "rp-a")}, ComponentKey: "source-a0"},
		{BlockID: "isolation-a0", Class: advisorBlockClassShared, NUMAID: 0, Owners: []string{owner(isolationPool, "pod-a0", "rp-a")}, ComponentKey: "isolation-a0"},
		{BlockID: "source-b0", Class: advisorBlockClassShared, NUMAID: 0, Owners: []string{owner("source", "source-b0", "rp-b")}, ComponentKey: "source-b0"},
		{BlockID: "isolation-b0", Class: advisorBlockClassShared, NUMAID: 0, Owners: []string{owner(isolationPool, "pod-b0", "rp-b")}, ComponentKey: "isolation-b0"},
		{BlockID: "source-a1", Class: advisorBlockClassShared, NUMAID: 1, Owners: []string{owner("source", "source-a1", "rp-a")}, ComponentKey: "source-a1"},
		{BlockID: "isolation-a1", Class: advisorBlockClassShared, NUMAID: 1, Owners: []string{owner(isolationPool, "pod-a1", "rp-a")}, ComponentKey: "isolation-a1"},
	}
	keys, members, err := p.advisorSourceIsolationComponents(descriptors)
	require.NoError(t, err)
	require.Equal(t, []string{"source-a0", "source-b0", "source-a1"}, keys)
	require.ElementsMatch(t, []string{"source-a0", "isolation-a0"}, advisorDescriptorBlockIDs(members["source-a0"]))
	require.ElementsMatch(t, []string{"source-b0", "isolation-b0"}, advisorDescriptorBlockIDs(members["source-b0"]))
	require.ElementsMatch(t, []string{"source-a1", "isolation-a1"}, advisorDescriptorBlockIDs(members["source-a1"]))
}

func TestPlanDisjointAdvisorBlocksRejectsConflictingSourceDomainRegardlessOfMapOrder(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)

	sourceEntry := func(blockID string) *advisorapi.CalculationEntries {
		return &advisorapi.CalculationEntries{Entries: map[string]*advisorapi.CalculationInfo{
			commonstate.FakedContainerName: {
				OwnerPoolName: "source-pool",
				CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
					0: {Blocks: []*advisorapi.Block{{BlockId: blockID, Result: 2}}},
				},
			},
		}}
	}
	for _, tc := range []struct {
		name       string
		entryOrder []string
		blockIDs   map[string]string
	}{
		{
			name:       "source-a-map-entry-first",
			entryOrder: []string{"entry-a", "entry-b"},
			blockIDs:   map[string]string{"entry-a": "source-a", "entry-b": "source-b"},
		},
		{
			name:       "source-b-map-entry-first",
			entryOrder: []string{"entry-b", "entry-a"},
			blockIDs:   map[string]string{"entry-a": "source-b", "entry-b": "source-a"},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			entries := make(map[string]*advisorapi.CalculationEntries, len(tc.entryOrder))
			for _, entryName := range tc.entryOrder {
				entries[entryName] = sourceEntry(tc.blockIDs[entryName])
			}
			resp := &advisorapi.ListAndWatchResponse{
				DisableDedicatedCoresOverlapReclaimedCores: true,
				Entries: entries,
			}

			got, err := p.planDisjointAdvisorBlocks(resp)
			require.EqualError(t, err,
				`source domain pool "source-pool" resource package "" numa 0 has conflicting descriptors "source-a" and "source-b"`)
			require.Nil(t, got, "fail-closed errors must not expose partial block results")
		})
	}
}

func TestPlanDisjointAdvisorBlocksAllowsRepeatedSourceBlockIDAfterNormalization(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)

	sourceEntry := func() *advisorapi.CalculationEntries {
		return &advisorapi.CalculationEntries{Entries: map[string]*advisorapi.CalculationInfo{
			commonstate.FakedContainerName: {
				OwnerPoolName: "source-pool",
				CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
					0: {Blocks: []*advisorapi.Block{{BlockId: "source", Result: 2}}},
				},
			},
		}}
	}
	resp := &advisorapi.ListAndWatchResponse{
		DisableDedicatedCoresOverlapReclaimedCores: true,
		Entries: map[string]*advisorapi.CalculationEntries{
			"entry-a": sourceEntry(),
			"entry-b": sourceEntry(),
		},
	}

	got, err := p.planDisjointAdvisorBlocks(resp)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, 2, got["source"].Size())
}

func TestAdvisorSourceIsolationComponentsRejectsConflictingAliasSourcesRegardlessOfOwnerOrder(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)

	const isolationPool = "isolation-workload"
	p.state.SetPodEntries(state.PodEntries{
		"pod-a": {"main": &state.AllocationInfo{AllocationMeta: commonstate.AllocationMeta{
			QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
			OwnerPoolName: isolationPool,
			Annotations:   map[string]string{apiconsts.PodAnnotationCPUEnhancementCPUSet: "source-a"},
		}}},
		"pod-b": {"main": &state.AllocationInfo{AllocationMeta: commonstate.AllocationMeta{
			QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
			OwnerPoolName: isolationPool,
			Annotations:   map[string]string{apiconsts.PodAnnotationCPUEnhancementCPUSet: "source-b"},
		}}},
	}, false)

	ownerA := canonicalAdvisorBlockOwner(isolationPool, "pod-a", "main", "")
	ownerB := canonicalAdvisorBlockOwner(isolationPool, "pod-b", "main", "")
	for _, owners := range [][]string{{ownerA, ownerB}, {ownerB, ownerA}} {
		descriptors := []advisorBlockDescriptor{
			{BlockID: "source-a", Class: advisorBlockClassShared, NUMAID: 0,
				Owners: []string{canonicalAdvisorBlockOwner("source-a", "source-a", "pool", "")}},
			{BlockID: "source-b", Class: advisorBlockClassShared, NUMAID: 0,
				Owners: []string{canonicalAdvisorBlockOwner("source-b", "source-b", "pool", "")}},
			{BlockID: "isolation", Class: advisorBlockClassShared, NUMAID: 0, Owners: owners},
		}
		_, _, err := p.advisorSourceIsolationComponents(descriptors)
		require.ErrorContains(t, err, "aliases resolve to different source domains")
	}
}

func TestAdvisorSourceIsolationComponentsPreservesSamePoolMultiEntryOwnerUnion(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)

	const (
		isolationPool = "isolation-workload"
		sourcePool    = "source"
	)
	oldA := machine.NewCPUSet(0, 1)
	oldB := machine.NewCPUSet(2, 3)
	p.state.SetPodEntries(state.PodEntries{
		"pod-a": {"main": &state.AllocationInfo{
			AllocationMeta: commonstate.AllocationMeta{
				QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
				OwnerPoolName: isolationPool,
				Annotations:   map[string]string{apiconsts.PodAnnotationCPUEnhancementCPUSet: sourcePool},
			},
			AllocationResult: oldA,
		}},
		"pod-b": {"main": &state.AllocationInfo{
			AllocationMeta: commonstate.AllocationMeta{
				QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
				OwnerPoolName: isolationPool,
				Annotations:   map[string]string{apiconsts.PodAnnotationCPUEnhancementCPUSet: sourcePool},
			},
			AllocationResult: oldB,
		}},
	}, false)
	resp := &advisorapi.ListAndWatchResponse{Entries: map[string]*advisorapi.CalculationEntries{
		sourcePool: {Entries: map[string]*advisorapi.CalculationInfo{
			commonstate.FakedContainerName: {
				OwnerPoolName: sourcePool,
				CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
					0: {Blocks: []*advisorapi.Block{{BlockId: "source-block", Result: 4}}},
				},
			},
		}},
		"pod-a": {Entries: map[string]*advisorapi.CalculationInfo{
			"main": {
				OwnerPoolName: isolationPool,
				CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
					0: {Blocks: []*advisorapi.Block{{BlockId: "isolation-block", Result: 4}}},
				},
			},
		}},
		"pod-b": {Entries: map[string]*advisorapi.CalculationInfo{
			"main": {
				OwnerPoolName: isolationPool,
				CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
					0: {Blocks: []*advisorapi.Block{{BlockId: "isolation-block", Result: 4}}},
				},
			},
		}},
	}}
	descriptors, err := buildAdvisorBlockDescriptors(
		resp, cpuTopology.CPUDetails, p.state.GetPodEntries(), nil, machine.NewCPUSet())
	require.NoError(t, err)

	var isolation advisorBlockDescriptor
	for _, descriptor := range descriptors {
		if descriptor.BlockID == "isolation-block" {
			isolation = descriptor
		}
	}
	require.Len(t, isolation.Owners, 2)
	require.Equal(t, oldA.Union(oldB), isolation.OldPreferred)

	keys, members, err := p.advisorSourceIsolationComponents(descriptors)
	require.NoError(t, err)
	require.Equal(t, []string{"source-block"}, keys)
	require.ElementsMatch(t, []string{"source-block", "isolation-block"},
		advisorDescriptorBlockIDs(members["source-block"]))
}

func TestGenerateBlockCPUSetDisjointPlannerAllocatesRealNUMAComponentBeforeFake(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)
	p.state.SetPodEntries(state.PodEntries{
		"pod-real": {"main": &state.AllocationInfo{AllocationMeta: commonstate.AllocationMeta{
			QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
			OwnerPoolName: "isolation-real",
			Annotations:   map[string]string{apiconsts.PodAnnotationCPUEnhancementCPUSet: "source-real"},
		}}},
		"pod-fake": {"main": &state.AllocationInfo{AllocationMeta: commonstate.AllocationMeta{
			QoSLevel:      apiconsts.PodAnnotationQoSLevelSharedCores,
			OwnerPoolName: "isolation-fake",
			Annotations:   map[string]string{apiconsts.PodAnnotationCPUEnhancementCPUSet: "source-fake"},
		}}},
	}, false)

	resp := advisorSourceIsolationTestResponse(map[string]struct {
		sourcePool     string
		isolationPool  string
		pod            string
		numaID         int64
		sourceBlock    string
		isolationBlock string
	}{
		"real": {"source-real", "isolation-real", "pod-real", 0, "z-real-source", "z-real-isolation"},
		"fake": {"source-fake", "isolation-fake", "pod-fake", commonstate.FakedNUMAID, "a-fake-source", "a-fake-isolation"},
	})
	featureGates := map[string]*advisorsvc.FeatureGate{
		feature_cpu.NegotiationFeatureGateDedicatedReclaimDisjointPartition: {
			Name: feature_cpu.NegotiationFeatureGateDedicatedReclaimDisjointPartition,
		},
	}

	got, err := p.generateBlockCPUSet(resp, featureGates)
	require.NoError(t, err)
	realUnion := got["z-real-source"].Union(got["z-real-isolation"])
	fakeUnion := got["a-fake-source"].Union(got["a-fake-isolation"])
	require.Equal(t, cpuTopology.CPUDetails.CPUsInNUMANodes(0), realUnion)
	require.Equal(t, cpuTopology.CPUDetails.CPUsInNUMANodes(1), fakeUnion)
}

func TestSolveAdvisorDescriptorPhaseBlockIDRotationPreservesGrowOwnerUnion(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)
	allCPUs := cpuTopology.CPUDetails.CPUs()

	solve := func(firstID, secondID string) map[string]machine.CPUSet {
		descriptors := []advisorBlockDescriptor{
			{BlockID: firstID, Owners: []string{"owner-a"}, Class: advisorBlockClassShared, NUMAID: commonstate.FakedNUMAID,
				Quantity: 4, ComponentKey: "component-a", Eligible: allCPUs, OldPreferred: machine.NewCPUSet(0)},
			{BlockID: secondID, Owners: []string{"owner-b"}, Class: advisorBlockClassShared, NUMAID: commonstate.FakedNUMAID,
				Quantity: 4, ComponentKey: "component-b", Eligible: allCPUs, OldPreferred: machine.NewCPUSet(1)},
		}
		result := advisorapi.NewBlockCPUSet()
		_, err := p.solveAdvisorDescriptorPhase(descriptors, allCPUs, result, false)
		require.NoError(t, err)
		return map[string]machine.CPUSet{
			"owner-a": result[firstID],
			"owner-b": result[secondID],
		}
	}

	require.Equal(t, solve("z-rotated-a", "a-rotated-b"), solve("a-next-a", "z-next-b"))
}

func TestPlanDisjointAdvisorBlocksOverlapNeverReintroducesStateForbiddenOrSystemCPUs(t *testing.T) {
	t.Parallel()

	cpuTopology, err := machine.GenerateDummyCPUTopology(16, 2, 2)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(cpuTopology, t.TempDir())
	require.NoError(t, err)

	systemPool := commonstate.GetSystemPoolName("latency")
	notAllocatable := machine.NewCPUSet(0, 1, 2, 3)
	p.state.SetPodEntries(state.PodEntries{
		commonstate.PoolNameInterrupt: {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameInterrupt),
				AllocationResult: machine.NewCPUSet(0, 1),
			},
		},
		systemPool: {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(systemPool),
				AllocationResult: machine.NewCPUSet(2, 3),
			},
		},
		commonstate.PoolNameReclaim: {
			commonstate.FakedContainerName: &state.AllocationInfo{
				AllocationMeta:   commonstate.GenerateGenericPoolAllocationMeta(commonstate.PoolNameReclaim),
				AllocationResult: notAllocatable,
			},
		},
	}, false)

	resp := &advisorapi.ListAndWatchResponse{
		DisableDedicatedCoresOverlapReclaimedCores: true,
		Entries: map[string]*advisorapi.CalculationEntries{
			commonstate.PoolNameReclaim: {Entries: map[string]*advisorapi.CalculationInfo{
				commonstate.FakedContainerName: {
					OwnerPoolName: commonstate.PoolNameReclaim,
					CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
						commonstate.FakedNUMAID: {Blocks: []*advisorapi.Block{{
							BlockId: "overlap-reclaim",
							Result:  4,
							OverlapTargets: []*advisorapi.OverlapTarget{{
								OverlapTargetPoolName: commonstate.PoolNameShare,
								OverlapType:           advisorapi.OverlapType_OverlapWithPool,
							}},
						}}},
					},
				},
			}},
		},
	}

	got, err := p.planDisjointAdvisorBlocks(resp)
	require.NoError(t, err)
	require.True(t, got["overlap-reclaim"].Intersection(notAllocatable).IsEmpty(),
		"overlap reclaim must not reintroduce forbidden/system CPUs: got=%s forbidden=%s",
		got["overlap-reclaim"].String(), notAllocatable.String())
}

func TestPlanDisjointAdvisorBlocksBalancesHardReclaim(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	numa0 := topology.CPUDetails.CPUsInNUMANodes(0)
	numa1 := topology.CPUDetails.CPUsInNUMANodes(1)
	allCPUs := numa0.Union(numa1)

	newPolicy := func(t *testing.T, hardPartition bool) *DynamicPolicy {
		t.Helper()
		p, err := getTestDynamicPolicyWithoutInitialization(topology, t.TempDir())
		require.NoError(t, err)
		p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = hardPartition
		p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = hardPartition
		return p
	}
	solve := func(
		t *testing.T,
		p *DynamicPolicy,
		quantity int,
		available, oldPreferred machine.CPUSet,
	) (machine.CPUSet, error) {
		t.Helper()
		result := advisorapi.NewBlockCPUSet()
		_, err := p.solveAdvisorDescriptorPhase([]advisorBlockDescriptor{{
			BlockID:      "reclaim",
			Owners:       []string{"reclaim-owner"},
			Class:        advisorBlockClassMandatoryReclaim,
			NUMAID:       commonstate.FakedNUMAID,
			Quantity:     quantity,
			ComponentKey: "mandatory-reclaim|reclaim-owner|-1",
			Eligible:     allCPUs,
			OldPreferred: oldPreferred,
		}}, available, result, true)
		return result["reclaim"], err
	}

	t.Run("four CPUs are split two per NUMA", func(t *testing.T) {
		got, err := solve(t, newPolicy(t, true), 4, allCPUs, machine.NewCPUSet())
		require.NoError(t, err)
		require.Equal(t, 2, got.Intersection(numa0).Size())
		require.Equal(t, 2, got.Intersection(numa1).Size())
	})

	t.Run("five CPUs use capacity-aware balanced quotas", func(t *testing.T) {
		numa0CPUs, numa1CPUs := numa0.ToSliceInt(), numa1.ToSliceInt()
		available := machine.NewCPUSet(
			numa0CPUs[0], numa0CPUs[1],
			numa1CPUs[0], numa1CPUs[1], numa1CPUs[2],
		)
		got, err := solve(t, newPolicy(t, true), 5, available, machine.NewCPUSet())
		require.NoError(t, err)
		require.Equal(t, 2, got.Intersection(numa0).Size())
		require.Equal(t, 3, got.Intersection(numa1).Size())
	})

	t.Run("old reclaim concentrated on one NUMA is rebalanced", func(t *testing.T) {
		got, err := solve(t, newPolicy(t, true), 4, allCPUs, numa1)
		require.NoError(t, err)
		require.Equal(t, 2, got.Intersection(numa0).Size())
		require.Equal(t, 2, got.Intersection(numa1).Size())
	})

	t.Run("real NUMA reclaim seeds fake NUMA water filling", func(t *testing.T) {
		p := newPolicy(t, true)
		result := advisorapi.NewBlockCPUSet()
		_, err := p.solveAdvisorDescriptorPhase([]advisorBlockDescriptor{
			{
				BlockID: "real-0", Class: advisorBlockClassMandatoryReclaim, NUMAID: 0,
				Quantity: 2, ComponentKey: "real-0", Eligible: numa0,
			},
			{
				BlockID: "fake", Class: advisorBlockClassMandatoryReclaim, NUMAID: commonstate.FakedNUMAID,
				Quantity: 4, ComponentKey: "fake", Eligible: allCPUs,
			},
		}, allCPUs, result, true)
		require.NoError(t, err)
		require.Equal(t, 2, result["real-0"].Intersection(numa0).Size())
		require.Equal(t, 1, result["fake"].Intersection(numa0).Size())
		require.Equal(t, 3, result["fake"].Intersection(numa1).Size())
		require.Equal(t, 3, result["real-0"].Union(result["fake"]).Intersection(numa0).Size())
		require.Equal(t, 3, result["real-0"].Union(result["fake"]).Intersection(numa1).Size())
	})

	t.Run("multiple fake blocks fail closed as a protocol error", func(t *testing.T) {
		p := newPolicy(t, true)
		numa0CPUs, numa1CPUs := numa0.ToSliceInt(), numa1.ToSliceInt()
		available := machine.NewCPUSet(
			numa0CPUs[0], numa0CPUs[1],
			numa1CPUs[0], numa1CPUs[1],
		)
		descriptors := []advisorBlockDescriptor{
			{
				BlockID: "fake-a", Class: advisorBlockClassMandatoryReclaim, NUMAID: commonstate.FakedNUMAID,
				Quantity: 2, ComponentKey: "fake-a", Eligible: available,
			},
			{
				BlockID: "fake-b", Class: advisorBlockClassMandatoryReclaim, NUMAID: commonstate.FakedNUMAID,
				Quantity: 2, ComponentKey: "fake-b", Eligible: available.Intersection(numa0),
			},
		}
		result := advisorapi.NewBlockCPUSet()
		_, err := p.solveAdvisorDescriptorPhase(descriptors, available, result, true)
		require.ErrorContains(t, err,
			"hard reclaim protocol error: expected at most one fake-NUMA mandatory reclaim block, got 2")
		require.Empty(t, result)
	})

	t.Run("real NUMA dedicated load constrains fake reclaim water filling", func(t *testing.T) {
		p := newPolicy(t, true)
		result := advisorapi.NewBlockCPUSet()
		_, err := p.solveAdvisorDescriptorPhase([]advisorBlockDescriptor{
			{
				BlockID: "dedicated-0", Class: advisorBlockClassDedicated, NUMAID: 0,
				Quantity: 2, ComponentKey: "dedicated-0", Eligible: numa0,
			},
			{
				BlockID: "fake", Class: advisorBlockClassMandatoryReclaim, NUMAID: commonstate.FakedNUMAID,
				Quantity: 5, ComponentKey: "fake", Eligible: allCPUs,
			},
		}, allCPUs, result, true)
		require.NoError(t, err)
		require.Equal(t, 2, result["dedicated-0"].Intersection(numa0).Size())
		require.Equal(t, 2, result["fake"].Intersection(numa0).Size())
		require.Equal(t, 3, result["fake"].Intersection(numa1).Size())
	})

	t.Run("real NUMA dedicated load rejects insufficient reclaim capacity", func(t *testing.T) {
		p := newPolicy(t, true)
		result := advisorapi.NewBlockCPUSet()
		_, err := p.solveAdvisorDescriptorPhase([]advisorBlockDescriptor{
			{
				BlockID: "dedicated-0", Class: advisorBlockClassDedicated, NUMAID: 0,
				Quantity: 2, ComponentKey: "dedicated-0", Eligible: numa0,
			},
			{
				BlockID: "fake", Class: advisorBlockClassMandatoryReclaim, NUMAID: commonstate.FakedNUMAID,
				Quantity: 7, ComponentKey: "fake", Eligible: allCPUs,
			},
		}, allCPUs, result, true)
		require.ErrorContains(t, err, "insufficient aggregate capacity")
		require.Empty(t, result)
	})

	t.Run("quantity below hard minimum fails", func(t *testing.T) {
		got, err := solve(t, newPolicy(t, true), 3, allCPUs, machine.NewCPUSet())
		require.ErrorContains(t, err, "smaller than required minimum")
		require.True(t, got.IsEmpty())
	})

	t.Run("hard partition disabled preserves global reclaim allocation", func(t *testing.T) {
		got, err := solve(t, newPolicy(t, false), 4, allCPUs, numa1)
		require.NoError(t, err)
		require.Equal(t, numa1, got)
	})
}

func TestPlanDisjointAdvisorBlocksPreservesCeiledOwnerRequestWhenDonating(t *testing.T) {
	t.Parallel()

	topology, err := machine.GenerateDummyCPUTopology(12, 1, 1)
	require.NoError(t, err)
	p, err := getTestDynamicPolicyWithoutInitialization(topology, t.TempDir())
	require.NoError(t, err)
	p.dynamicConfig.GetDynamicConfiguration().EnableReclaim = true
	p.dynamicConfig.GetDynamicConfiguration().EnableRampUpReclaimHardPartition = true

	allCPUs := topology.CPUDetails.CPUs().ToSliceInt()
	p.state.SetPodEntries(state.PodEntries{
		"dedicated-pod": {
			"main": &state.AllocationInfo{
				AllocationMeta: commonstate.AllocationMeta{
					PodUid:        "dedicated-pod",
					ContainerName: "main",
					OwnerPoolName: commonstate.PoolNameDedicated,
					QoSLevel:      apiconsts.PodAnnotationQoSLevelDedicatedCores,
				},
				AllocationResult: machine.NewCPUSet(allCPUs[:8]...),
				RequestQuantity:  6.2,
			},
		},
	}, false)

	resp := &advisorapi.ListAndWatchResponse{
		DisableDedicatedCoresOverlapReclaimedCores: true,
		Entries: map[string]*advisorapi.CalculationEntries{
			"dedicated-pod": {Entries: map[string]*advisorapi.CalculationInfo{
				"main": {
					OwnerPoolName: commonstate.PoolNameDedicated,
					CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
						0: {Blocks: []*advisorapi.Block{{BlockId: "dedicated", Result: 6}}},
					},
				},
			}},
			commonstate.PoolNameReclaim: {Entries: map[string]*advisorapi.CalculationInfo{
				commonstate.FakedContainerName: {
					OwnerPoolName: commonstate.PoolNameReclaim,
					CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
						0: {Blocks: []*advisorapi.Block{{BlockId: "reclaim", Result: 6}}},
					},
				},
			}},
		},
	}

	result, err := p.planDisjointAdvisorBlocks(resp)
	require.ErrorContains(t, err, "NUMA 0 needs 1 more reclaim CPUs")
	require.Empty(t, result)
}

func advisorDescriptorBlockIDs(descriptors []advisorBlockDescriptor) []string {
	result := make([]string, 0, len(descriptors))
	for _, descriptor := range descriptors {
		result = append(result, descriptor.BlockID)
	}
	return result
}

func advisorSourceIsolationTestResponse(components map[string]struct {
	sourcePool     string
	isolationPool  string
	pod            string
	numaID         int64
	sourceBlock    string
	isolationBlock string
}) *advisorapi.ListAndWatchResponse {
	resp := &advisorapi.ListAndWatchResponse{
		DisableDedicatedCoresOverlapReclaimedCores: true,
		Entries: make(map[string]*advisorapi.CalculationEntries),
	}
	for _, component := range components {
		resp.Entries[component.sourcePool] = &advisorapi.CalculationEntries{Entries: map[string]*advisorapi.CalculationInfo{
			commonstate.FakedContainerName: {
				OwnerPoolName: component.sourcePool,
				CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
					component.numaID: {Blocks: []*advisorapi.Block{{BlockId: component.sourceBlock, Result: 4}}},
				},
			},
		}}
		resp.Entries[component.pod] = &advisorapi.CalculationEntries{Entries: map[string]*advisorapi.CalculationInfo{
			"main": {
				OwnerPoolName: component.isolationPool,
				CalculationResultsByNumas: map[int64]*advisorapi.NumaCalculationResult{
					component.numaID: {Blocks: []*advisorapi.Block{{BlockId: component.isolationBlock, Result: 4}}},
				},
			},
		}}
	}
	return resp
}
