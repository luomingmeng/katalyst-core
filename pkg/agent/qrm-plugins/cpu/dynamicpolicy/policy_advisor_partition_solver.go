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

package dynamicpolicy

import (
	"fmt"
	"math"
	"sort"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type partitionDemand struct {
	key       string
	quantity  int
	eligible  machine.CPUSet
	preferred machine.CPUSet
	class     advisorBlockClass
}

type partitionFlowEdge struct {
	to      int
	reverse int
	cap     int
	cost    int64
}

type partitionAssignmentEdge struct {
	demand int
	edge   int
}

func solveDisjointPartitions(
	demands []partitionDemand,
	topology *machine.CPUTopology,
) (map[string]machine.CPUSet, error) {
	sortedDemands, cpus, total, err := validatePartitionDemands(demands, topology)
	if err != nil {
		return nil, err
	}

	result := make(map[string]machine.CPUSet, len(sortedDemands))
	for _, demand := range sortedDemands {
		result[demand.key] = machine.NewCPUSet()
	}
	if total == 0 {
		return result, nil
	}

	source := 0
	cpuBase := 1
	demandBase := cpuBase + len(cpus)
	sink := demandBase + len(sortedDemands)
	graph := make([][]partitionFlowEdge, sink+1)
	assignmentEdges := make([][]partitionAssignmentEdge, len(cpus))

	dedicatedEligible := machine.NewCPUSet()
	for _, demand := range sortedDemands {
		if demand.class == advisorBlockClassDedicated {
			dedicatedEligible = dedicatedEligible.Union(demand.eligible)
		}
	}
	oldWeight, reclaimWeight, topologyWeight := partitionCostWeights(total, len(cpus), len(sortedDemands))

	for cpuIndex, cpu := range cpus {
		cpuNode := cpuBase + cpuIndex
		addPartitionFlowEdge(graph, source, cpuNode, 1, 0)
		for demandIndex, demand := range sortedDemands {
			if !demand.eligible.Contains(cpu) {
				continue
			}
			cost := partitionEdgeCost(
				cpu, cpuIndex, demandIndex, demand, dedicatedEligible, topology,
				oldWeight, reclaimWeight, topologyWeight, len(sortedDemands),
			)
			edgeIndex := len(graph[cpuNode])
			addPartitionFlowEdge(graph, cpuNode, demandBase+demandIndex, 1, cost)
			assignmentEdges[cpuIndex] = append(assignmentEdges[cpuIndex], partitionAssignmentEdge{
				demand: demandIndex,
				edge:   edgeIndex,
			})
		}
	}
	for demandIndex, demand := range sortedDemands {
		addPartitionFlowEdge(graph, demandBase+demandIndex, sink, demand.quantity, 0)
	}

	if partitionMinCostFlow(graph, source, sink, total) != total {
		return nil, fmt.Errorf("partition demands have no feasible assignment")
	}
	for cpuIndex, edges := range assignmentEdges {
		cpuNode := cpuBase + cpuIndex
		for _, assignment := range edges {
			if graph[cpuNode][assignment.edge].cap == 0 {
				key := sortedDemands[assignment.demand].key
				result[key] = result[key].Union(machine.NewCPUSet(cpus[cpuIndex]))
				break
			}
		}
	}
	return result, nil
}

func validatePartitionDemands(
	demands []partitionDemand,
	topology *machine.CPUTopology,
) ([]partitionDemand, []int, int, error) {
	if topology == nil {
		return nil, nil, 0, fmt.Errorf("partition topology is nil")
	}

	sortedDemands := append([]partitionDemand(nil), demands...)
	sort.Slice(sortedDemands, func(i, j int) bool {
		return sortedDemands[i].key < sortedDemands[j].key
	})

	allEligible := machine.NewCPUSet()
	total := 0
	for i, demand := range sortedDemands {
		if demand.key == "" {
			return nil, nil, 0, fmt.Errorf("partition demand has empty key")
		}
		if i > 0 && sortedDemands[i-1].key == demand.key {
			return nil, nil, 0, fmt.Errorf("partition demand has duplicate key %q", demand.key)
		}
		if demand.quantity < 0 {
			return nil, nil, 0, fmt.Errorf("partition demand %q has negative quantity", demand.key)
		}
		if demand.class != advisorBlockClassDedicated &&
			demand.class != advisorBlockClassMandatoryReclaim {
			return nil, nil, 0, fmt.Errorf("partition demand %q has unsupported class %q", demand.key, demand.class)
		}
		if demand.eligible.Size() < demand.quantity {
			return nil, nil, 0, fmt.Errorf("partition demands have no feasible assignment")
		}
		for _, cpu := range demand.eligible.ToSliceInt() {
			if _, ok := topology.CPUDetails[cpu]; !ok {
				return nil, nil, 0, fmt.Errorf("partition demand %q references CPU %d missing from topology", demand.key, cpu)
			}
		}
		allEligible = allEligible.Union(demand.eligible)
		total += demand.quantity
		if total < 0 {
			return nil, nil, 0, fmt.Errorf("partition demand quantity overflow")
		}
	}
	if allEligible.Size() < total {
		return nil, nil, 0, fmt.Errorf("partition demands have no feasible assignment")
	}
	return sortedDemands, allEligible.ToSliceInt(), total, nil
}

func partitionCostWeights(total, cpuCount, demandCount int) (int64, int64, int64) {
	maxTiePerEdge := int64(cpuCount*(demandCount+1) + demandCount + 1)
	maxTieTotal := int64(total) * maxTiePerEdge
	topologyWeight := maxTieTotal + 1
	reclaimWeight := int64(total)*3*topologyWeight + maxTieTotal + 1
	oldWeight := int64(total)*reclaimWeight + int64(total)*3*topologyWeight + maxTieTotal + 1
	return oldWeight, reclaimWeight, topologyWeight
}

func partitionEdgeCost(
	cpu, cpuRank, demandRank int,
	demand partitionDemand,
	dedicatedEligible machine.CPUSet,
	topology *machine.CPUTopology,
	oldWeight, reclaimWeight, topologyWeight int64,
	demandCount int,
) int64 {
	var cost int64
	if !demand.preferred.Contains(cpu) {
		cost += oldWeight
	}
	if demand.class == advisorBlockClassMandatoryReclaim && dedicatedEligible.Contains(cpu) {
		cost += reclaimWeight
	}
	cost += int64(partitionTopologyDistance(cpu, demand.preferred, topology)) * topologyWeight
	cost += int64(cpuRank*(demandCount+1) + demandRank + 1)
	return cost
}

func partitionTopologyDistance(cpu int, preferred machine.CPUSet, topology *machine.CPUTopology) int {
	candidate := topology.CPUDetails[cpu]
	distance := 3
	for _, oldCPU := range preferred.ToSliceInt() {
		old, ok := topology.CPUDetails[oldCPU]
		if !ok {
			continue
		}
		switch {
		case candidate.SocketID == old.SocketID && candidate.CoreID == old.CoreID:
			return 0
		case candidate.NUMANodeID == old.NUMANodeID:
			if distance > 1 {
				distance = 1
			}
		case candidate.SocketID == old.SocketID:
			if distance > 2 {
				distance = 2
			}
		}
	}
	return distance
}

func addPartitionFlowEdge(graph [][]partitionFlowEdge, from, to, cap int, cost int64) {
	forward := partitionFlowEdge{to: to, reverse: len(graph[to]), cap: cap, cost: cost}
	reverse := partitionFlowEdge{to: from, reverse: len(graph[from]), cap: 0, cost: -cost}
	graph[from] = append(graph[from], forward)
	graph[to] = append(graph[to], reverse)
}

func partitionMinCostFlow(graph [][]partitionFlowEdge, source, sink, wanted int) int {
	flow := 0
	for flow < wanted {
		distance := make([]int64, len(graph))
		previousNode := make([]int, len(graph))
		previousEdge := make([]int, len(graph))
		for i := range distance {
			distance[i] = math.MaxInt64
			previousNode[i] = -1
		}
		distance[source] = 0

		for iteration := 0; iteration < len(graph)-1; iteration++ {
			changed := false
			for node := range graph {
				if distance[node] == math.MaxInt64 {
					continue
				}
				for edgeIndex, edge := range graph[node] {
					if edge.cap == 0 ||
						(edge.cost > 0 && distance[node] > math.MaxInt64-edge.cost) ||
						(edge.cost < 0 && distance[node] < math.MinInt64-edge.cost) {
						continue
					}
					nextDistance := distance[node] + edge.cost
					if nextDistance < distance[edge.to] {
						distance[edge.to] = nextDistance
						previousNode[edge.to] = node
						previousEdge[edge.to] = edgeIndex
						changed = true
					}
				}
			}
			if !changed {
				break
			}
		}
		if previousNode[sink] == -1 {
			break
		}

		for node := sink; node != source; node = previousNode[node] {
			from := previousNode[node]
			edgeIndex := previousEdge[node]
			reverse := graph[from][edgeIndex].reverse
			graph[from][edgeIndex].cap--
			graph[node][reverse].cap++
		}
		flow++
	}
	return flow
}
