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
	"container/heap"
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

const (
	// The edge budget admits a fully connected 1024 CPU x 1024 demand graph,
	// while bounding memory before an unexpectedly large eligibility matrix is built.
	partitionAssignmentEdgeBudget = 1024 * 1024
	// Every residual-edge examination in shortest-path and admissible-flow scans
	// consumes this budget, bounding pathological successive-shortest-path inputs.
	partitionFlowOperationBudget = 100_000_000
)

type partitionDistanceItem struct {
	node     int
	distance int64
}

type partitionDistanceQueue []partitionDistanceItem

func (q partitionDistanceQueue) Len() int { return len(q) }

func (q partitionDistanceQueue) Less(i, j int) bool {
	if q[i].distance != q[j].distance {
		return q[i].distance < q[j].distance
	}
	return q[i].node < q[j].node
}

func (q partitionDistanceQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

func (q *partitionDistanceQueue) Push(value interface{}) {
	*q = append(*q, value.(partitionDistanceItem))
}

func (q *partitionDistanceQueue) Pop() interface{} {
	old := *q
	last := len(old) - 1
	value := old[last]
	*q = old[:last]
	return value
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
	oldWeight, reclaimWeight, topologyWeight, err := partitionCostWeights(total, len(cpus), len(sortedDemands))
	if err != nil {
		return nil, err
	}

	assignmentEdgeCount := 0
	for cpuIndex, cpu := range cpus {
		cpuNode := cpuBase + cpuIndex
		addPartitionFlowEdge(graph, source, cpuNode, 1, 0)
		for demandIndex, demand := range sortedDemands {
			if !demand.eligible.Contains(cpu) {
				continue
			}
			assignmentEdgeCount++
			if assignmentEdgeCount > partitionAssignmentEdgeBudget {
				return nil, fmt.Errorf("partition graph edge budget exceeded")
			}
			cost, costErr := partitionEdgeCost(
				cpu, cpuIndex, demandIndex, demand, dedicatedEligible, topology,
				oldWeight, reclaimWeight, topologyWeight, len(sortedDemands), total < len(cpus),
			)
			if costErr != nil {
				return nil, costErr
			}
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

	flow, err := partitionMinCostFlow(graph, source, sink, total)
	if err != nil {
		return nil, err
	}
	if flow != total {
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

func partitionCostWeights(total, cpuCount, demandCount int) (int64, int64, int64, error) {
	demandBase, err := checkedPartitionCostAdd(int64(demandCount), 1)
	if err != nil {
		return 0, 0, 0, err
	}
	maxTiePerEdge, err := checkedPartitionCostMul(int64(cpuCount), demandBase)
	if err != nil {
		return 0, 0, 0, err
	}
	maxTiePerEdge, err = checkedPartitionCostAdd(maxTiePerEdge, demandBase)
	if err != nil {
		return 0, 0, 0, err
	}
	maxTieTotal, err := checkedPartitionCostMul(int64(total), maxTiePerEdge)
	if err != nil {
		return 0, 0, 0, err
	}
	topologyWeight, err := checkedPartitionCostAdd(maxTieTotal, 1)
	if err != nil {
		return 0, 0, 0, err
	}
	maxTopologyTotal, err := checkedPartitionCostMul(int64(total), 3)
	if err != nil {
		return 0, 0, 0, err
	}
	maxTopologyTotal, err = checkedPartitionCostMul(maxTopologyTotal, topologyWeight)
	if err != nil {
		return 0, 0, 0, err
	}
	reclaimWeight, err := checkedPartitionCostAdd(maxTopologyTotal, maxTieTotal)
	if err != nil {
		return 0, 0, 0, err
	}
	reclaimWeight, err = checkedPartitionCostAdd(reclaimWeight, 1)
	if err != nil {
		return 0, 0, 0, err
	}
	maxReclaimTotal, err := checkedPartitionCostMul(int64(total), reclaimWeight)
	if err != nil {
		return 0, 0, 0, err
	}
	oldWeight, err := checkedPartitionCostAdd(maxReclaimTotal, maxTopologyTotal)
	if err != nil {
		return 0, 0, 0, err
	}
	oldWeight, err = checkedPartitionCostAdd(oldWeight, maxTieTotal)
	if err != nil {
		return 0, 0, 0, err
	}
	oldWeight, err = checkedPartitionCostAdd(oldWeight, 1)
	if err != nil {
		return 0, 0, 0, err
	}
	return oldWeight, reclaimWeight, topologyWeight, nil
}

func partitionEdgeCost(
	cpu, cpuRank, demandRank int,
	demand partitionDemand,
	dedicatedEligible machine.CPUSet,
	topology *machine.CPUTopology,
	oldWeight, reclaimWeight, topologyWeight int64,
	demandCount int,
	includeTie bool,
) (int64, error) {
	var cost int64
	var err error
	if !demand.preferred.Contains(cpu) {
		cost, err = checkedPartitionCostAdd(cost, oldWeight)
		if err != nil {
			return 0, err
		}
	}
	if demand.class == advisorBlockClassMandatoryReclaim && dedicatedEligible.Contains(cpu) {
		cost, err = checkedPartitionCostAdd(cost, reclaimWeight)
		if err != nil {
			return 0, err
		}
	}
	topologyCost, err := checkedPartitionCostMul(
		int64(partitionTopologyDistance(cpu, demand.preferred, topology)),
		topologyWeight,
	)
	if err != nil {
		return 0, err
	}
	cost, err = checkedPartitionCostAdd(cost, topologyCost)
	if err != nil {
		return 0, err
	}
	// When every eligible CPU must be assigned, the sum of CPU and demand ranks
	// is constant across feasible solutions. Omitting that constant exposes all
	// equal-cost paths to the blocking-flow phase without changing the optimum.
	if !includeTie {
		return cost, nil
	}
	demandBase, err := checkedPartitionCostAdd(int64(demandCount), 1)
	if err != nil {
		return 0, err
	}
	tieCost, err := checkedPartitionCostMul(int64(cpuRank), demandBase)
	if err != nil {
		return 0, err
	}
	demandTie, err := checkedPartitionCostAdd(int64(demandRank), 1)
	if err != nil {
		return 0, err
	}
	tieCost, err = checkedPartitionCostAdd(tieCost, demandTie)
	if err != nil {
		return 0, err
	}
	return checkedPartitionCostAdd(cost, tieCost)
}

func checkedPartitionCostAdd(left, right int64) (int64, error) {
	if (right > 0 && left > math.MaxInt64-right) || (right < 0 && left < math.MinInt64-right) {
		return 0, fmt.Errorf("partition cost overflow")
	}
	return left + right, nil
}

func checkedPartitionCostMul(left, right int64) (int64, error) {
	if left == 0 || right == 0 {
		return 0, nil
	}
	if (left == math.MinInt64 && right == -1) || (right == math.MinInt64 && left == -1) {
		return 0, fmt.Errorf("partition cost overflow")
	}
	product := left * right
	if product/right != left {
		return 0, fmt.Errorf("partition cost overflow")
	}
	return product, nil
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

func partitionMinCostFlow(graph [][]partitionFlowEdge, source, sink, wanted int) (int, error) {
	flow := 0
	potential := make([]int64, len(graph))
	operations := 0
	for flow < wanted {
		distance := make([]int64, len(graph))
		for i := range distance {
			distance[i] = math.MaxInt64
		}
		distance[source] = 0
		queue := partitionDistanceQueue{{node: source, distance: 0}}
		heap.Init(&queue)

		for queue.Len() > 0 {
			item := heap.Pop(&queue).(partitionDistanceItem)
			if item.distance != distance[item.node] {
				continue
			}
			for _, edge := range graph[item.node] {
				if err := consumePartitionFlowOperation(&operations); err != nil {
					return 0, err
				}
				if edge.cap == 0 {
					continue
				}
				reducedCost, err := partitionReducedCost(item.node, edge, potential)
				if err != nil {
					return 0, err
				}
				if reducedCost < 0 {
					return 0, fmt.Errorf("partition reduced cost became negative")
				}
				nextDistance, err := checkedPartitionCostAdd(item.distance, reducedCost)
				if err != nil {
					return 0, err
				}
				if nextDistance < distance[edge.to] {
					distance[edge.to] = nextDistance
					heap.Push(&queue, partitionDistanceItem{node: edge.to, distance: nextDistance})
				}
			}
		}
		if distance[sink] == math.MaxInt64 {
			break
		}

		for node := range potential {
			if distance[node] == math.MaxInt64 {
				continue
			}
			nextPotential, err := checkedPartitionCostAdd(potential[node], distance[node])
			if err != nil {
				return 0, err
			}
			potential[node] = nextPotential
		}

		pushed, err := partitionPushAdmissibleFlow(
			graph, source, sink, wanted-flow, potential, &operations,
		)
		if err != nil {
			return 0, err
		}
		if pushed == 0 {
			return 0, fmt.Errorf("partition shortest path produced no admissible flow")
		}
		flow += pushed
	}
	return flow, nil
}

func partitionPushAdmissibleFlow(
	graph [][]partitionFlowEdge,
	source, sink, wanted int,
	potential []int64,
	operations *int,
) (int, error) {
	total := 0
	for total < wanted {
		level := make([]int, len(graph))
		for node := range level {
			level[node] = -1
		}
		level[source] = 0
		queue := make([]int, 1, len(graph))
		queue[0] = source
		for head := 0; head < len(queue); head++ {
			node := queue[head]
			for _, edge := range graph[node] {
				if err := consumePartitionFlowOperation(operations); err != nil {
					return 0, err
				}
				if edge.cap == 0 || level[edge.to] != -1 {
					continue
				}
				reducedCost, err := partitionReducedCost(node, edge, potential)
				if err != nil {
					return 0, err
				}
				if reducedCost != 0 {
					continue
				}
				level[edge.to] = level[node] + 1
				queue = append(queue, edge.to)
			}
		}
		if level[sink] == -1 {
			break
		}

		nextEdge := make([]int, len(graph))
		for total < wanted {
			pushed, err := partitionPushAdmissiblePath(
				graph, source, sink, wanted-total, potential, level, nextEdge, operations,
			)
			if err != nil {
				return 0, err
			}
			if pushed == 0 {
				break
			}
			total += pushed
		}
	}
	return total, nil
}

func partitionPushAdmissiblePath(
	graph [][]partitionFlowEdge,
	node, sink, available int,
	potential []int64,
	level, nextEdge []int,
	operations *int,
) (int, error) {
	if node == sink {
		return available, nil
	}
	for nextEdge[node] < len(graph[node]) {
		edgeIndex := nextEdge[node]
		edge := graph[node][edgeIndex]
		if err := consumePartitionFlowOperation(operations); err != nil {
			return 0, err
		}
		if edge.cap > 0 && level[edge.to] == level[node]+1 {
			reducedCost, err := partitionReducedCost(node, edge, potential)
			if err != nil {
				return 0, err
			}
			if reducedCost == 0 {
				pathAvailable := available
				if edge.cap < pathAvailable {
					pathAvailable = edge.cap
				}
				pushed, err := partitionPushAdmissiblePath(
					graph, edge.to, sink, pathAvailable,
					potential, level, nextEdge, operations,
				)
				if err != nil {
					return 0, err
				}
				if pushed > 0 {
					reverse := edge.reverse
					graph[node][edgeIndex].cap -= pushed
					graph[edge.to][reverse].cap += pushed
					return pushed, nil
				}
			}
		}
		nextEdge[node]++
	}
	return 0, nil
}

func partitionReducedCost(from int, edge partitionFlowEdge, potential []int64) (int64, error) {
	reducedCost, err := checkedPartitionCostAdd(edge.cost, potential[from])
	if err != nil {
		return 0, err
	}
	return checkedPartitionCostSub(reducedCost, potential[edge.to])
}

func checkedPartitionCostSub(left, right int64) (int64, error) {
	if (right > 0 && left < math.MinInt64+right) || (right < 0 && left > math.MaxInt64+right) {
		return 0, fmt.Errorf("partition cost overflow")
	}
	return left - right, nil
}

func consumePartitionFlowOperation(operations *int) error {
	*operations = *operations + 1
	if *operations > partitionFlowOperationBudget {
		return fmt.Errorf("partition flow operation budget exceeded")
	}
	return nil
}
