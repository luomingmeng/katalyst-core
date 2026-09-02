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
	"sort"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/model"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	utilmetric "github.com/kubewharf/katalyst-core/pkg/util/metric"
)

type labeledPool struct {
	identity model.CPUSetPoolIdentity
	label    string
	cpus     machine.CPUSet
}

type labelAssignment struct {
	pools             []labeledPool
	conflictCPUByKind map[model.CPUSetPoolKind]machine.CPUSet
}

type pendingUIDLabel struct {
	identity   model.CPUSetPoolIdentity
	candidates []string
	index      int
}

func assignPoolLabels(
	byIdentity map[model.CPUSetPoolIdentity]machine.CPUSet,
) labelAssignment {
	result := labelAssignment{
		conflictCPUByKind: map[model.CPUSetPoolKind]machine.CPUSet{
			model.CPUSetPoolKindShare:     machine.NewCPUSet(),
			model.CPUSetPoolKindDedicated: machine.NewCPUSet(),
			model.CPUSetPoolKindIsolation: machine.NewCPUSet(),
		},
	}
	identities := sortedAppliedPoolIdentities(byIdentity)
	labels := make(map[model.CPUSetPoolIdentity]string, len(identities))
	fixedByLabel := make(map[string][]model.CPUSetPoolIdentity)
	reservedLabels := make(map[string]struct{})

	for _, identity := range identities {
		label, fixed := formattedFixedLabel(identity)
		if !fixed {
			continue
		}
		if label == "" {
			addConflictCPU(&result, identity, byIdentity[identity])
			continue
		}
		fixedByLabel[label] = append(fixedByLabel[label], identity)
		reservedLabels[label] = struct{}{}
	}

	for label, fixed := range fixedByLabel {
		reclaimIndex := -1
		for i, identity := range fixed {
			if identity.Kind == model.CPUSetPoolKindReclaim {
				reclaimIndex = i
				break
			}
		}
		if reclaimIndex >= 0 {
			labels[fixed[reclaimIndex]] = label
			for i, identity := range fixed {
				if i != reclaimIndex {
					addConflictCPU(&result, identity, byIdentity[identity])
				}
			}
			continue
		}
		if len(fixed) == 1 {
			labels[fixed[0]] = label
			continue
		}
		for _, identity := range fixed {
			addConflictCPU(&result, identity, byIdentity[identity])
		}
	}

	pending := make([]pendingUIDLabel, 0, len(identities))
	for _, identity := range identities {
		prefix := uidLabelPrefix(identity.Kind)
		if prefix == "" {
			continue
		}
		if identity.PodUID == "" {
			addConflictCPU(&result, identity, byIdentity[identity])
			continue
		}
		pending = append(pending, pendingUIDLabel{
			identity:   identity,
			candidates: uidLabelCandidates(prefix, identity.PodUID),
		})
	}

	assignedDynamic := make(map[string]struct{})
	for len(pending) > 0 {
		byCandidate := make(map[string][]int, len(pending))
		for i := range pending {
			candidate := pending[i].candidates[pending[i].index]
			byCandidate[candidate] = append(byCandidate[candidate], i)
		}

		nextPending := make([]pendingUIDLabel, 0, len(pending))
		for _, candidate := range sortedStringsFromMap(byCandidate) {
			indexes := byCandidate[candidate]
			groupStart := len(nextPending)
			_, fixedCollision := reservedLabels[candidate]
			_, assignedCollision := assignedDynamic[candidate]
			if len(indexes) == 1 && !fixedCollision && !assignedCollision {
				item := pending[indexes[0]]
				labels[item.identity] = candidate
				assignedDynamic[candidate] = struct{}{}
				continue
			}

			advanced := false
			for _, pendingIndex := range indexes {
				item := pending[pendingIndex]
				if item.index+1 < len(item.candidates) {
					item.index++
					nextPending = append(nextPending, item)
					advanced = true
				} else if !fixedCollision && !assignedCollision {
					nextPending = append(nextPending, item)
				} else {
					addConflictCPU(&result, item.identity, byIdentity[item.identity])
				}
			}
			if advanced {
				continue
			}

			if fixedCollision {
				for _, identity := range fixedByLabel[candidate] {
					delete(labels, identity)
					addConflictCPU(&result, identity, byIdentity[identity])
				}
			}
			for _, pendingIndex := range indexes {
				item := pending[pendingIndex]
				addConflictCPU(&result, item.identity, byIdentity[item.identity])
			}
			nextPending = nextPending[:groupStart]
		}
		pending = nextPending
	}

	for _, identity := range identities {
		label, ok := labels[identity]
		if !ok {
			continue
		}
		result.pools = append(result.pools, labeledPool{
			identity: identity,
			label:    label,
			cpus:     byIdentity[identity].Clone(),
		})
	}
	return result
}

func formattedFixedLabel(identity model.CPUSetPoolIdentity) (string, bool) {
	switch identity.Kind {
	case model.CPUSetPoolKindReclaim:
		return utilmetric.MetricTagValueFormat(model.CPUSetPoolKindReclaim), true
	case model.CPUSetPoolKindShare:
		return utilmetric.MetricTagValueFormat(identity.Name), true
	default:
		return "", false
	}
}

func uidLabelPrefix(kind model.CPUSetPoolKind) string {
	switch kind {
	case model.CPUSetPoolKindDedicated, model.CPUSetPoolKindIsolation:
		return string(kind) + "-"
	default:
		return ""
	}
}

func sortedAppliedPoolIdentities(
	byIdentity map[model.CPUSetPoolIdentity]machine.CPUSet,
) []model.CPUSetPoolIdentity {
	identities := make([]model.CPUSetPoolIdentity, 0, len(byIdentity))
	for identity := range byIdentity {
		identities = append(identities, identity)
	}
	sort.Slice(identities, func(i, j int) bool {
		if identities[i].Kind != identities[j].Kind {
			return identities[i].Kind < identities[j].Kind
		}
		if identities[i].Name != identities[j].Name {
			return identities[i].Name < identities[j].Name
		}
		return identities[i].PodUID < identities[j].PodUID
	})
	return identities
}

func uidLabelCandidates(prefix, uid string) []string {
	uidRunes := []rune(uid)
	firstLength := 2
	if len(uidRunes) < firstLength {
		firstLength = len(uidRunes)
	}
	lastPrefixLength := 8
	if len(uidRunes) < lastPrefixLength {
		lastPrefixLength = len(uidRunes)
	}

	candidates := make([]string, 0, lastPrefixLength-firstLength+2)
	for length := firstLength; length <= lastPrefixLength; length++ {
		candidates = appendDistinct(candidates,
			utilmetric.MetricTagValueFormat(prefix+string(uidRunes[:length])))
	}
	candidates = appendDistinct(candidates, utilmetric.MetricTagValueFormat(prefix+uid))
	return candidates
}

func appendDistinct(values []string, value string) []string {
	if len(values) == 0 || values[len(values)-1] != value {
		return append(values, value)
	}
	return values
}

func addConflictCPU(
	result *labelAssignment,
	identity model.CPUSetPoolIdentity,
	cpus machine.CPUSet,
) {
	current, ok := result.conflictCPUByKind[identity.Kind]
	if !ok {
		return
	}
	result.conflictCPUByKind[identity.Kind] = current.Union(cpus)
}

func sortedStringsFromMap[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
