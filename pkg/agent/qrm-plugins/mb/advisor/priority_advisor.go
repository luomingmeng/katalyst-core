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

package advisor

import (
	"context"
	"sync"

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/mb/domain"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/mb/monitor"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/mb/plan"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
)

// priorityAdvisor is able to work with resctrl groups with priority;
// one groups' priority can be the same as that of others'.
// It leverages uniqPriorityAdvisor working with groups of distinct priorities only.
// It targets the scenarios where the groups of same priority don't share ccd.
// todo: enhance to handle multiple groups of same priority sharing ccd
type priorityAdvisor struct {
	mu                  sync.RWMutex
	uniqPriorityAdvisor *uniqPriorityAdvisor
	lastSuppressedCCDs  []SuppressedCCD
}

// groupInfo stores the mapping of groups and their CCDs for each domain
type groupInfo struct { // DomainGroups maps domain ID -> combined group key -> original group key -> CCD IDs
	DomainGroups map[int]domainGroupMapping
}

// domainGroupMapping maps combined group keys to their original groups
type domainGroupMapping map[string]combinedGroupMapping

// combinedGroupMapping maps original group keys to their CCD IDs
type combinedGroupMapping map[string]ccdSet

// ccdSet represents a set of CCD IDs
type ccdSet = sets.Int

func (a *priorityAdvisor) GetPlan(ctx context.Context, domainsMon *monitor.DomainStats) (*plan.MBPlan, error) {
	domainStats, groupInfos, err := a.combinedDomainStats(domainsMon)
	if err != nil {
		return nil, err
	}
	mbPlan, err := a.uniqPriorityAdvisor.GetPlan(ctx, domainStats)
	if err != nil {
		return nil, err
	}

	a.updateSuppressedCCDs(groupInfos)

	return a.splitPlan(mbPlan, groupInfos), nil
}

func (a *priorityAdvisor) combinedDomainStats(domainsMon *monitor.DomainStats) (*monitor.DomainStats, *groupInfo, error) {
	domainStats := &monitor.DomainStats{
		Incomings:            make(map[int]monitor.DomainMonStat),
		Outgoings:            make(map[int]monitor.DomainMonStat),
		OutgoingGroupSumStat: make(map[string][]monitor.MBInfo),
	}
	groupInfos := &groupInfo{
		DomainGroups: make(map[int]domainGroupMapping),
	}
	var err error
	for id, domainMon := range domainsMon.Incomings {
		domainStats.Incomings[id], groupInfos.DomainGroups[id], err = preProcessGroupInfo(domainMon)
		if err != nil {
			return nil, nil, err
		}
	}
	for id, domainMon := range domainsMon.Outgoings {
		domainStats.Outgoings[id], _, err = preProcessGroupInfo(domainMon)
		if err != nil {
			return nil, nil, err
		}
	}
	domainStats.OutgoingGroupSumStat = preProcessGroupSumStat(domainsMon.OutgoingGroupSumStat)
	return domainStats, groupInfos, nil
}

func (a *priorityAdvisor) splitPlan(mbPlan *plan.MBPlan, groupInfos *groupInfo) *plan.MBPlan {
	for groupKey, ccdPlan := range mbPlan.MBGroups {
		if !isCombinedGroup(groupKey) {
			continue
		}
		for _, domainGroupInfos := range groupInfos.DomainGroups {
			for group, groupInfo := range domainGroupInfos[groupKey] {
				for ccd := range groupInfo {
					if _, exists := ccdPlan[ccd]; exists {
						if mbPlan.MBGroups[group] == nil {
							mbPlan.MBGroups[group] = make(plan.GroupCCDPlan)
						}
						mbPlan.MBGroups[group][ccd] = ccdPlan[ccd]
					}
				}
			}
		}
		delete(mbPlan.MBGroups, groupKey)
	}
	return mbPlan
}

func (a *priorityAdvisor) getDomainQuotaSuppression(groupInfos *groupInfo) map[int]map[string]map[int]string {
	var result map[int]map[string]map[int]string

	for domID, groupCCDs := range a.uniqPriorityAdvisor.lastQuotaSuppressedCCDs {
		for groupKey, ccdIDs := range groupCCDs {
			if isCombinedGroup(groupKey) {
				domGroupMapping, hasMapping := groupInfos.DomainGroups[domID]
				if !hasMapping {
					continue
				}
				combinedMapping, hasCombined := domGroupMapping[groupKey]
				if !hasCombined {
					continue
				}
				for realGroup, ccdSet := range combinedMapping {
					for _, ccd := range ccdIDs {
						if ccdSet.Has(ccd) {
							addQuadruplet(&result, domID, realGroup, ccd, suppressionTypeDomainStress)
						}
					}
				}
			} else {
				for _, ccd := range ccdIDs {
					addQuadruplet(&result, domID, groupKey, ccd, suppressionTypeDomainStress)
				}
			}
		}
	}

	return result
}

func addQuadruplet(receptacle *map[int]map[string]map[int]string,
	domID int, group string, ccd int, suppressionType string,
) {
	if *receptacle == nil {
		*receptacle = make(map[int]map[string]map[int]string)
	}
	result := *receptacle

	if _, ok := result[domID]; !ok {
		result[domID] = make(map[string]map[int]string)
	}

	if _, ok := result[domID][group]; !ok {
		result[domID][group] = make(map[int]string)
	}

	result[domID][group][ccd] = suppressionType
}

func (a *priorityAdvisor) updateSuppressedCCDs(groupInfos *groupInfo) {
	domGroupCCDTypes := a.getDomainQuotaSuppression(groupInfos)
	capHint := a.getLastSuppressedCCDCap()
	suppressed := buildSuppressedCCDs(domGroupCCDTypes, nil, capHint)
	a.setLastSuppressedCCDs(suppressed)
}

func (a *priorityAdvisor) getLastSuppressedCCDCap() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return cap(a.lastSuppressedCCDs)
}

func (a *priorityAdvisor) setLastSuppressedCCDs(v []SuppressedCCD) {
	a.mu.Lock()
	a.lastSuppressedCCDs = v
	a.mu.Unlock()
}

func buildSuppressedCCDs(domGroupCCDTypes map[int]map[string]map[int]string, extra []SuppressedCCD, bufCap int) []SuppressedCCD {
	result := make([]SuppressedCCD, 0, bufCap)
	for domID, groupCCDTypes := range domGroupCCDTypes {
		for group, ccdTypes := range groupCCDTypes {
			for ccdID, suppressionType := range ccdTypes {
				result = append(result, SuppressedCCD{
					DomID:           domID,
					Group:           group,
					CCDID:           ccdID,
					SuppressionType: suppressionType,
				})
			}
		}
	}
	result = append(result, extra...)
	return result
}

func (a *priorityAdvisor) GetSuppressedCCDs() []SuppressedCCD {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.lastSuppressedCCDs
}

func New(emitter metrics.MetricEmitter, domains domain.Domains, ccdMinMB, ccdMaxMB int, defaultDomainCapacity int,
	capPercent int, XDomGroups []string, groupNeverThrottles []string,
	groupCapacity map[string]int,
) Advisor {
	return &priorityAdvisor{
		uniqPriorityAdvisor: newUniqPriorityAdvisor(emitter,
			domains, ccdMinMB, ccdMaxMB, defaultDomainCapacity, capPercent,
			XDomGroups, groupNeverThrottles,
			groupCapacity,
		),
	}
}
