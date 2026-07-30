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

package resctrl

import (
	"fmt"
	"sort"
	"strings"

	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	qrmresctrl "github.com/kubewharf/katalyst-core/pkg/config/agent/qrm/resctrl"
	"github.com/kubewharf/katalyst-core/pkg/consts"
)

type ClosAssignmentMeta struct {
	QoSLevel  string
	OwnerPool string
}

// SharedSubgroupClosID returns the CLOS ID for a shared subgroup.
func SharedSubgroupClosID(subgroup int) string {
	if subgroup < 0 {
		return consts.ResctrlGroupShare
	}
	return fmt.Sprintf("%s%02d", consts.ResctrlShareSubgroupPrefix, subgroup)
}

func NormalizeClosID(closID string) string {
	if strings.HasPrefix(closID, consts.ResctrlObsoleteSharedSubgroupPrefix) {
		return consts.ResctrlShareSubgroupPrefix + strings.TrimPrefix(closID, consts.ResctrlObsoleteSharedSubgroupPrefix)
	}
	return closID
}

func ResolveSharedPoolClosID(poolName string, config *qrmresctrl.ResctrlConfig) string {
	if config != nil {
		if subgroup, ok := config.CPUSetPoolToSharedSubgroup[poolName]; ok {
			return SharedSubgroupClosID(subgroup)
		}
		return SharedSubgroupClosID(config.DefaultSharedSubgroup)
	}
	return SharedSubgroupClosID(-1)
}

func ResolvePoolClosID(meta ClosAssignmentMeta, config *qrmresctrl.ResctrlConfig) (string, error) {
	switch meta.QoSLevel {
	case apiconsts.PodAnnotationQoSLevelSharedCores:
		return ResolveSharedPoolClosID(meta.OwnerPool, config), nil
	case apiconsts.PodAnnotationQoSLevelDedicatedCores:
		return consts.ResctrlGroupDedicated, nil
	case apiconsts.PodAnnotationQoSLevelReclaimedCores:
		return consts.ResctrlGroupReclaim, nil
	case apiconsts.PodAnnotationQoSLevelSystemCores:
		return consts.ResctrlGroupSystem, nil
	default:
		if meta.OwnerPool == "" {
			return "", fmt.Errorf("empty owner pool for qos %q", meta.QoSLevel)
		}
		return NormalizeClosID(meta.OwnerPool), nil
	}
}

func BuildExpectedClosPools(metas []ClosAssignmentMeta, config *qrmresctrl.ResctrlConfig) (map[string][]string, error) {
	poolsByClos := make(map[string][]string)
	seen := make(map[string]map[string]struct{})
	for _, meta := range metas {
		closID, err := ResolvePoolClosID(meta, config)
		if err != nil {
			return nil, err
		}
		source := meta.OwnerPool
		if source == "" {
			source = meta.QoSLevel
		}
		if seen[closID] == nil {
			seen[closID] = make(map[string]struct{})
		}
		if _, ok := seen[closID][source]; ok {
			continue
		}
		seen[closID][source] = struct{}{}
		poolsByClos[closID] = append(poolsByClos[closID], source)
	}
	for closID := range poolsByClos {
		sort.Strings(poolsByClos[closID])
	}
	return poolsByClos, nil
}

func ResolveCATWayKey(key string, config *qrmresctrl.ResctrlConfig) string {
	if config != nil {
		if subgroup, ok := config.CPUSetPoolToSharedSubgroup[key]; ok {
			return SharedSubgroupClosID(subgroup)
		}
	}
	return NormalizeClosID(key)
}

func IsManagedClosID(closID string, config *qrmresctrl.ResctrlConfig) bool {
	closID = NormalizeClosID(closID)
	managed := map[string]struct{}{
		consts.ResctrlGroupDedicated: {},
		consts.ResctrlGroupReclaim:   {},
		consts.ResctrlGroupSystem:    {},
		consts.ResctrlGroupShare:     {},
	}
	if config != nil {
		for _, defaultClosID := range config.DefaultClosIDs {
			managed[NormalizeClosID(defaultClosID)] = struct{}{}
		}
		for _, subgroup := range config.CPUSetPoolToSharedSubgroup {
			managed[SharedSubgroupClosID(subgroup)] = struct{}{}
		}
		if config.DefaultSharedSubgroup >= 0 {
			managed[SharedSubgroupClosID(config.DefaultSharedSubgroup)] = struct{}{}
		}
	}
	_, ok := managed[closID]
	return ok
}

func IsCPUListManagedClosID(closID string, config *qrmresctrl.ResctrlConfig) bool {
	closID = NormalizeClosID(closID)
	if closID == consts.ResctrlGroupDedicated || closID == consts.ResctrlGroupShare {
		return true
	}
	if config == nil {
		return false
	}
	for _, subgroup := range config.CPUSetPoolToSharedSubgroup {
		if closID == SharedSubgroupClosID(subgroup) {
			return true
		}
	}
	if config.DefaultSharedSubgroup >= 0 && closID == SharedSubgroupClosID(config.DefaultSharedSubgroup) {
		return true
	}
	return false
}
