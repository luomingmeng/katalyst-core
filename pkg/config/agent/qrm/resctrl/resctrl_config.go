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

import "k8s.io/apimachinery/pkg/util/sets"

type ResctrlConfig struct {
	// EnableResctrlHint is the flag that enable/disable resctrl option related pod admission.
	EnableResctrlHint bool
	// EnableResctrlGroupLifecycleManagement is deprecated and kept only for CLI compatibility.
	// Resctrl CLOS lifecycle is controlled by dynamic RDTConfig.DisableRDT.
	EnableResctrlGroupLifecycleManagement bool

	// CPUSetPoolToSharedSubgroup specifies, if present, the subgroup id for shared-core QoS pod
	// based on its cpu set pool annotation.
	CPUSetPoolToSharedSubgroup map[string]int
	DefaultSharedSubgroup      int
	EnabledQoS                 []string

	// MonGroupEnabledClosIDs is about mon_group layout hint policy.
	MonGroupEnabledClosIDs []string
	// MonGroupMaxCountRatio is the ratio of mon_groups max count in info/L3_MON/num_rmids.
	MonGroupMaxCountRatio float64

	// DefaultClosIDs is the list of resctrl CLOS directories that should exist by default.
	DefaultClosIDs []string
	// SkipCleanupClosIDs is a list of resctrl closID directories to skip cleaning.
	SkipCleanupClosIDs sets.String
	// OwnershipCheckpointPath persists the exact CLOS IDs created by Katalyst.
	// Empty disables persistence and is intended only for tests.
	OwnershipCheckpointPath string
}

func NewResctrlConfig() *ResctrlConfig {
	return &ResctrlConfig{
		CPUSetPoolToSharedSubgroup: make(map[string]int),
		DefaultSharedSubgroup:      -1,
		SkipCleanupClosIDs:         sets.NewString(),
	}
}
