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

package qrm

import (
	"k8s.io/apimachinery/pkg/util/sets"
	cliflag "k8s.io/component-base/cli/flag"

	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	qrmconfigresctrl "github.com/kubewharf/katalyst-core/pkg/config/agent/qrm/resctrl"
)

type ResctrlOptions struct {
	EnableResctrlHint                     bool
	EnableResctrlGroupLifecycleManagement bool
	CPUSetPoolToSharedSubgroup            map[string]int
	DefaultSharedSubgroup                 int
	EnabledQoS                            []string
	MonGroupEnabledClosIDs                []string
	MonGroupMaxCountRatio                 float64
	DefaultClosIDs                        []string
	SkipCleanupClosIDs                    []string
}

func NewResctrlOptions() *ResctrlOptions {
	return &ResctrlOptions{
		CPUSetPoolToSharedSubgroup: make(map[string]int),
		DefaultSharedSubgroup:      -1,
		EnabledQoS:                 []string{apiconsts.PodAnnotationQoSLevelSharedCores},
		MonGroupEnabledClosIDs:     []string{},
		SkipCleanupClosIDs:         []string{},
	}
}

func (o *ResctrlOptions) AddFlags(fss *cliflag.NamedFlagSets) {
	fs := fss.FlagSet("resctrl")

	fs.BoolVar(&o.EnableResctrlHint, "pod-admit-resctrl-layout-hint",
		o.EnableResctrlHint, "if set true, we will enable resctrl hint on pod admission")
	fs.BoolVar(&o.EnableResctrlGroupLifecycleManagement, "enable-resctrl-group-lifecycle-management",
		o.EnableResctrlGroupLifecycleManagement, "deprecated no-op kept for compatibility; resctrl CLOS lifecycle is controlled by dynamic RDTConfig.DisableRDT")
	fs.StringToIntVar(&o.CPUSetPoolToSharedSubgroup, "resctrl-cpuset-pool-to-shared-subgroup",
		o.CPUSetPoolToSharedSubgroup, "customize share-xx subgroup if present")
	fs.IntVar(&o.DefaultSharedSubgroup, "resctrl-default-shared-subgroup",
		o.DefaultSharedSubgroup, "default subgroup for shared qos")
	fs.StringSliceVar(&o.EnabledQoS, "resctrl-enabled-qos",
		o.EnabledQoS, "enabled qos levels to create resctrl closID")
	fs.StringSliceVar(&o.MonGroupEnabledClosIDs, "resctrl-mon-groups-enabled-closids",
		o.MonGroupEnabledClosIDs, "enabled-closid mon-groups")
	fs.Float64Var(&o.MonGroupMaxCountRatio, "resctrl-mon-groups-max-count-ratio",
		o.MonGroupMaxCountRatio, "ratio of mon_groups max count")
	fs.StringSliceVar(&o.DefaultClosIDs, "resctrl-default-closids",
		o.DefaultClosIDs, "a list of resctrl closID directories to create by default")
	fs.StringSliceVar(&o.SkipCleanupClosIDs, "resctrl-skip-cleanup-closids",
		o.SkipCleanupClosIDs, "a list of resctrl closID directories to skip cleaning")
}

func (o *ResctrlOptions) ApplyTo(conf *qrmconfigresctrl.ResctrlConfig) error {
	conf.EnableResctrlHint = o.EnableResctrlHint
	conf.EnableResctrlGroupLifecycleManagement = o.EnableResctrlGroupLifecycleManagement
	conf.CPUSetPoolToSharedSubgroup = o.CPUSetPoolToSharedSubgroup
	conf.DefaultSharedSubgroup = o.DefaultSharedSubgroup
	conf.EnabledQoS = o.EnabledQoS
	conf.MonGroupEnabledClosIDs = o.MonGroupEnabledClosIDs
	conf.MonGroupMaxCountRatio = o.MonGroupMaxCountRatio
	conf.DefaultClosIDs = o.DefaultClosIDs
	conf.SkipCleanupClosIDs = sets.NewString(o.SkipCleanupClosIDs...)
	return nil
}
