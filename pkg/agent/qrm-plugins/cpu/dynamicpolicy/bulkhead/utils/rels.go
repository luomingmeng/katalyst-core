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

package utils

import (
	"fmt"
	"sort"
	"strings"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpusetmaterializer"
	bulkheadconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/qrm/bulkhead"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	cgcommon "github.com/kubewharf/katalyst-core/pkg/util/cgroup/common"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type ContainerRelPathResolveStage string

const (
	ContainerRelPathResolveStageContainerID ContainerRelPathResolveStage = "container_id"
	ContainerRelPathResolveStageCgroupPath  ContainerRelPathResolveStage = "cgroup_path"
)

type ContainerRelPathResolveError struct {
	Stage ContainerRelPathResolveStage
	Err   error
}

func (e *ContainerRelPathResolveError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("resolve container rel path stage=%s: %v", e.Stage, e.Err)
}

func (e *ContainerRelPathResolveError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func ResolveContainerRelPath(metaServer *metaserver.MetaServer, podUID, containerName string) (string, error) {
	if metaServer == nil {
		return "", fmt.Errorf("nil metaServer")
	}
	containerID, err := metaServer.GetContainerID(podUID, containerName)
	if err != nil {
		return "", &ContainerRelPathResolveError{Stage: ContainerRelPathResolveStageContainerID, Err: err}
	}
	rel, err := cgcommon.GetContainerRelativeCgroupPath(podUID, containerID)
	if err != nil {
		return "", &ContainerRelPathResolveError{Stage: ContainerRelPathResolveStageCgroupPath, Err: err}
	}
	return strings.Trim(rel, "/"), nil
}

type ControlledRelTarget struct {
	Rel    string
	Target machine.CPUSet
}

func BuildControlledRelInventory(
	cfg bulkheadconfig.BulkheadConfiguration,
	target cpusetmaterializer.Target,
	systemServiceEnabled bool,
	siblings []string,
	containerRels map[string]machine.CPUSet,
) []ControlledRelTarget {
	targetByRel := map[string]machine.CPUSet{}
	add := func(rel string, cpus machine.CPUSet) {
		rel = strings.Trim(rel, "/")
		if rel == "" {
			return
		}
		targetByRel[rel] = cpus.Clone()
	}

	nonReclaim := target.NonReclaimCPUSet()
	reclaim := target.ReclaimCPUSet()
	add(cfg.BulkheadPrimaryRelPath, nonReclaim)
	if systemServiceEnabled {
		add(cfg.BulkheadSystemRelPath, reclaim)
	}
	for reclaimIdx, reclaimRel := range cfg.BulkheadReclaimRelPaths {
		add(reclaimRel, reclaim)
		for numaID, cpus := range target.ReclaimCPUSetByNUMA() {
			add(cfg.ReclaimPerNUMA(reclaimIdx, numaID), cpus)
		}
	}
	for _, rel := range siblings {
		add(rel, reclaim)
	}
	for rel, cpus := range containerRels {
		add(rel, cpus)
	}

	rels := make([]string, 0, len(targetByRel))
	for rel := range targetByRel {
		rels = append(rels, rel)
	}
	sort.Strings(rels)
	out := make([]ControlledRelTarget, 0, len(rels))
	for _, rel := range rels {
		out = append(out, ControlledRelTarget{Rel: rel, Target: targetByRel[rel]})
	}
	return out
}

func CollectActiveRels(
	cfg bulkheadconfig.BulkheadConfiguration,
	view *CPUSetPartitionView,
	metaServer *metaserver.MetaServer,
	reclaimSiblings []string,
	relExists RelExistsFunc,
) map[string]struct{} {
	out := map[string]struct{}{}
	out[""] = struct{}{}

	addIfExists := func(rel string) {
		rel = strings.Trim(rel, "/")
		if rel == "" {
			return
		}
		if relExists != nil {
			if err := relExists(rel); err != nil {
				general.InfofV(5, "bulkhead: active rel path does not exist, skipping, rel=%q err=%v", rel, err)
				return
			}
		}
		out[rel] = struct{}{}
	}

	addIfExists(cfg.BulkheadPrimaryRelPath)
	for _, rel := range cfg.BulkheadReclaimRelPaths {
		addIfExists(rel)
	}
	for _, rel := range cfg.BulkheadPartitionRelPaths {
		addIfExists(rel)
	}
	for _, rel := range reclaimSiblings {
		addIfExists(rel)
	}

	if view != nil {
		for reclaimIdx := range cfg.BulkheadReclaimRelPaths {
			for numaID := range view.ReclaimEffectivePerNUMA {
				addIfExists(cfg.ReclaimPerNUMA(reclaimIdx, numaID))
			}
		}
	}

	if view != nil && metaServer != nil {
		for podUID, containers := range view.ContainerCPUSetByPod {
			for containerName := range containers {
				rel, err := ResolveContainerRelPath(metaServer, podUID, containerName)
				if err != nil {
					general.InfofV(5, "bulkhead: CollectActiveRels resolve container rel failed, pod=%q container=%q err=%v",
						podUID, containerName, err)
					continue
				}
				addIfExists(rel)
			}
		}
	}
	return out
}
