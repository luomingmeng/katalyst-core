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

package client

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"

	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/provisioner/malachite/types"
	metrictypes "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/types"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	cgroupcm "github.com/kubewharf/katalyst-core/pkg/util/cgroup/common"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/native"
)

// PodContainersStats keeps source membership separate from successful samples.
// A nil PodUIDSet means inventory failed and must not authorize metric GC.
type PodContainersStats struct {
	PodUIDSet map[string]bool
	Stats     map[string]map[string]*types.MalachiteCgroupInfo
}

func (c *MalachiteClient) GetAllPodContainersStats(ctx context.Context) (PodContainersStats, error) {
	pods, err := c.fetcher.GetPodList(ctx, func(_ *v1.Pod) bool { return true })
	if err != nil {
		return PodContainersStats{}, fmt.Errorf("GetAllPodContainersStats fetch-pods: %w", err)
	}
	result := PodContainersStats{
		PodUIDSet: make(map[string]bool, len(pods)),
		Stats:     make(map[string]map[string]*types.MalachiteCgroupInfo),
	}
	var errs []error
	for _, p := range pods {
		if p == nil || p.UID == "" {
			return PodContainersStats{}, fmt.Errorf("container metric inventory has no Pod UID")
		}
		if provider, ok := c.fetcher.(metrictypes.PodMetricTargetProvider); ok {
			_, handled, resolveErr := provider.ResolvePodMetricTarget(ctx, p)
			if resolveErr != nil {
				general.Errorf("skip container metric inventory Pod %s: resolve metric target failed: %v",
					p.UID, resolveErr)
				continue
			}
			if handled {
				continue
			}
		}
		uid := string(p.UID)
		if result.PodUIDSet[uid] {
			return PodContainersStats{}, fmt.Errorf("container metric inventory has duplicate Pod UID %s", uid)
		}
		result.PodUIDSet[uid] = true
	}
	for uid := range result.PodUIDSet {
		stats, err := c.GetPodStats(ctx, uid)
		if err != nil {
			errs = append(errs, err)
		}
		if len(stats) > 0 {
			result.Stats[uid] = stats
		}
	}
	return result, utilerrors.NewAggregate(errs)
}

func (c *MalachiteClient) GetPodStats(ctx context.Context, podUID string) (map[string]*types.MalachiteCgroupInfo, error) {
	pod, err := c.fetcher.GetPod(ctx, podUID)
	if err != nil {
		return nil, fmt.Errorf("GetPodStats %s fetch-pod err %v", podUID, err)
	}

	containersStats := make(map[string]*types.MalachiteCgroupInfo)
	var errs []error
	for _, containerStatus := range pod.Status.ContainerStatuses {
		containerID := native.TrimContainerIDPrefix(containerStatus.ContainerID)
		stats, err := c.GetPodContainerStats(podUID, containerID)
		if err != nil {
			errs = append(errs, fmt.Errorf("Pod %s container %s stats: %w", podUID, containerStatus.Name, err))
			general.Errorf("failed to get pod %s container %s stats, err %v", podUID, containerID, err)
			_ = c.emitter.StoreInt64(metricMalachiteContainerStatsMissing, 1, metrics.MetricTypeNameCount,
				metrics.MetricTag{Key: "podUID", Val: podUID},
				metrics.MetricTag{Key: "containerID", Val: containerID},
			)
			continue
		}
		containersStats[containerStatus.Name] = stats
	}
	return containersStats, utilerrors.NewAggregate(errs)
}

func (c *MalachiteClient) GetPodContainerStats(podUID, containerID string) (*types.MalachiteCgroupInfo, error) {
	var cgroupPath string
	var err error

	// if relativePathFunc has been set, we should use it
	if c.relativePathFunc != nil {
		cgroupPath, err = (*c.relativePathFunc)(podUID, containerID)
	} else {
		cgroupPath, err = cgroupcm.GetContainerRelativeCgroupPath(podUID, containerID)
	}
	if err != nil {
		return nil, fmt.Errorf("GetPodContainerStats %s/%v get-relative-path err %v", podUID, containerID, err)
	}

	containersStats, err := c.GetCgroupStats(cgroupPath)
	if err != nil {
		return nil, fmt.Errorf("GetPodContainerStats %s/%v get-status %v err %v", podUID, containerID, cgroupPath, err)
	}

	return containersStats, nil
}
