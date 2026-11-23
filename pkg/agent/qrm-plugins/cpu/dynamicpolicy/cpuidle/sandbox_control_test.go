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

package cpuidle

import (
	"context"
	"errors"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"

	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/config"
	coreconsts "github.com/kubewharf/katalyst-core/pkg/consts"
	metrictypes "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/types"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
	"github.com/kubewharf/katalyst-core/pkg/util/cgroup/common"
	cgroupmgr "github.com/kubewharf/katalyst-core/pkg/util/cgroup/manager"
)

type idleTargetFetcher struct {
	pod.PodFetcherStub
	target  metrictypes.PodMetricTarget
	handled bool
	err     error
}

func (f *idleTargetFetcher) GetPodMetricTargets(context.Context) ([]metrictypes.PodMetricTarget, error) {
	return []metrictypes.PodMetricTarget{f.target}, f.err
}

func (f *idleTargetFetcher) ResolvePodMetricTarget(_ context.Context, p *v1.Pod) (metrictypes.PodMetricTarget, bool, error) {
	if p.UID == f.target.Pod.UID {
		return f.target, f.handled, f.err
	}
	return metrictypes.PodMetricTarget{}, false, nil
}

func idleControlPod(uid string) *v1.Pod {
	runtimeClass := "kata"
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{UID: k8stypes.UID(uid), Name: uid, Annotations: map[string]string{
			apiconsts.PodAnnotationContainerCPUIdleRateKey: `{"sidecar":50}`,
			coreconsts.MainContainerNameAnnotationKey:      "main",
		}},
		Spec: v1.PodSpec{RuntimeClassName: &runtimeClass, Containers: []v1.Container{
			{Name: "main", Resources: v1.ResourceRequirements{Limits: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")}}},
			{Name: "sidecar", Resources: v1.ResourceRequirements{Limits: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")}}},
		}},
		Status: v1.PodStatus{Phase: v1.PodRunning, ContainerStatuses: []v1.ContainerStatus{
			{Name: "main", Ready: true, ContainerID: "containerd://main"},
			{Name: "sidecar", Ready: true, ContainerID: "containerd://sidecar"},
		}},
	}
}

func TestContainerCPUIdleExcludesWholePodTargets(t *testing.T) {
	managerTestMutex.Lock()
	defer managerTestMutex.Unlock()
	for _, tc := range []struct {
		name    string
		path    string
		handled bool
		err     error
	}{
		{name: "separate-root", path: "/sandboxes/instance", handled: true},
		{name: "kubernetes-root", path: "/kubepods/burstable/instance", handled: true},
		{name: "mapping-unavailable", path: "/kubepods/burstable/instance", handled: true, err: errors.New("mapping unavailable")},
		{name: "classification-failed", path: "/kubepods/burstable/instance", err: errors.New("source unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockey.PatchConvey(tc.name, t, func() {
				ordinary, whole := idleControlPod("ordinary"), idleControlPod("whole")
				f := &idleTargetFetcher{PodFetcherStub: pod.PodFetcherStub{PodList: []*v1.Pod{whole, ordinary}},
					target: metrictypes.PodMetricTarget{Pod: whole, CgroupPath: tc.path}, handled: tc.handled, err: tc.err}
				meta := generateTestMetaServer(nil)
				meta.PodFetcher = f
				conf := config.NewConfiguration()
				conf.MainContainerAnnotationKey = coreconsts.MainContainerNameAnnotationKey
				initial := common.CPUData{CpuQuota: 100000, CpuPeriod: 100000}
				state := map[string]common.CPUData{tc.path: initial}
				// Replace only kernel support and cgroup I/O. The annotation parser,
				// readiness, limits, quota calculation and public manager remain real.
				mockey.Mock(common.CheckCgroup2UnifiedMode).Return(true).Build()
				mockey.Mock(common.IsCPUIdleSupported).Return(true).Build()
				mockey.Mock(common.GetContainerRelativeCgroupPath).To(func(uid, id string) (string, error) {
					if uid == "whole" {
						return tc.path, nil
					}
					return uid + "/" + id, nil
				}).Build()
				mockey.Mock(cgroupmgr.GetCPUWithRelativePath).Return(&common.CPUStats{CpuQuota: 100000, CpuPeriod: 100000}, nil).Build()
				mockey.Mock(cgroupmgr.ApplyCPUWithRelativePath).To(func(path string, data *common.CPUData) error {
					state[path] = *data
					return nil
				}).Build()
				err := newManager(meta).UpdateContainerCPUIdle(conf)
				if !tc.handled && tc.err != nil {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, initial, state[tc.path], "whole-workload quota and idle must remain untouched")
				require.Len(t, state, 2)
				applied := state["ordinary/sidecar"]
				require.Equal(t, int64(150000), applied.CpuQuota)
				require.Equal(t, uint64(100000), applied.CpuPeriod)
				require.NotNil(t, applied.CpuIdlePtr)
				require.True(t, *applied.CpuIdlePtr)
			})
		})
	}
}
