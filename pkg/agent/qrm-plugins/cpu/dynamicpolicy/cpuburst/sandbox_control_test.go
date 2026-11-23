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

package cpuburst

import (
	"context"
	"errors"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"

	"github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/config"
	metrictypes "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/types"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
	"github.com/kubewharf/katalyst-core/pkg/util/cgroup/common"
	"github.com/kubewharf/katalyst-core/pkg/util/cgroup/manager"
)

type burstTargetFetcher struct {
	pod.PodFetcherStub
	target  metrictypes.PodMetricTarget
	handled bool
	err     error
}

func (f *burstTargetFetcher) GetPodMetricTargets(context.Context) ([]metrictypes.PodMetricTarget, error) {
	return []metrictypes.PodMetricTarget{f.target}, f.err
}

func (f *burstTargetFetcher) ResolvePodMetricTarget(_ context.Context, p *v1.Pod) (metrictypes.PodMetricTarget, bool, error) {
	if p.UID == f.target.Pod.UID {
		return f.target, f.handled, f.err
	}
	return metrictypes.PodMetricTarget{}, false, nil
}

func burstControlPod(uid string) *v1.Pod {
	runtimeClass := "kata"
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{UID: k8stypes.UID(uid), Name: uid, Annotations: map[string]string{
			consts.PodAnnotationQoSLevelKey: consts.PodAnnotationQoSLevelSharedCores,
		}},
		Spec: v1.PodSpec{RuntimeClassName: &runtimeClass, Containers: []v1.Container{{Name: "main"}, {Name: "sidecar"}}},
		Status: v1.PodStatus{Phase: v1.PodRunning, ContainerStatuses: []v1.ContainerStatus{
			{Name: "main", ContainerID: "containerd://main"}, {Name: "sidecar", ContainerID: "containerd://sidecar"},
		}},
	}
}

func TestCPUBurstExcludesWholePodTargets(t *testing.T) {
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
				ordinary, whole := burstControlPod("ordinary"), burstControlPod("whole")
				// An explicit policy forces the old implementation down a write path;
				// merely testing default policy could pass without object isolation.
				whole.Annotations[consts.PodAnnotationCPUEnhancementKey] = `{"cpu_burst_policy":"static","cpu_burst_percent":"50"}`
				f := &burstTargetFetcher{target: metrictypes.PodMetricTarget{Pod: whole, CgroupPath: tc.path}, handled: tc.handled, err: tc.err}
				meta := generateTestMetaServer(nil)
				meta.PodFetcher = f
				m := newManager(meta)
				conf := config.NewConfiguration()
				conf.EnableDefaultSharedCoresCPUBurst = true
				conf.EnableCPUBurstForMainContainerOnly = false

				// Only the OS I/O boundary is replaced; list, classification, policy,
				// sole-shared calculation, quota conversion and updates remain real.
				state := map[string]uint64{tc.path: 77}
				mockey.Mock(common.IsContainerCgroupExist).Return(true, nil).Build()
				mockey.Mock(common.GetContainerAbsCgroupPath).To(func(_, uid, id string) (string, error) {
					if uid == "whole" {
						return tc.path, nil
					}
					return uid + "/" + id, nil
				}).Build()
				mockey.Mock(manager.GetCPUWithAbsolutePath).Return(&common.CPUStats{CpuQuota: 200000}, nil).Build()
				mockey.Mock(manager.ApplyCPUWithAbsolutePath).To(func(path string, data *common.CPUData) error {
					state[path] = *data.CpuBurst
					return nil
				}).Build()

				// Ordinary-only behavior must match the mixed-list result exactly.
				f.PodList = []*v1.Pod{ordinary}
				require.NoError(t, m.UpdateCPUBurst(conf, nil))
				want := map[string]uint64{"ordinary/main": state["ordinary/main"], "ordinary/sidecar": state["ordinary/sidecar"], tc.path: 77}
				require.NotZero(t, want["ordinary/main"])
				state["ordinary/main"], state["ordinary/sidecar"] = 1, 1
				f.PodList = []*v1.Pod{whole, ordinary}
				err := m.UpdateCPUBurst(conf, nil)
				if !tc.handled && tc.err != nil {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, want, state)
				require.Len(t, f.PodList, 2, "filtering must not mutate the shared source slice")
				require.Same(t, whole, f.PodList[0])
			})
		})
	}
}
