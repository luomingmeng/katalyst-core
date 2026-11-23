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

package malachite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apitypes "k8s.io/apimachinery/pkg/types"

	"github.com/kubewharf/katalyst-core/pkg/config/agent/global"
	"github.com/kubewharf/katalyst-core/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/provisioner/malachite/client"
	mt "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/provisioner/malachite/types"
	metrictypes "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/types"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/cgroup/common"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	utilmetric "github.com/kubewharf/katalyst-core/pkg/util/metric"
)

type containerInventoryFetcher struct {
	pod.PodFetcherStub
	listErr error
	getErr  error
}

func (f *containerInventoryFetcher) GetPod(ctx context.Context, uid string) (*v1.Pod, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.PodFetcherStub.GetPod(ctx, uid)
}

func (f *containerInventoryFetcher) GetPodList(ctx context.Context, filter func(*v1.Pod) bool) ([]*v1.Pod, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.PodFetcherStub.GetPodList(ctx, filter)
}

func (f *containerInventoryFetcher) GetPodMetricTargets(context.Context) ([]metrictypes.PodMetricTarget, error) {
	return nil, nil
}

func (f *containerInventoryFetcher) ResolvePodMetricTarget(_ context.Context, p *v1.Pod) (metrictypes.PodMetricTarget, bool, error) {
	if p.Annotations["whole-workload"] == "true" {
		return metrictypes.PodMetricTarget{}, true, errors.New("mapping unavailable")
	}
	if p.Annotations["resolve-error"] == "true" {
		return metrictypes.PodMetricTarget{}, false, errors.New("target source unavailable")
	}
	return metrictypes.PodMetricTarget{}, false, nil
}

func TestContainerSamplingFailuresDoNotAffectReadiness(t *testing.T) {
	general.RegisterReportCheck(malachiteProvisionerHealthCheckName, 0, general.HealthzCheckStateReady)
	t.Cleanup(func() { general.UnregisterHealthCheck(malachiteProvisionerHealthCheckName) })

	for _, scenario := range []string{"running sample failure", "terminal sample failure", "partial samples", "pod reread failure", "inventory failure", "system failure"} {
		t.Run(scenario, func(t *testing.T) {
			uid := "readiness-" + scenario
			p := &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: apitypes.UID(uid)}, Status: v1.PodStatus{
				Phase:             v1.PodRunning,
				ContainerStatuses: []v1.ContainerStatus{{Name: "failed", ContainerID: "containerd://failed", State: v1.ContainerState{Running: &v1.ContainerStateRunning{}}}},
			}}
			f := &containerInventoryFetcher{PodFetcherStub: pod.PodFetcherStub{PodList: []*v1.Pod{p}}}
			switch scenario {
			case "terminal sample failure":
				p.Status.Phase = v1.PodSucceeded
				p.Status.ContainerStatuses[0].State = v1.ContainerState{Terminated: &v1.ContainerStateTerminated{}}
			case "partial samples":
				p.Status.ContainerStatuses = append(p.Status.ContainerStatuses, v1.ContainerStatus{Name: "success", ContainerID: "containerd://success"})
			case "pod reread failure":
				f.getErr = context.DeadlineExceeded
			case "inventory failure":
				f.listErr = context.DeadlineExceeded
			}
			unregister := common.RegisterRelativeCgroupPathHandlerWithUnregister(common.RelativeCgroupPathHandler{
				Name: uid,
				Handler: func(podUID, id string) (string, bool, error) {
					if podUID != uid {
						return "", true, nil
					}
					if id != "success" {
						return "", false, fmt.Errorf("container cgroup unavailable")
					}
					return uid + "/" + id, false, nil
				},
			})
			defer unregister()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/fail" {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				if r.URL.Path == "/cgroup" {
					subsys := mt.SubSystemGroupsV1{}
					subsys.Memory.V1.MemoryV1Data = mt.MemoryCgDataV1{TotalRss: 7, UpdateTime: time.Now().Unix()}
					subsys.Cpuacct.V1.CPUData.UpdateTime = time.Now().Unix()
					raw, err := json.Marshal(subsys)
					require.NoError(t, err)
					require.NoError(t, json.NewEncoder(w).Encode(mt.MalachiteCgroupResponse{Data: mt.CgroupDataInner{CgroupType: "V1", SubSystemGroups: raw}}))
					return
				}
				require.NoError(t, json.NewEncoder(w).Encode(map[string]interface{}{"status": 0, "data": map[string]interface{}{"update_time": time.Now().Unix()}}))
			}))
			defer server.Close()
			c := client.NewMalachiteClient(f, metrics.DummyMetrics{})
			urls := map[string]string{client.CgroupResource: server.URL + "/cgroup"}
			for _, resource := range []string{client.SystemInfoResource, client.SystemComputeResource, client.SystemMemoryResource, client.SystemIOResource, client.SystemNetResource} {
				urls[resource] = server.URL + "/system"
			}
			if scenario == "system failure" {
				urls[client.SystemMemoryResource] = server.URL + "/fail"
			}
			c.SetURL(urls)
			store := utilmetric.NewMetricStore()
			oldTime := time.Now().Add(-time.Minute)
			old := utilmetric.MetricData{Value: 99, Time: &oldTime}
			store.SetContainerMetric(uid, "failed", consts.MetricMemRssContainer, old)
			m := &MalachiteMetricsProvisioner{podFetcher: f, malachiteClient: c, metricStore: store, emitter: metrics.DummyMetrics{},
				baseConf: &global.BaseConfiguration{MalachiteConfiguration: &global.MalachiteConfiguration{}}, pathExistsFn: func(string) bool { return false }, reclaimPathsFn: func() []string { return nil }}
			// Start from NotReady to prove a successful collection round recovers it.
			require.NoError(t, general.UpdateHealthzState(malachiteProvisionerHealthCheckName, general.HealthzCheckStateNotReady, "previous failure"))
			m.sample(context.Background())
			result := general.GetRegisterReadinessCheckResult()[general.HealthzCheckName(malachiteProvisionerHealthCheckName)]
			wantReady := scenario != "inventory failure" && scenario != "system failure"
			require.Equal(t, wantReady, result.Ready, result.Message)
			if wantReady {
				require.Empty(t, result.Message)
			}
			value, err := store.GetContainerMetric(uid, "failed", consts.MetricMemRssContainer)
			require.NoError(t, err)
			require.Equal(t, old, value)
			if scenario == "partial samples" {
				value, err := store.GetContainerMetric(uid, "success", consts.MetricMemRssContainer)
				require.NoError(t, err)
				require.Equal(t, float64(7), value.Value)
			}
		})
	}
}

func TestContainerMetricGCUsesInventory(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"list failure", "empty inventory", "no container status", "pod reread failure", "sample failure", "partial samples", "whole workload", "nil member", "empty UID", "duplicate UID", "resolve failure"} {
		scenario := scenario
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			f := &containerInventoryFetcher{}
			oldTime := time.Now().Add(-time.Minute)
			old := utilmetric.MetricData{Value: 99, Time: &oldTime}
			store := utilmetric.NewMetricStore()
			store.SetPodMetrics("sandbox", utilmetric.PodMetricData{
				CgroupPath: "/workload", MappingRevision: 1,
				Metrics: map[string]utilmetric.MetricData{consts.MetricMemRssCgroup: old},
			})
			uid := "metric-inventory-" + scenario
			for _, member := range []string{uid, "deleted"} {
				store.SetContainerMetric(member, "failed", consts.MetricMemRssContainer, old)
				store.SetContainerNumaMetric(member, "failed", 0, consts.MetricsMemTotalPerNumaContainer, old)
			}
			living := &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: apitypes.UID(uid)}, Status: v1.PodStatus{
				ContainerStatuses: []v1.ContainerStatus{{Name: "failed", ContainerID: "containerd://failed"}},
			}}
			switch scenario {
			case "list failure":
				f.listErr = context.DeadlineExceeded
			case "empty inventory":
			case "no container status":
				living.Status.ContainerStatuses = nil
				f.PodList = []*v1.Pod{living}
			case "pod reread failure":
				f.getErr = context.DeadlineExceeded
				f.PodList = []*v1.Pod{living}
			case "sample failure":
				f.PodList = []*v1.Pod{living}
			case "partial samples":
				living.Status.ContainerStatuses = append(living.Status.ContainerStatuses, v1.ContainerStatus{Name: "success", ContainerID: "containerd://success"})
				f.PodList = []*v1.Pod{living}
			case "nil member":
				f.PodList = []*v1.Pod{living, nil}
			case "empty UID":
				f.PodList = []*v1.Pod{living, {}}
			case "duplicate UID":
				f.PodList = []*v1.Pod{living, living.DeepCopy()}
			case "resolve failure":
				living.Annotations = map[string]string{"resolve-error": "true"}
				f.PodList = []*v1.Pod{living}
			case "whole workload":
				living.Annotations = map[string]string{"whole-workload": "true"}
				f.PodList = []*v1.Pod{living}
			}
			unregister := common.RegisterRelativeCgroupPathHandlerWithUnregister(common.RelativeCgroupPathHandler{
				Name: uid,
				Handler: func(podUID, id string) (string, bool, error) {
					if podUID != uid {
						return "", true, nil
					}
					return fmt.Sprintf("%s/%s", podUID, id), false, nil
				},
			})
			defer unregister()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get(client.CgroupPathParamKey) != uid+"/success" {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				subsys := mt.SubSystemGroupsV1{}
				subsys.Memory.V1.MemoryV1Data = mt.MemoryCgDataV1{TotalRss: 7, UpdateTime: time.Now().Unix()}
				subsys.Cpuacct.V1.CPUData.UpdateTime = time.Now().Unix()
				raw, err := json.Marshal(subsys)
				require.NoError(t, err)
				require.NoError(t, json.NewEncoder(w).Encode(mt.MalachiteCgroupResponse{Data: mt.CgroupDataInner{CgroupType: "V1", SubSystemGroups: raw}}))
			}))
			defer server.Close()
			c := client.NewMalachiteClient(f, metrics.DummyMetrics{})
			c.SetURL(map[string]string{client.CgroupResource: server.URL})
			m := &MalachiteMetricsProvisioner{podFetcher: f, malachiteClient: c, metricStore: store, emitter: metrics.DummyMetrics{}, baseConf: &global.BaseConfiguration{MalachiteConfiguration: &global.MalachiteConfiguration{}}}
			inventoryFailed := scenario == "list failure" || scenario == "nil member" || scenario == "empty UID" || scenario == "duplicate UID" || scenario == "resolve failure"
			err := m.updatePodsCgroupData(context.Background())
			if inventoryFailed {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			for _, member := range []string{uid, "deleted"} {
				value, metricErr := store.GetContainerMetric(member, "failed", consts.MetricMemRssContainer)
				numa, numaErr := store.GetContainerNumaMetric(member, "failed", 0, consts.MetricsMemTotalPerNumaContainer)
				keep := inventoryFailed || (member == uid && scenario != "empty inventory" && scenario != "whole workload")
				if keep {
					require.NoError(t, metricErr)
					require.NoError(t, numaErr)
					require.Equal(t, old, value)
					require.Equal(t, old, numa)
				} else {
					require.Error(t, metricErr)
					require.Error(t, numaErr)
				}
			}
			if scenario == "partial samples" {
				value, err := store.GetContainerMetric(uid, "success", consts.MetricMemRssContainer)
				require.NoError(t, err)
				require.Equal(t, float64(7), value.Value)
			}
			sandbox, err := store.GetPodMetric("sandbox", "/workload", 1, consts.MetricMemRssCgroup, -1)
			require.NoError(t, err)
			require.Equal(t, old, sandbox)
			// A subsequent complete empty inventory is an authoritative deletion,
			// including after a failed previous read.
			f.listErr = nil
			f.PodList = nil
			require.NoError(t, m.updatePodsCgroupData(context.Background()))
			_, err = store.GetContainerMetric(uid, "failed", consts.MetricMemRssContainer)
			require.Error(t, err)
		})
	}
}
