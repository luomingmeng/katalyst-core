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

package policy

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	cpustate "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/mb/advisor"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/mb/allocator"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/mb/domain"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/mb/monitor"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/mb/plan"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/mb/reader"
	"github.com/kubewharf/katalyst-core/pkg/config"
	"github.com/kubewharf/katalyst-core/pkg/config/agent"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/eviction"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type mockAdvisor struct {
	mock.Mock
}

func (ma *mockAdvisor) GetPlan(ctx context.Context, domainsMon *monitor.DomainStats) (*plan.MBPlan, error) {
	args := ma.Called(ctx, domainsMon)
	return args.Get(0).(*plan.MBPlan), args.Error(1)
}

func (ma *mockAdvisor) GetSuppressedCCDs() []advisor.SuppressedCCD {
	args := ma.Called()
	return args.Get(0).([]advisor.SuppressedCCD)
}

type mockPlanAlloctor struct {
	mock.Mock
	allocator.PlanAllocator
}

func (mp *mockPlanAlloctor) Allocate(ctx context.Context, plan *plan.MBPlan) error {
	args := mp.Called(ctx, plan)
	return args.Error(0)
}

type mockReader struct {
	mock.Mock
}

func (mr *mockReader) GetMBData() (*reader.MBData, error) {
	args := mr.Called()
	return args.Get(0).(*reader.MBData), args.Error(1)
}

type mockMetricEmitter struct {
	mock.Mock
	metrics.MetricEmitter
}

func (m *mockMetricEmitter) StoreInt64(key string, val int64, emitType metrics.MetricTypeName, tags ...metrics.MetricTag) error {
	args := m.Called(key, val, emitType, tags)
	return args.Error(0)
}

func TestBuildCCDToPodsMapFromState(t *testing.T) {
	t.Parallel()

	newAllocationInfo := func(cpus ...int) *cpustate.AllocationInfo {
		return &cpustate.AllocationInfo{
			AllocationMeta: commonstate.AllocationMeta{
				ContainerType: pluginapi.ContainerType_MAIN.String(),
			},
			AllocationResult: machine.NewCPUSet(cpus...),
		}
	}

	tests := []struct {
		name        string
		cpuTopology machine.CPUDetails
		podEntries  cpustate.PodEntries
		getPod      getPodFunc
		want        map[int]map[string]*corev1.Pod
	}{
		{
			name: "happy path - two pods on different ccds",
			cpuTopology: machine.CPUDetails{
				0: {L3CacheID: 0}, 1: {L3CacheID: 0},
				2: {L3CacheID: 1}, 3: {L3CacheID: 1},
			},
			podEntries: cpustate.PodEntries{
				"uid-1": cpustate.ContainerEntries{"main": newAllocationInfo(0, 1)},
				"uid-2": cpustate.ContainerEntries{"main": newAllocationInfo(2, 3)},
			},
			getPod: successPodFetcher,
			want: map[int]map[string]*corev1.Pod{
				0: {"uid-1": stubPod("uid-1")},
				1: {"uid-2": stubPod("uid-2")},
			},
		},
		{
			name: "single pod spans multiple ccds",
			cpuTopology: machine.CPUDetails{
				0: {L3CacheID: 0},
				2: {L3CacheID: 1},
			},
			podEntries: cpustate.PodEntries{
				"uid-1": cpustate.ContainerEntries{"main": newAllocationInfo(0, 2)},
			},
			getPod: successPodFetcher,
			want: map[int]map[string]*corev1.Pod{
				0: {"uid-1": stubPod("uid-1")},
				1: {"uid-1": stubPod("uid-1")},
			},
		},
		{
			name:        "nil pod entries",
			cpuTopology: machine.CPUDetails{0: {L3CacheID: 0}},
			podEntries:  nil,
			want:        map[int]map[string]*corev1.Pod{},
		},
		{
			name: "nil allocation info is skipped",
			cpuTopology: machine.CPUDetails{
				0: {L3CacheID: 0},
			},
			podEntries: cpustate.PodEntries{
				"uid-1": cpustate.ContainerEntries{"main": nil},
			},
			want: map[int]map[string]*corev1.Pod{},
		},
		{
			name: "getPod returns error",
			cpuTopology: machine.CPUDetails{
				0: {L3CacheID: 0},
			},
			podEntries: cpustate.PodEntries{
				"uid-1": cpustate.ContainerEntries{"main": newAllocationInfo(0)},
			},
			getPod: func(_ context.Context, _ string) (*corev1.Pod, error) {
				return nil, errors.New("fetch error")
			},
			want: map[int]map[string]*corev1.Pod{},
		},
		{
			name: "getPod returns nil pod",
			cpuTopology: machine.CPUDetails{
				0: {L3CacheID: 0},
			},
			podEntries: cpustate.PodEntries{
				"uid-1": cpustate.ContainerEntries{"main": newAllocationInfo(0)},
			},
			getPod: func(_ context.Context, _ string) (*corev1.Pod, error) {
				return nil, nil
			},
			want: map[int]map[string]*corev1.Pod{},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := buildCCDToPodsMapFromState(tt.podEntries, tt.cpuTopology, tt.getPod)

			assert.Equal(t, len(tt.want), len(got))
			for ccdID, expectedPods := range tt.want {
				gotPods, ok := got[ccdID]
				assert.True(t, ok, "ccd %d not found in result", ccdID)
				assert.Equal(t, len(expectedPods), len(gotPods))
				for podUID, expectedPod := range expectedPods {
					gotPod, ok := gotPods[podUID]
					assert.True(t, ok, "pod %s not found in ccd %d", podUID, ccdID)
					assert.Equal(t, expectedPod.Name, gotPod.Name)
				}
			}
		})
	}
}

func successPodFetcher(_ context.Context, podUID string) (*corev1.Pod, error) {
	return stubPod(podUID), nil
}

func stubPod(uid string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid), Name: "pod-" + uid}}
}

func TestMBPlugin_run(t *testing.T) {
	t.Parallel()

	dummyPlan := &plan.MBPlan{}

	mReader := new(mockReader)
	mReader.On("GetMBData").Return(&reader.MBData{
		MBBody: monitor.GroupMBStats{
			"/": {
				0: {
					LocalMB:  888,
					RemoteMB: 222,
					TotalMB:  1110,
				},
				1: {
					LocalMB:  777,
					RemoteMB: 333,
					TotalMB:  1110,
				},
			},
		},
		UpdateTime: 0,
	}, nil)

	mAdvisor := new(mockAdvisor)
	mAdvisor.On("GetPlan", mock.Anything, mock.Anything).Return(dummyPlan, nil)

	mPlanAllocator := new(mockPlanAlloctor)
	mPlanAllocator.On("Allocate", mock.Anything, dummyPlan).Return(nil)

	type fields struct {
		chStop        chan struct{}
		emitter       metrics.MetricEmitter
		ccdToDomain   map[int]int
		xDomGroups    sets.String
		domains       domain.Domains
		reader        reader.MBReader
		advisor       advisor.Advisor
		planAllocator allocator.PlanAllocator
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "happy path",
			fields: fields{
				emitter:       &metrics.DummyMetrics{},
				ccdToDomain:   map[int]int{0: 0, 1: 0},
				xDomGroups:    nil,
				domains:       nil,
				reader:        mReader,
				advisor:       mAdvisor,
				planAllocator: mPlanAllocator,
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := &MBPlugin{
				chStop:        tt.fields.chStop,
				emitter:       tt.fields.emitter,
				ccdToDomain:   tt.fields.ccdToDomain,
				xDomGroups:    tt.fields.xDomGroups,
				domains:       tt.fields.domains,
				reader:        tt.fields.reader,
				advisor:       tt.fields.advisor,
				planAllocator: tt.fields.planAllocator,
			}
			m.run()
			mock.AssertExpectationsForObjects(t, tt.fields.advisor)
			mock.AssertExpectationsForObjects(t, tt.fields.planAllocator)
		})
	}
}

func TestEmitSuppressedPodsMetrics(t *testing.T) {
	t.Parallel()

	podUID1 := "uid-1"
	podUID2 := "uid-2"

	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "uid-1",
			Namespace: "ns1",
			Name:      "pod-a",
			Labels:    map[string]string{"psm": "svc-a"},
		},
	}
	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "uid-2",
			Namespace: "ns2",
			Name:      "pod-b",
		},
	}

	ccdToPods := map[int]map[string]*corev1.Pod{
		0: {podUID1: pod1},
		1: {podUID2: pod2},
		2: {podUID1: pod1},
		3: {podUID2: pod2},
	}

	suppressedCCDs := []advisor.SuppressedCCD{
		{CCDID: 0, SuppressionType: "domain_stress"},
		{CCDID: 1, SuppressionType: "ccd_limit"},
		{CCDID: 2, SuppressionType: "domain_stress"},
		{CCDID: 3, SuppressionType: "ccd_limit"},
		{CCDID: 99, SuppressionType: "domain_stress"},
	}

	emitter := new(mockMetricEmitter)
	emitter.On("StoreInt64", "mbm_load_suppressed", int64(1), metrics.MetricTypeNameRaw,
		mock.MatchedBy(func(tags []metrics.MetricTag) bool {
			return tagsMatch(tags, map[string]string{
				"suppressed_namespace": "ns1",
				"suppressed_pod":       "pod-a",
				"suppression_type":     "domain_stress",
				"psm":                  "svc-a",
			})
		})).Return(nil)
	emitter.On("StoreInt64", "mbm_load_suppressed", int64(1), metrics.MetricTypeNameRaw,
		mock.MatchedBy(func(tags []metrics.MetricTag) bool {
			return tagsMatch(tags, map[string]string{
				"suppressed_namespace": "ns2",
				"suppressed_pod":       "pod-b",
				"suppression_type":     "ccd_limit",
			})
		})).Return(nil)

	conf := &config.Configuration{
		AgentConfiguration: &agent.AgentConfiguration{
			GenericAgentConfiguration: &agent.GenericAgentConfiguration{
				GenericEvictionConfiguration: &eviction.GenericEvictionConfiguration{
					PodMetricLabels: sets.NewString("psm"),
				},
			},
		},
	}

	m := &MBPlugin{emitter: emitter, conf: conf}
	m.emitSuppressedPodsMetrics(suppressedCCDs, ccdToPods)

	mock.AssertExpectationsForObjects(t, emitter)
}

func tagsMatch(tags []metrics.MetricTag, expected map[string]string) bool {
	if len(tags) != len(expected) {
		return false
	}
	for _, tag := range tags {
		if v, ok := expected[tag.Key]; !ok || v != tag.Val {
			return false
		}
	}
	return true
}
