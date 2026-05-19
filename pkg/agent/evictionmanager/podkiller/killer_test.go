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

package podkiller

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes/fake"
	coretesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/events"
	critesting "k8s.io/cri-api/pkg/apis/testing"

	katalyst_base "github.com/kubewharf/katalyst-core/cmd/base"
	"github.com/kubewharf/katalyst-core/pkg/config"
	evictionconfig "github.com/kubewharf/katalyst-core/pkg/config/agent/eviction"
	"github.com/kubewharf/katalyst-core/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
)

func TestEvictionQueue(t *testing.T) {
	t.Parallel()

	pods := []*v1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-1"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-2"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-3"},
		},
	}

	ctx, err := katalyst_base.GenerateFakeGenericContext([]runtime.Object{pods[0], pods[1]})
	require.NoError(t, err)

	deleteKiller, BuildErr := NewDeletionAPIKiller(nil, ctx.Client.KubeClient, &events.FakeRecorder{}, metrics.DummyMetrics{})
	require.NoError(t, BuildErr)

	err = deleteKiller.Evict(context.Background(), pods[0], 0, "test-api", "test")
	require.NoError(t, err)

	err = deleteKiller.Evict(context.Background(), pods[0], 0, "test-api", "test")
	require.NoError(t, err)

	err = deleteKiller.Evict(context.Background(), pods[1], 0, "test-api", "test")
	require.NoError(t, err)

	err = evict(ctx.Client.KubeClient, &events.FakeRecorder{}, metrics.DummyMetrics{}, pods[2],
		0, "test-api", "test", func(_ *v1.Pod, gracePeriod int64) error {
			return fmt.Errorf("test")
		})
	require.ErrorContains(t, err, "test")
}

func TestContainerKiller_Evict(t *testing.T) {
	t.Parallel()

	fakeRuntimeService := critesting.NewFakeRuntimeService()
	containerKiller := &ContainerKiller{
		containerManager: fakeRuntimeService,
		recorder:         events.NewFakeRecorder(5),
		emitter:          metrics.DummyMetrics{},
	}
	containerKiller.containerManager = fakeRuntimeService
	assert.Equal(t, consts.KillerNameContainerKiller, containerKiller.Name())

	fakeRuntimeService.Containers["container-01"] = &critesting.FakeContainer{}
	fakeRuntimeService.Containers["container-02"] = &critesting.FakeContainer{}

	tests := []struct {
		name                   string
		pod                    *v1.Pod
		wantKillContainerCount int
		wantStopFailed         bool
	}{
		{
			name: "1 container",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "container-01",
						},
					},
				},
				Status: v1.PodStatus{
					ContainerStatuses: []v1.ContainerStatus{
						{
							Name:        "container-01",
							ContainerID: "containerd://container-01",
						},
					},
				},
			},
			wantKillContainerCount: 1,
			wantStopFailed:         false,
		},
		{
			name: "2 containers",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "container-01",
						},
						{
							Name: "container-02",
						},
					},
				},
				Status: v1.PodStatus{
					ContainerStatuses: []v1.ContainerStatus{
						{
							Name:        "container-01",
							ContainerID: "containerd://container-01",
						},
						{
							Name:        "container-02",
							ContainerID: "containerd://container-02",
						},
					},
				},
			},
			wantKillContainerCount: 2,
			wantStopFailed:         false,
		},
		{
			name: "1 container but has error",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "container-03",
						},
					},
				},
				Status: v1.PodStatus{
					ContainerStatuses: []v1.ContainerStatus{
						{
							Name:        "container-03",
							ContainerID: "containerd://container-03",
						},
					},
				},
			},
			wantStopFailed: true,
		},
	}
	for _, tt := range tests {
		fakeRuntimeService.Called = make([]string, 0)
		evictErr := containerKiller.Evict(context.TODO(), tt.pod, 0, "test", "test")
		if tt.wantStopFailed {
			require.Error(t, evictErr)
		} else {
			require.NoError(t, evictErr)
			require.Equal(t, tt.wantKillContainerCount, len(fakeRuntimeService.GetCalls()))
		}
	}
}

func TestEvictionAPIKiller_buildEvictionExplicitTriggerAnnotations(t *testing.T) {
	t.Parallel()

	newConf := func(podAnnos map[string]sets.String, evictKey string) *config.Configuration {
		c := config.NewConfiguration()
		c.GenericEvictionConfiguration.EvictionExplicitTriggerAnnotationConfig = &evictionconfig.EvictionExplicitTriggerAnnotationConfig{
			ExplicitlyTriggeringPodAnnotations: podAnnos,
			ExplicitTriggerAnnotationKey:       evictKey,
		}
		return c
	}

	tests := []struct {
		name           string
		conf           *config.Configuration
		podAnnotations map[string]string
		want           map[string]string
	}{
		{
			name:           "nil conf returns nil",
			conf:           nil,
			podAnnotations: map[string]string{"shield": "strict"},
			want:           nil,
		},
		{
			name:           "empty trigger key disables rule",
			conf:           newConf(map[string]sets.String{"shield": sets.NewString("strict")}, ""),
			podAnnotations: map[string]string{"shield": "strict"},
			want:           nil,
		},
		{
			name:           "empty ExplicitlyTriggeringPodAnnotations disables rule",
			conf:           newConf(map[string]sets.String{}, "evicted-by"),
			podAnnotations: map[string]string{"shield": "strict"},
			want:           nil,
		},
		{
			name:           "single key/value match",
			conf:           newConf(map[string]sets.String{"shield": sets.NewString("strict")}, "evicted-by"),
			podAnnotations: map[string]string{"shield": "strict"},
			want:           map[string]string{"evicted-by": evictionconfig.EvictionExplicitTriggerAnnotationValue},
		},
		{
			name:           "match via second value in set",
			conf:           newConf(map[string]sets.String{"shield": sets.NewString("strict", "loose")}, "evicted-by"),
			podAnnotations: map[string]string{"shield": "loose"},
			want:           map[string]string{"evicted-by": evictionconfig.EvictionExplicitTriggerAnnotationValue},
		},
		{
			name: "match via second key in map",
			conf: newConf(map[string]sets.String{
				"shield":    sets.NewString("strict"),
				"protected": sets.NewString("true"),
			}, "evicted-by"),
			podAnnotations: map[string]string{"protected": "true"},
			want:           map[string]string{"evicted-by": evictionconfig.EvictionExplicitTriggerAnnotationValue},
		},
		{
			name:           "no match: wrong value",
			conf:           newConf(map[string]sets.String{"shield": sets.NewString("strict")}, "evicted-by"),
			podAnnotations: map[string]string{"shield": "off"},
			want:           nil,
		},
		{
			name:           "no match: key missing",
			conf:           newConf(map[string]sets.String{"shield": sets.NewString("strict")}, "evicted-by"),
			podAnnotations: map[string]string{"other": "anything"},
			want:           nil,
		},
		{
			name:           "no match: pod has nil annotations",
			conf:           newConf(map[string]sets.String{"shield": sets.NewString("strict")}, "evicted-by"),
			podAnnotations: nil,
			want:           nil,
		},
		{
			name:           "wildcard: empty allowed values matches any pod value",
			conf:           newConf(map[string]sets.String{"shield": sets.NewString()}, "evicted-by"),
			podAnnotations: map[string]string{"shield": "anything"},
			want:           map[string]string{"evicted-by": evictionconfig.EvictionExplicitTriggerAnnotationValue},
		},
		{
			name:           "wildcard: empty allowed values still requires the key",
			conf:           newConf(map[string]sets.String{"shield": sets.NewString()}, "evicted-by"),
			podAnnotations: map[string]string{"other": "anything"},
			want:           nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := &EvictionAPIKiller{conf: tt.conf}
			pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: tt.podAnnotations}}
			got := e.buildEvictionExplicitTriggerAnnotations(pod)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEvictionAPIKiller_EvictStampsExplicitTriggerAnnotation(t *testing.T) {
	t.Parallel()

	type tcase struct {
		name           string
		podAnnotations map[string]string
		wantAnnos      map[string]string
	}
	cases := []tcase{
		{
			name:           "matching pod gets explicit-trigger annotation",
			podAnnotations: map[string]string{"shield": "strict"},
			wantAnnos:      map[string]string{"evicted-by": evictionconfig.EvictionExplicitTriggerAnnotationValue},
		},
		{
			name:           "non-matching pod gets no explicit-trigger annotation",
			podAnnotations: map[string]string{"shield": "off"},
			wantAnnos:      nil,
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "pod-1",
					Namespace:   "ns-1",
					UID:         "uid-1",
					Annotations: tt.podAnnotations,
				},
			}

			ctx, err := katalyst_base.GenerateFakeGenericContext([]runtime.Object{pod})
			require.NoError(t, err)
			fakeKube := ctx.Client.KubeClient.(*fake.Clientset)

			var captured *policy.Eviction
			fakeKube.PrependReactor("create", "pods", func(action coretesting.Action) (bool, runtime.Object, error) {
				if action.GetSubresource() != "eviction" {
					return false, nil, nil
				}
				captured = action.(coretesting.CreateAction).GetObject().(*policy.Eviction)
				// Simulate the API server actually deleting the pod so that
				// waitForDeleted in evict() returns promptly.
				_ = fakeKube.Tracker().Delete(
					v1.SchemeGroupVersion.WithResource("pods"),
					captured.Namespace, captured.Name,
				)
				return true, nil, nil
			})

			conf := config.NewConfiguration()
			conf.GenericEvictionConfiguration.EvictionExplicitTriggerAnnotationConfig = &evictionconfig.EvictionExplicitTriggerAnnotationConfig{
				ExplicitlyTriggeringPodAnnotations: map[string]sets.String{"shield": sets.NewString("strict")},
				ExplicitTriggerAnnotationKey:       "evicted-by",
			}

			killer, err := NewEvictionAPIKiller(conf, ctx.Client.KubeClient, &events.FakeRecorder{}, metrics.DummyMetrics{})
			require.NoError(t, err)

			require.NoError(t, killer.Evict(context.Background(), pod, 0, "test-api", "test"))

			require.NotNil(t, captured)
			assert.Equal(t, tt.wantAnnos, captured.ObjectMeta.Annotations)
		})
	}
}
