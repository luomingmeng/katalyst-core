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

package util

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"

	"github.com/kubewharf/katalyst-api/pkg/apis/workload/v1alpha1"
	"github.com/kubewharf/katalyst-api/pkg/consts"
)

func TestDeploySPDBaselineCoefficient_Cmp(t *testing.T) {
	t.Parallel()

	type args struct {
		c1 SPDBaselinePodMeta
	}
	tests := []struct {
		name string
		c    SPDBaselinePodMeta
		args args
		want int
	}{
		{
			name: "less",
			c: SPDBaselinePodMeta{
				TimeStamp: metav1.NewTime(time.UnixMilli(0)),
				PodName:   "pod2",
			},
			args: args{
				c1: SPDBaselinePodMeta{
					TimeStamp: metav1.NewTime(time.UnixMilli(1)),
					PodName:   "pod3",
				},
			},
			want: -1,
		},
		{
			name: "greater",
			c: SPDBaselinePodMeta{
				TimeStamp: metav1.NewTime(time.UnixMilli(1)),
				PodName:   "pod3",
			},
			args: args{
				c1: SPDBaselinePodMeta{
					TimeStamp: metav1.NewTime(time.UnixMilli(1)),
					PodName:   "pod2",
				},
			},
			want: 1,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.c.Cmp(tt.args.c1); got != tt.want {
				t.Errorf("Cmp() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBaselineCoefficient_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		c    SPDBaselinePodMeta
		want string
	}{
		{
			name: "deploy SPD",
			c: SPDBaselinePodMeta{
				TimeStamp: metav1.NewTime(time.UnixMilli(1)),
				PodName:   "pod2",
			},
			want: "{\"timeStamp\":\"1970-01-01T00:00:00Z\",\"podName\":\"pod2\",\"customCompareKey\":\"\",\"customCompareValue\":null}",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.c.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetPodBaselineCoefficient(t *testing.T) {
	t.Parallel()

	type args struct {
		pod              *v1.Pod
		customCompareKey CustomCompareKey
	}
	tests := []struct {
		name string
		args args
		want *SPDBaselinePodMeta
	}{
		{
			name: "deploy spd normal",
			args: args{
				pod: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "test-pod",
						CreationTimestamp: metav1.NewTime(time.Date(2023, time.August, 1, 0, 0, 0, 0, time.Local)),
					},
				},
			},
			want: &SPDBaselinePodMeta{
				TimeStamp: metav1.NewTime(time.Date(2023, time.August, 1, 0, 0, 0, 0, time.Local)),
				PodName:   "test-pod",
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := GetSPDBaselinePodMeta(tt.args.pod.ObjectMeta, tt.args.customCompareKey)
			if err != nil {
				t.Errorf("getSPDBaselinePodMeta() error = %v", err)
				return
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetDeploySPDBaselinePercentile(t *testing.T) {
	t.Parallel()

	type args struct {
		spd *v1alpha1.ServiceProfileDescriptor
	}
	tests := []struct {
		name    string
		args    args
		want    SPDBaselinePodMeta
		wantErr bool
	}{
		{
			name: "normal",
			args: args{
				spd: &v1alpha1.ServiceProfileDescriptor{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-spd",
						Annotations: map[string]string{
							consts.SPDAnnotationBaselineSentinelKey: "{\"timeStamp\":\"2023-12-01T00:00:00Z\",\"podName\":\"pod1\"}",
						},
					},
				},
			},
			want: SPDBaselinePodMeta{
				TimeStamp: metav1.NewTime(time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC)),
				PodName:   "pod1",
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := GetSPDBaselineSentinel(tt.args.spd)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetSPDBaselineSentinel() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, tt.want.PodName, got.PodName)
			assert.True(t, tt.want.TimeStamp.Equal(&got.TimeStamp))
		})
	}
}

func TestIsBaselinePod(t *testing.T) {
	t.Parallel()

	type args struct {
		pod              *v1.Pod
		baselinePercent  *int32
		baselineSentinel *SPDBaselinePodMeta
		customCompareKey CustomCompareKey
	}
	tests := []struct {
		name              string
		args              args
		wantIsBaselinePod bool
		wantErr           bool
	}{
		{
			name: "deploy baseline pod",
			args: args{
				pod: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "test-pod",
						CreationTimestamp: metav1.NewTime(time.Date(2023, time.August, 1, 0, 0, 0, 0, time.UTC)),
					},
				},
				baselinePercent: pointer.Int32(10),
				baselineSentinel: &SPDBaselinePodMeta{
					TimeStamp: metav1.NewTime(time.Date(2023, time.August, 1, 0, 0, 0, 0, time.UTC)),
					PodName:   "test-pod",
				},
			},
			wantIsBaselinePod: true,
		},
		{
			name: "deploy not baseline pod",
			args: args{
				pod: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "test-pod",
						CreationTimestamp: metav1.NewTime(time.Date(2023, time.August, 2, 0, 0, 0, 0, time.UTC)),
					},
				},
				baselinePercent: pointer.Int32(10),
				baselineSentinel: &SPDBaselinePodMeta{
					TimeStamp: metav1.Time{},
					PodName:   "",
				},
			},
			wantIsBaselinePod: false,
		},
		{
			name: "deploy baseline disabled",
			args: args{
				pod: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "test-pod",
						CreationTimestamp: metav1.NewTime(time.Date(2023, time.August, 2, 0, 0, 0, 0, time.UTC)),
					},
				},
				baselinePercent:  nil,
				baselineSentinel: nil,
			},
			wantIsBaselinePod: false,
		},
		{
			name: "deploy baseline 100%",
			args: args{
				pod: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "test-pod",
						CreationTimestamp: metav1.NewTime(time.Date(2023, time.August, 2, 0, 0, 0, 0, time.UTC)),
					},
				},
				baselinePercent:  pointer.Int32(100),
				baselineSentinel: nil,
			},
			wantIsBaselinePod: true,
		},
		{
			name: "deploy baseline 0%",
			args: args{
				pod: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "test-pod",
						CreationTimestamp: metav1.NewTime(time.Date(2023, time.August, 2, 0, 0, 0, 0, time.UTC)),
					},
				},
				baselinePercent:  pointer.Int32(0),
				baselineSentinel: nil,
			},
			wantIsBaselinePod: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := IsBaselinePod(tt.args.pod.ObjectMeta, tt.args.baselinePercent, tt.args.baselineSentinel, tt.args.customCompareKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("IsBaselinePod() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.wantIsBaselinePod {
				t.Errorf("IsBaselinePod() got = %v, want %v", got, tt.wantIsBaselinePod)
			}
		})
	}
}

func TestSetSPDBaselinePercentile(t *testing.T) {
	t.Parallel()

	type args struct {
		spd *v1alpha1.ServiceProfileDescriptor
		c   *SPDBaselinePodMeta
	}
	tests := []struct {
		name    string
		args    args
		wantSPD *v1alpha1.ServiceProfileDescriptor
	}{
		{
			name: "deploy add",
			args: args{
				spd: &v1alpha1.ServiceProfileDescriptor{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-spd",
					},
				},
				c: &SPDBaselinePodMeta{
					TimeStamp: metav1.NewTime(time.UnixMilli(1690848000)),
					PodName:   "test-spd",
				},
			},
			wantSPD: &v1alpha1.ServiceProfileDescriptor{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-spd",
					Annotations: map[string]string{
						consts.SPDAnnotationBaselineSentinelKey: "{\"timeStamp\":\"1970-01-20T13:40:48Z\",\"podName\":\"test-spd\",\"customCompareKey\":\"\",\"customCompareValue\":null}",
					},
				},
			},
		},
		{
			name: "deploy delete",
			args: args{
				spd: &v1alpha1.ServiceProfileDescriptor{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-spd",
						Annotations: map[string]string{
							consts.SPDAnnotationBaselineSentinelKey: "{\"timeStamp\":\"1970-01-20T13:40:48Z\",\"podName\":\"test-spd\"}",
						},
					},
				},
				c: nil,
			},
			wantSPD: &v1alpha1.ServiceProfileDescriptor{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-spd",
					Annotations: map[string]string{},
				},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			SetSPDBaselineSentinel(tt.args.spd, tt.args.c)
			assert.Equal(t, tt.wantSPD, tt.args.spd)
		})
	}
}

// fakeShardKeyProcessor derives a comparable shard index from the pod's "shard"
// annotation, used to exercise the custom-compare-key code path.
func fakeShardKeyProcessor(podMeta metav1.ObjectMeta, m *SPDBaselinePodMeta, customKey CustomCompareKey) error {
	v, ok := podMeta.Annotations["shard"]
	if !ok {
		return fmt.Errorf("pod %s missing shard annotation", podMeta.Name)
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fmt.Errorf("pod %s has invalid shard %q: %w", podMeta.Name, v, err)
	}
	m.CustomCompareKey = customKey
	m.CustomCompareValue = n
	return nil
}

func fakeShardCmp(c1, c2 SPDBaselinePodMeta) int {
	a, _ := c1.CustomCompareValue.(int)
	b, _ := c2.CustomCompareValue.(int)
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func TestRegisterAndGetSPDPodMetaCustomProcessor(t *testing.T) {
	t.Parallel()

	const key CustomCompareKey = "test_register_get_key"

	// not registered yet -> error
	_, err := GetSPDPodMetaCustomProcessor(key)
	assert.Error(t, err)

	proc := &PodMetaCustomProcessor{
		PodMetaCustomKeyProcessor: fakeShardKeyProcessor,
		PodMetaCustomCmp:          fakeShardCmp,
	}
	RegisterSPDPodMetaCustomProcessor(key, proc)

	got, err := GetSPDPodMetaCustomProcessor(key)
	assert.NoError(t, err)
	assert.Equal(t, proc, got)
}

func TestSPDBaselinePodMeta_CustomCmp(t *testing.T) {
	t.Parallel()

	const key CustomCompareKey = "test_custom_cmp_key"
	RegisterSPDPodMetaCustomProcessor(key, &PodMetaCustomProcessor{
		PodMetaCustomKeyProcessor: fakeShardKeyProcessor,
		PodMetaCustomCmp:          fakeShardCmp,
	})

	lower := SPDBaselinePodMeta{CustomCompareKey: key, CustomCompareValue: 1}
	higher := SPDBaselinePodMeta{CustomCompareKey: key, CustomCompareValue: 5}

	// custom comparator path (processor registered, PodMetaCustomCmp != nil)
	assert.Equal(t, -1, lower.Cmp(higher))
	assert.Equal(t, 1, higher.Cmp(lower))
	assert.Equal(t, 0, lower.Cmp(SPDBaselinePodMeta{CustomCompareKey: key, CustomCompareValue: 1}))

	// fallback path: custom key set but no processor registered -> default
	// timestamp/podName comparison, and crucially must NOT panic (regression guard).
	const unregistered CustomCompareKey = "test_custom_cmp_unregistered_key"
	earlier := SPDBaselinePodMeta{CustomCompareKey: unregistered, TimeStamp: metav1.NewTime(time.UnixMilli(0)), PodName: "a"}
	later := SPDBaselinePodMeta{CustomCompareKey: unregistered, TimeStamp: metav1.NewTime(time.UnixMilli(10)), PodName: "b"}
	assert.Equal(t, -1, earlier.Cmp(later))
	assert.Equal(t, 1, later.Cmp(earlier))

	// mismatched custom keys -> falls through to default comparison
	mixed1 := SPDBaselinePodMeta{CustomCompareKey: key, TimeStamp: metav1.NewTime(time.UnixMilli(0)), PodName: "a"}
	mixed2 := SPDBaselinePodMeta{CustomCompareKey: "other", TimeStamp: metav1.NewTime(time.UnixMilli(5)), PodName: "b"}
	assert.Equal(t, -1, mixed1.Cmp(mixed2))
}

func TestGetSPDBaselinePodMeta_CustomKey(t *testing.T) {
	t.Parallel()

	const key CustomCompareKey = "test_get_baseline_custom_key"
	RegisterSPDPodMetaCustomProcessor(key, &PodMetaCustomProcessor{
		PodMetaCustomKeyProcessor: fakeShardKeyProcessor,
		PodMetaCustomCmp:          fakeShardCmp,
	})

	// success: processor parses the shard annotation into the compare value
	pm, err := GetSPDBaselinePodMeta(metav1.ObjectMeta{
		Name:        "pod-shard-3",
		Annotations: map[string]string{"shard": "3"},
	}, key)
	assert.NoError(t, err)
	assert.Equal(t, key, pm.CustomCompareKey)
	assert.Equal(t, 3, pm.CustomCompareValue)
	assert.Equal(t, "pod-shard-3", pm.PodName)

	// key processor returns an error (missing shard annotation)
	_, err = GetSPDBaselinePodMeta(metav1.ObjectMeta{Name: "pod-no-shard"}, key)
	assert.Error(t, err)

	// custom key set but no processor registered for it
	_, err = GetSPDBaselinePodMeta(metav1.ObjectMeta{Name: "pod"}, CustomCompareKey("test_get_baseline_unregistered_key"))
	assert.Error(t, err)
}

func TestGetSPDCustomCompareKeys(t *testing.T) {
	t.Parallel()

	// annotation present
	spd := &v1alpha1.ServiceProfileDescriptor{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "spd-with-key",
			Annotations: map[string]string{consts.SPDAnnotationKeyCustomCompareKey: "shard_id"},
		},
	}
	assert.Equal(t, CustomCompareKey("shard_id"), GetSPDCustomCompareKeys(spd))

	// annotation absent
	spdNoKey := &v1alpha1.ServiceProfileDescriptor{
		ObjectMeta: metav1.ObjectMeta{Name: "spd-no-key"},
	}
	assert.Equal(t, CustomCompareKey(""), GetSPDCustomCompareKeys(spdNoKey))
}

func TestIsExtendedBaselinePod(t *testing.T) {
	t.Parallel()

	baselineTime := metav1.NewTime(time.Date(2023, time.August, 1, 0, 0, 0, 0, time.UTC))
	podMeta := metav1.ObjectMeta{Name: "test-pod", CreationTimestamp: baselineTime}

	// sentinel present for the indicator, pod at/under sentinel -> baseline
	got, err := IsExtendedBaselinePod(podMeta, pointer.Int32(10),
		map[string]*SPDBaselinePodMeta{"ind": {TimeStamp: baselineTime, PodName: "test-pod"}}, "ind", "")
	assert.NoError(t, err)
	assert.True(t, got)

	// indicator name absent -> sentinel nil while percent set -> error
	_, err = IsExtendedBaselinePod(podMeta, pointer.Int32(10),
		map[string]*SPDBaselinePodMeta{}, "missing", "")
	assert.Error(t, err)

	// baseline disabled (percent nil)
	got, err = IsExtendedBaselinePod(podMeta, nil, nil, "ind", "")
	assert.NoError(t, err)
	assert.False(t, got)
}

func TestExtendedBaselineSentinelRoundTrip(t *testing.T) {
	t.Parallel()

	spd := &v1alpha1.ServiceProfileDescriptor{ObjectMeta: metav1.ObjectMeta{Name: "spd"}}

	// not set -> nil, no error
	m, err := GetSPDExtendedBaselineSentinel(spd)
	assert.NoError(t, err)
	assert.Nil(t, m)

	// set then get round-trips
	SetSPDExtendedBaselineSentinel(spd, map[string]SPDBaselinePodMeta{
		"ind": {PodName: "pod-a", TimeStamp: metav1.NewTime(time.UnixMilli(1))},
	})
	got, err := GetSPDExtendedBaselineSentinel(spd)
	assert.NoError(t, err)
	assert.NotNil(t, got)
	assert.Equal(t, "pod-a", got["ind"].PodName)

	// delete via nil map
	SetSPDExtendedBaselineSentinel(spd, nil)
	_, ok := spd.Annotations[consts.SPDAnnotationExtendedBaselineSentinelKey]
	assert.False(t, ok)

	// invalid json -> error
	spdBad := &v1alpha1.ServiceProfileDescriptor{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "spd-bad",
			Annotations: map[string]string{consts.SPDAnnotationExtendedBaselineSentinelKey: "not-json"},
		},
	}
	_, err = GetSPDExtendedBaselineSentinel(spdBad)
	assert.Error(t, err)
}

func TestGetSPDBaselineSentinel_EdgeCases(t *testing.T) {
	t.Parallel()

	// no annotation -> nil, nil
	spd := &v1alpha1.ServiceProfileDescriptor{ObjectMeta: metav1.ObjectMeta{Name: "spd"}}
	got, err := GetSPDBaselineSentinel(spd)
	assert.NoError(t, err)
	assert.Nil(t, got)

	// invalid json -> error
	spdBad := &v1alpha1.ServiceProfileDescriptor{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "spd-bad",
			Annotations: map[string]string{consts.SPDAnnotationBaselineSentinelKey: "{bad"},
		},
	}
	_, err = GetSPDBaselineSentinel(spdBad)
	assert.Error(t, err)
}

func TestIsBaselinePod_CustomKeyError(t *testing.T) {
	t.Parallel()

	// percent in (0,100) with a non-nil sentinel forces GetSPDBaselinePodMeta to run;
	// an unregistered custom key makes it error, which IsBaselinePod must propagate.
	_, err := IsBaselinePod(
		metav1.ObjectMeta{Name: "test-pod", CreationTimestamp: metav1.NewTime(time.UnixMilli(0))},
		pointer.Int32(10),
		&SPDBaselinePodMeta{PodName: "sentinel"},
		CustomCompareKey("isbaseline_unregistered_key"),
	)
	assert.Error(t, err)
}
