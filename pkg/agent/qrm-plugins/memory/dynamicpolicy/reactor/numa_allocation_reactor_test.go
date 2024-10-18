package reactor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"

	"github.com/kubewharf/katalyst-api/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	cpuconsts "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/memory/dynamicpolicy/state"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

func Test_numaAllocationReactor_UpdateAllocation(t *testing.T) {
	type fields struct {
		podFetcher pod.PodFetcher
		client     kubernetes.Interface
	}
	type args struct {
		allocation *state.AllocationInfo
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantPod *v1.Pod
		wantErr bool
	}{
		{
			name: "actual_numa_binding_pod",
			fields: fields{
				podFetcher: &pod.PodFetcherStub{
					PodList: []*v1.Pod{
						{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "test-1",
								Namespace: "test",
								UID:       "test-1-uid",
							},
						},
					},
				},
				client: fake.NewSimpleClientset(
					&v1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-1",
							Namespace: "test",
							UID:       "test-1-uid",
						},
					},
				),
			},
			args: args{
				allocation: &state.AllocationInfo{
					AllocationMeta: commonstate.AllocationMeta{
						PodUid:         "test-1-uid",
						PodNamespace:   "test",
						PodName:        "test-1",
						ContainerName:  "container-1",
						ContainerType:  pluginapi.ContainerType_MAIN.String(),
						ContainerIndex: 0,
						QoSLevel:       consts.PodAnnotationQoSLevelSharedCores,
						Annotations: map[string]string{
							consts.PodAnnotationQoSLevelKey:                  consts.PodAnnotationQoSLevelSharedCores,
							consts.PodAnnotationMemoryEnhancementNumaBinding: consts.PodAnnotationMemoryEnhancementNumaBindingEnable,
							cpuconsts.CPUStateAnnotationKeyNUMAHint:          "0",
						},
						Labels: map[string]string{
							consts.PodAnnotationQoSLevelKey: consts.PodAnnotationQoSLevelSharedCores,
						},
					},
					AggregatedQuantity:   7516192768,
					NumaAllocationResult: machine.NewCPUSet(0),
					TopologyAwareAllocations: map[int]uint64{
						0: 7516192768,
					},
				},
			},
			wantPod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-1",
					Namespace: "test",
					UID:       types.UID("test-1-uid"),
					Annotations: map[string]string{
						consts.PodAnnotationNUMABindResultKey: "0",
					},
				},
			},
		},
		{
			name: "non-actual_numa_binding_pod",
			fields: fields{
				podFetcher: &pod.PodFetcherStub{
					PodList: []*v1.Pod{
						{
							ObjectMeta: metav1.ObjectMeta{
								Name:      "test-1",
								Namespace: "test",
								UID:       "test-1-uid",
							},
						},
					},
				},
				client: fake.NewSimpleClientset(
					&v1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-1",
							Namespace: "test",
							UID:       "test-1-uid",
						},
					},
				),
			},
			args: args{
				allocation: &state.AllocationInfo{
					AllocationMeta: commonstate.AllocationMeta{
						PodUid:         "test-1-uid",
						PodNamespace:   "test",
						PodName:        "test-1",
						ContainerName:  "container-1",
						ContainerType:  pluginapi.ContainerType_MAIN.String(),
						ContainerIndex: 0,
						QoSLevel:       consts.PodAnnotationQoSLevelSharedCores,
						Annotations: map[string]string{
							consts.PodAnnotationQoSLevelKey:                  consts.PodAnnotationQoSLevelSharedCores,
							consts.PodAnnotationMemoryEnhancementNumaBinding: consts.PodAnnotationMemoryEnhancementNumaBindingEnable,
						},
						Labels: map[string]string{
							consts.PodAnnotationQoSLevelKey: consts.PodAnnotationQoSLevelSharedCores,
						},
					},
					AggregatedQuantity:   7516192768,
					NumaAllocationResult: machine.NewCPUSet(0),
					TopologyAwareAllocations: map[int]uint64{
						0: 7516192768,
					},
				},
			},
			wantPod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-1",
					Namespace: "test",
					UID:       types.UID("test-1-uid"),
					Annotations: map[string]string{
						consts.PodAnnotationNUMABindResultKey: "-1",
					},
				},
			},
		},
		{
			name: "actual_numa_binding_pod_fallback_to_api-server",
			fields: fields{
				podFetcher: &pod.PodFetcherStub{
					PodList: []*v1.Pod{},
				},
				client: fake.NewSimpleClientset(
					&v1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-1",
							Namespace: "test",
							UID:       "test-1-uid",
						},
					},
				),
			},
			args: args{
				allocation: &state.AllocationInfo{
					AllocationMeta: commonstate.AllocationMeta{
						PodUid:         "test-1-uid",
						PodNamespace:   "test",
						PodName:        "test-1",
						ContainerName:  "container-1",
						ContainerType:  pluginapi.ContainerType_MAIN.String(),
						ContainerIndex: 0,
						QoSLevel:       consts.PodAnnotationQoSLevelSharedCores,
						Annotations: map[string]string{
							consts.PodAnnotationQoSLevelKey:                  consts.PodAnnotationQoSLevelSharedCores,
							consts.PodAnnotationMemoryEnhancementNumaBinding: consts.PodAnnotationMemoryEnhancementNumaBindingEnable,
							cpuconsts.CPUStateAnnotationKeyNUMAHint:          "0",
						},
						Labels: map[string]string{
							consts.PodAnnotationQoSLevelKey: consts.PodAnnotationQoSLevelSharedCores,
						},
					},
					AggregatedQuantity:   7516192768,
					NumaAllocationResult: machine.NewCPUSet(0),
					TopologyAwareAllocations: map[int]uint64{
						0: 7516192768,
					},
				},
			},
			wantPod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-1",
					Namespace: "test",
					UID:       types.UID("test-1-uid"),
					Annotations: map[string]string{
						consts.PodAnnotationNUMABindResultKey: "0",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewNUMAAllocationReactor(tt.fields.podFetcher, tt.fields.client)
			if err := r.UpdateAllocation(context.Background(), tt.args.allocation); (err != nil) != tt.wantErr {
				t.Errorf("UpdateAllocation() error = %v, wantErr %v", err, tt.wantErr)
			}

			getPod, err := tt.fields.client.CoreV1().Pods(tt.args.allocation.PodNamespace).Get(context.Background(), tt.args.allocation.PodName, metav1.GetOptions{})
			if err != nil {
				t.Errorf("GetPod() error = %v, wantErr %v", err, tt.wantErr)
			} else {
				assert.Equal(t, tt.wantPod, getPod)
			}
		})
	}
}
