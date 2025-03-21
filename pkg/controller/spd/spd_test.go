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

package spd

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	metrics "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"k8s.io/utils/pointer"

	apis "github.com/kubewharf/katalyst-api/pkg/apis/autoscaling/v1alpha1"
	apiworkload "github.com/kubewharf/katalyst-api/pkg/apis/workload/v1alpha1"
	apiconsts "github.com/kubewharf/katalyst-api/pkg/consts"
	apimetricpod "github.com/kubewharf/katalyst-api/pkg/metric/pod"
	katalystbase "github.com/kubewharf/katalyst-core/cmd/base"
	"github.com/kubewharf/katalyst-core/pkg/config/controller"
	"github.com/kubewharf/katalyst-core/pkg/config/generic"
	"github.com/kubewharf/katalyst-core/pkg/consts"
	indicator_plugin "github.com/kubewharf/katalyst-core/pkg/controller/spd/indicator-plugin"
	"github.com/kubewharf/katalyst-core/pkg/util/native"
)

var (
	stsGVK = schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "StatefulSet"}
	stsGVR = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "statefulsets"}
)

func TestSPDController_Run(t *testing.T) {
	t.Parallel()

	type fields struct {
		pod       *v1.Pod
		workload  *appsv1.StatefulSet
		spd       *apiworkload.ServiceProfileDescriptor
		spdConfig *controller.SPDConfig
	}
	tests := []struct {
		name         string
		fields       fields
		wantWorkload *appsv1.StatefulSet
		wantSPD      *apiworkload.ServiceProfileDescriptor
	}{
		{
			name: "delete unwanted spd",
			fields: fields{
				pod: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pod1",
						Namespace: "default",
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "apps/v1",
								Kind:       "StatefulSet",
								Name:       "sts1",
							},
						},
						Annotations: map[string]string{
							apiconsts.PodAnnotationSPDNameKey: "spd1",
						},
						Labels: map[string]string{
							"workload": "sts1",
						},
					},
				},
				workload: &appsv1.StatefulSet{
					TypeMeta: metav1.TypeMeta{
						Kind:       "StatefulSet",
						APIVersion: "apps/v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:        "sts1",
						Namespace:   "default",
						Annotations: map[string]string{},
					},
					Spec: appsv1.StatefulSetSpec{
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"workload": "sts1",
							},
						},
					},
				},
				spd: &apiworkload.ServiceProfileDescriptor{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      "sts1",
					},
					Spec: apiworkload.ServiceProfileDescriptorSpec{
						TargetRef: apis.CrossVersionObjectReference{
							Kind:       stsGVK.Kind,
							Name:       "sts1",
							APIVersion: stsGVK.GroupVersion().String(),
						},
					},
					Status: apiworkload.ServiceProfileDescriptorStatus{},
				},
				spdConfig: &controller.SPDConfig{
					SPDWorkloadGVResources: []string{"statefulsets.v1.apps"},
					BaselinePercent: map[string]int64{
						apiconsts.PodAnnotationQoSLevelSharedCores:    0,
						apiconsts.PodAnnotationQoSLevelDedicatedCores: 100,
					},
				},
			},
			wantWorkload: &appsv1.StatefulSet{
				TypeMeta: metav1.TypeMeta{
					Kind:       "StatefulSet",
					APIVersion: "apps/v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "sts1",
					Namespace: "default",
				},
				Spec: appsv1.StatefulSetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"workload": "sts1",
						},
					},
				},
			},
			wantSPD: nil,
		},
		{
			name: "auto create spd",
			fields: fields{
				workload: &appsv1.StatefulSet{
					TypeMeta: metav1.TypeMeta{
						Kind:       "StatefulSet",
						APIVersion: "apps/v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "sts1",
						Namespace: "default",
						Annotations: map[string]string{
							apiconsts.WorkloadAnnotationSPDEnableKey: apiconsts.WorkloadAnnotationSPDEnabled,
						},
					},
					Spec: appsv1.StatefulSetSpec{
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"workload": "sts1",
							},
						},
						Template: v1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Annotations: map[string]string{
									"katalyst.kubewharf.io/qos_level": "dedicated_cores",
								},
							},
							Spec: v1.PodSpec{},
						},
					},
				},
				spd: nil,
				spdConfig: &controller.SPDConfig{
					SPDWorkloadGVResources: []string{"statefulsets.v1.apps"},
					BaselinePercent: map[string]int64{
						apiconsts.PodAnnotationQoSLevelSharedCores:    0,
						apiconsts.PodAnnotationQoSLevelDedicatedCores: 100,
					},
				},
			},
			wantWorkload: &appsv1.StatefulSet{
				TypeMeta: metav1.TypeMeta{
					Kind:       "StatefulSet",
					APIVersion: "apps/v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "sts1",
					Namespace: "default",
					Annotations: map[string]string{
						apiconsts.WorkloadAnnotationSPDEnableKey: apiconsts.WorkloadAnnotationSPDEnabled,
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"workload": "sts1",
						},
					},
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{
								"katalyst.kubewharf.io/qos_level": "dedicated_cores",
							},
						},
						Spec: v1.PodSpec{},
					},
				},
			},
			wantSPD: &apiworkload.ServiceProfileDescriptor{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "sts1",
					Annotations: map[string]string{
						consts.ServiceProfileDescriptorAnnotationKeyConfigHash: "51131be1b092",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "apps/v1",
							Kind:       "StatefulSet",
							Name:       "sts1",
						},
					},
				},
				Spec: apiworkload.ServiceProfileDescriptorSpec{
					TargetRef: apis.CrossVersionObjectReference{
						Kind:       stsGVK.Kind,
						Name:       "sts1",
						APIVersion: stsGVK.GroupVersion().String(),
					},
					BaselinePercent: pointer.Int32(100),
				},
				Status: apiworkload.ServiceProfileDescriptorStatus{
					AggMetrics: []apiworkload.AggPodMetrics{},
				},
			},
		},
		{
			name: "auto create spd(dedicated_cores)",
			fields: fields{
				workload: &appsv1.StatefulSet{
					TypeMeta: metav1.TypeMeta{
						Kind:       "StatefulSet",
						APIVersion: "apps/v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "sts1",
						Namespace: "default",
						Annotations: map[string]string{
							apiconsts.WorkloadAnnotationSPDEnableKey: apiconsts.WorkloadAnnotationSPDEnabled,
						},
					},
					Spec: appsv1.StatefulSetSpec{
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"workload": "sts1",
							},
						},
						Template: v1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Annotations: map[string]string{"katalyst.kubewharf.io/qos_level": "dedicated_cores"},
							},
							Spec: v1.PodSpec{},
						},
					},
				},
				spd: nil,
				spdConfig: &controller.SPDConfig{
					SPDWorkloadGVResources: []string{"statefulsets.v1.apps"},
					BaselinePercent: map[string]int64{
						apiconsts.PodAnnotationQoSLevelSharedCores:    0,
						apiconsts.PodAnnotationQoSLevelDedicatedCores: 100,
					},
				},
			},
			wantWorkload: &appsv1.StatefulSet{
				TypeMeta: metav1.TypeMeta{
					Kind:       "StatefulSet",
					APIVersion: "apps/v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "sts1",
					Namespace: "default",
					Annotations: map[string]string{
						apiconsts.WorkloadAnnotationSPDEnableKey: apiconsts.WorkloadAnnotationSPDEnabled,
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"workload": "sts1",
						},
					},
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{"katalyst.kubewharf.io/qos_level": "dedicated_cores"},
						},
						Spec: v1.PodSpec{},
					},
				},
			},
			wantSPD: &apiworkload.ServiceProfileDescriptor{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "sts1",
					Annotations: map[string]string{
						consts.ServiceProfileDescriptorAnnotationKeyConfigHash: "51131be1b092",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "apps/v1",
							Kind:       "StatefulSet",
							Name:       "sts1",
						},
					},
				},
				Spec: apiworkload.ServiceProfileDescriptorSpec{
					TargetRef: apis.CrossVersionObjectReference{
						Kind:       stsGVK.Kind,
						Name:       "sts1",
						APIVersion: stsGVK.GroupVersion().String(),
					},
					BaselinePercent: pointer.Int32(100),
				},
				Status: apiworkload.ServiceProfileDescriptorStatus{
					AggMetrics: []apiworkload.AggPodMetrics{},
				},
			},
		},
		{
			name: "auto create spd(shared_cores)",
			fields: fields{
				workload: &appsv1.StatefulSet{
					TypeMeta: metav1.TypeMeta{
						Kind:       "StatefulSet",
						APIVersion: "apps/v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "sts1",
						Namespace: "default",
						Annotations: map[string]string{
							apiconsts.WorkloadAnnotationSPDEnableKey: apiconsts.WorkloadAnnotationSPDEnabled,
						},
					},
					Spec: appsv1.StatefulSetSpec{
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"workload": "sts1",
							},
						},
						Template: v1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Annotations: map[string]string{"katalyst.kubewharf.io/qos_level": "shared_cores"},
							},
							Spec: v1.PodSpec{},
						},
					},
				},
				spd: nil,
				spdConfig: &controller.SPDConfig{
					SPDWorkloadGVResources: []string{"statefulsets.v1.apps"},
					BaselinePercent: map[string]int64{
						apiconsts.PodAnnotationQoSLevelSharedCores:    0,
						apiconsts.PodAnnotationQoSLevelDedicatedCores: 100,
					},
				},
			},
			wantWorkload: &appsv1.StatefulSet{
				TypeMeta: metav1.TypeMeta{
					Kind:       "StatefulSet",
					APIVersion: "apps/v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "sts1",
					Namespace: "default",
					Annotations: map[string]string{
						apiconsts.WorkloadAnnotationSPDEnableKey: apiconsts.WorkloadAnnotationSPDEnabled,
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"workload": "sts1",
						},
					},
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{"katalyst.kubewharf.io/qos_level": "shared_cores"},
						},
						Spec: v1.PodSpec{},
					},
				},
			},
			wantSPD: &apiworkload.ServiceProfileDescriptor{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "sts1",
					Annotations: map[string]string{
						consts.ServiceProfileDescriptorAnnotationKeyConfigHash: "a62e4c90e3ed",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "apps/v1",
							Kind:       "StatefulSet",
							Name:       "sts1",
						},
					},
				},
				Spec: apiworkload.ServiceProfileDescriptorSpec{
					TargetRef: apis.CrossVersionObjectReference{
						Kind:       stsGVK.Kind,
						Name:       "sts1",
						APIVersion: stsGVK.GroupVersion().String(),
					},
					BaselinePercent: pointer.Int32(0),
				},
				Status: apiworkload.ServiceProfileDescriptorStatus{
					AggMetrics: []apiworkload.AggPodMetrics{},
				},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			spdConfig := tt.fields.spdConfig
			genericConfig := &generic.GenericConfiguration{}
			controllerConf := &controller.GenericControllerConfiguration{
				DynamicGVResources: []string{"statefulsets.v1.apps"},
			}

			ctx := context.TODO()
			controlCtx, err := katalystbase.GenerateFakeGenericContext([]runtime.Object{tt.fields.pod},
				[]runtime.Object{tt.fields.spd}, []runtime.Object{tt.fields.workload})
			assert.NoError(t, err)

			spdController, err := NewSPDController(ctx, controlCtx, genericConfig, controllerConf, spdConfig, generic.NewQoSConfiguration(), struct{}{})
			assert.NoError(t, err)

			controlCtx.StartInformer(ctx)
			go spdController.Run()
			synced := cache.WaitForCacheSync(ctx.Done(), spdController.syncedFunc...)
			assert.True(t, synced)
			time.Sleep(1 * time.Second)

			targetSPD := tt.fields.spd
			if targetSPD == nil {
				targetSPD = tt.wantSPD
			}
			newSPD, _ := controlCtx.Client.InternalClient.WorkloadV1alpha1().
				ServiceProfileDescriptors(targetSPD.Namespace).Get(ctx, targetSPD.Name, metav1.GetOptions{})
			assert.Equal(t, tt.wantSPD, newSPD)

			newObject, _ := controlCtx.Client.DynamicClient.Resource(stsGVR).
				Namespace(tt.fields.workload.GetNamespace()).Get(ctx, tt.fields.workload.GetName(), metav1.GetOptions{})

			newWorkload := &appsv1.StatefulSet{}
			err = runtime.DefaultUnstructuredConverter.FromUnstructured(newObject.UnstructuredContent(), newWorkload)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantWorkload, newWorkload)
		})
	}
}

func TestPodIndexerDuplicate(t *testing.T) {
	t.Parallel()

	spdConf := controller.NewSPDConfig()
	genericConfig := &generic.GenericConfiguration{}
	controllerConf := &controller.GenericControllerConfiguration{}
	controlCtx, err := katalystbase.GenerateFakeGenericContext(nil, nil, nil)
	assert.NoError(t, err)

	spdConf.SPDPodLabelIndexerKeys = []string{"test-1"}

	_, err = NewSPDController(context.TODO(), controlCtx, genericConfig, controllerConf, spdConf, nil, struct{}{})
	assert.NoError(t, err)

	_, err = NewSPDController(context.TODO(), controlCtx, genericConfig, controllerConf, spdConf, nil, struct{}{})
	assert.NoError(t, err)

	indexers := controlCtx.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer().GetIndexers()
	assert.Equal(t, 2, len(indexers))
	_, exist := indexers["test-1"]
	assert.Equal(t, true, exist)
}

func TestSPDController_Run_(t *testing.T) {
	t.Parallel()

	type fields struct {
		pod      *v1.Pod
		workload *appsv1.StatefulSet
		spd      *apiworkload.ServiceProfileDescriptor
	}
	tests := []struct {
		name         string
		fields       fields
		wantWorkload *appsv1.StatefulSet
		wantSPD      *apiworkload.ServiceProfileDescriptor
	}{
		{
			name: "delete unwanted spd",
			fields: fields{
				pod: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pod1",
						Namespace: "default",
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "apps/v1",
								Kind:       "StatefulSet",
								Name:       "sts1",
							},
						},
						Annotations: map[string]string{
							apiconsts.PodAnnotationSPDNameKey: "spd1",
						},
						Labels: map[string]string{
							"workload": "sts1",
						},
					},
				},
				workload: &appsv1.StatefulSet{
					TypeMeta: metav1.TypeMeta{
						Kind:       "StatefulSet",
						APIVersion: "apps/v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:        "sts1",
						Namespace:   "default",
						Annotations: map[string]string{},
					},
					Spec: appsv1.StatefulSetSpec{
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"workload": "sts1",
							},
						},
					},
				},
				spd: &apiworkload.ServiceProfileDescriptor{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      "sts1",
					},
					Spec: apiworkload.ServiceProfileDescriptorSpec{
						TargetRef: apis.CrossVersionObjectReference{
							Kind:       stsGVK.Kind,
							Name:       "sts1",
							APIVersion: stsGVK.GroupVersion().String(),
						},
					},
					Status: apiworkload.ServiceProfileDescriptorStatus{},
				},
			},
			wantWorkload: &appsv1.StatefulSet{
				TypeMeta: metav1.TypeMeta{
					Kind:       "StatefulSet",
					APIVersion: "apps/v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "sts1",
					Namespace: "default",
				},
				Spec: appsv1.StatefulSetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"workload": "sts1",
						},
					},
				},
			},
			wantSPD: nil,
		},
		{
			name: "auto create spd",
			fields: fields{
				workload: &appsv1.StatefulSet{
					TypeMeta: metav1.TypeMeta{
						Kind:       "StatefulSet",
						APIVersion: "apps/v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "sts1",
						Namespace: "default",
						Annotations: map[string]string{
							apiconsts.WorkloadAnnotationSPDEnableKey: apiconsts.WorkloadAnnotationSPDEnabled,
						},
					},
					Spec: appsv1.StatefulSetSpec{
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"workload": "sts1",
							},
						},
						Template: v1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Annotations: map[string]string{
									"katalyst.kubewharf.io/qos_level": "dedicated_cores",
								},
							},
							Spec: v1.PodSpec{},
						},
					},
				},
				spd: nil,
			},
			wantWorkload: &appsv1.StatefulSet{
				TypeMeta: metav1.TypeMeta{
					Kind:       "StatefulSet",
					APIVersion: "apps/v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "sts1",
					Namespace: "default",
					Annotations: map[string]string{
						apiconsts.WorkloadAnnotationSPDEnableKey: apiconsts.WorkloadAnnotationSPDEnabled,
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"workload": "sts1",
						},
					},
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{
								"katalyst.kubewharf.io/qos_level": "dedicated_cores",
							},
						},
						Spec: v1.PodSpec{},
					},
				},
			},
			wantSPD: &apiworkload.ServiceProfileDescriptor{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "sts1",
					Annotations: map[string]string{
						consts.ServiceProfileDescriptorAnnotationKeyConfigHash: "51131be1b092",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "apps/v1",
							Kind:       "StatefulSet",
							Name:       "sts1",
						},
					},
				},
				Spec: apiworkload.ServiceProfileDescriptorSpec{
					TargetRef: apis.CrossVersionObjectReference{
						Kind:       stsGVK.Kind,
						Name:       "sts1",
						APIVersion: stsGVK.GroupVersion().String(),
					},
					BaselinePercent: pointer.Int32(100),
				},
				Status: apiworkload.ServiceProfileDescriptorStatus{
					AggMetrics: []apiworkload.AggPodMetrics{},
				},
			},
		},
		{
			name: "auto create spd(dedicated_cores)",
			fields: fields{
				workload: &appsv1.StatefulSet{
					TypeMeta: metav1.TypeMeta{
						Kind:       "StatefulSet",
						APIVersion: "apps/v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "sts1",
						Namespace: "default",
						Annotations: map[string]string{
							apiconsts.WorkloadAnnotationSPDEnableKey: apiconsts.WorkloadAnnotationSPDEnabled,
						},
					},
					Spec: appsv1.StatefulSetSpec{
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"workload": "sts1",
							},
						},
						Template: v1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Annotations: map[string]string{"katalyst.kubewharf.io/qos_level": "dedicated_cores"},
							},
							Spec: v1.PodSpec{},
						},
					},
				},
				spd: nil,
			},
			wantWorkload: &appsv1.StatefulSet{
				TypeMeta: metav1.TypeMeta{
					Kind:       "StatefulSet",
					APIVersion: "apps/v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "sts1",
					Namespace: "default",
					Annotations: map[string]string{
						apiconsts.WorkloadAnnotationSPDEnableKey: apiconsts.WorkloadAnnotationSPDEnabled,
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"workload": "sts1",
						},
					},
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{"katalyst.kubewharf.io/qos_level": "dedicated_cores"},
						},
						Spec: v1.PodSpec{},
					},
				},
			},
			wantSPD: &apiworkload.ServiceProfileDescriptor{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "sts1",
					Annotations: map[string]string{
						consts.ServiceProfileDescriptorAnnotationKeyConfigHash: "51131be1b092",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "apps/v1",
							Kind:       "StatefulSet",
							Name:       "sts1",
						},
					},
				},
				Spec: apiworkload.ServiceProfileDescriptorSpec{
					TargetRef: apis.CrossVersionObjectReference{
						Kind:       stsGVK.Kind,
						Name:       "sts1",
						APIVersion: stsGVK.GroupVersion().String(),
					},
					BaselinePercent: pointer.Int32(100),
				},
				Status: apiworkload.ServiceProfileDescriptorStatus{
					AggMetrics: []apiworkload.AggPodMetrics{},
				},
			},
		},
		{
			name: "auto create spd(shared_cores)",
			fields: fields{
				workload: &appsv1.StatefulSet{
					TypeMeta: metav1.TypeMeta{
						Kind:       "StatefulSet",
						APIVersion: "apps/v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "sts1",
						Namespace: "default",
						Annotations: map[string]string{
							apiconsts.WorkloadAnnotationSPDEnableKey: apiconsts.WorkloadAnnotationSPDEnabled,
						},
					},
					Spec: appsv1.StatefulSetSpec{
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"workload": "sts1",
							},
						},
						Template: v1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Annotations: map[string]string{"katalyst.kubewharf.io/qos_level": "shared_cores"},
							},
							Spec: v1.PodSpec{},
						},
					},
				},
				spd: nil,
			},
			wantWorkload: &appsv1.StatefulSet{
				TypeMeta: metav1.TypeMeta{
					Kind:       "StatefulSet",
					APIVersion: "apps/v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "sts1",
					Namespace: "default",
					Annotations: map[string]string{
						apiconsts.WorkloadAnnotationSPDEnableKey: apiconsts.WorkloadAnnotationSPDEnabled,
					},
				},
				Spec: appsv1.StatefulSetSpec{
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"workload": "sts1",
						},
					},
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{"katalyst.kubewharf.io/qos_level": "shared_cores"},
						},
						Spec: v1.PodSpec{},
					},
				},
			},
			wantSPD: &apiworkload.ServiceProfileDescriptor{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "sts1",
					Annotations: map[string]string{
						consts.ServiceProfileDescriptorAnnotationKeyConfigHash: "a62e4c90e3ed",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "apps/v1",
							Kind:       "StatefulSet",
							Name:       "sts1",
						},
					},
				},
				Spec: apiworkload.ServiceProfileDescriptorSpec{
					TargetRef: apis.CrossVersionObjectReference{
						Kind:       stsGVK.Kind,
						Name:       "sts1",
						APIVersion: stsGVK.GroupVersion().String(),
					},
					BaselinePercent: pointer.Int32(0),
				},
				Status: apiworkload.ServiceProfileDescriptorStatus{
					AggMetrics: []apiworkload.AggPodMetrics{},
				},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			spdConfig := &controller.SPDConfig{
				SPDWorkloadGVResources: []string{"statefulsets.v1.apps"},
				BaselinePercent: map[string]int64{
					apiconsts.PodAnnotationQoSLevelSharedCores:    0,
					apiconsts.PodAnnotationQoSLevelDedicatedCores: 100,
				},
			}
			genericConfig := &generic.GenericConfiguration{}
			controllerConf := &controller.GenericControllerConfiguration{
				DynamicGVResources: []string{"statefulsets.v1.apps"},
			}

			ctx := context.TODO()
			controlCtx, err := katalystbase.GenerateFakeGenericContext([]runtime.Object{tt.fields.pod},
				[]runtime.Object{tt.fields.spd}, []runtime.Object{tt.fields.workload})
			assert.NoError(t, err)

			spdController, err := NewSPDController(ctx, controlCtx, genericConfig, controllerConf, spdConfig, generic.NewQoSConfiguration(), struct{}{})
			assert.NoError(t, err)

			controlCtx.StartInformer(ctx)
			go spdController.Run()
			synced := cache.WaitForCacheSync(ctx.Done(), spdController.syncedFunc...)
			assert.True(t, synced)
			time.Sleep(1 * time.Second)

			targetSPD := tt.fields.spd
			if targetSPD == nil {
				targetSPD = tt.wantSPD
			}
			newSPD, _ := controlCtx.Client.InternalClient.WorkloadV1alpha1().
				ServiceProfileDescriptors(targetSPD.Namespace).Get(ctx, targetSPD.Name, metav1.GetOptions{})
			assert.Equal(t, tt.wantSPD, newSPD)

			newObject, _ := controlCtx.Client.DynamicClient.Resource(stsGVR).
				Namespace(tt.fields.workload.GetNamespace()).Get(ctx, tt.fields.workload.GetName(), metav1.GetOptions{})

			newWorkload := &appsv1.StatefulSet{}
			err = runtime.DefaultUnstructuredConverter.FromUnstructured(newObject.UnstructuredContent(), newWorkload)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantWorkload, newWorkload)
		})
	}
}

func TestIndicatorUpdater(t *testing.T) {
	t.Parallel()

	var current float32 = 8.3
	var value float32 = 23.1

	workload := &appsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{
			Kind:       "StatefulSet",
			APIVersion: "apps/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sts1",
			Namespace: "default",
			Annotations: map[string]string{
				apiconsts.WorkloadAnnotationSPDEnableKey: apiconsts.WorkloadAnnotationSPDEnabled,
			},
		},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"workload": "sts1",
				},
			},
		},
	}

	spd := &apiworkload.ServiceProfileDescriptor{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "default",
			Name:            "spd1",
			ResourceVersion: "1",
		},
		Spec: apiworkload.ServiceProfileDescriptorSpec{
			TargetRef: apis.CrossVersionObjectReference{
				Kind:       stsGVK.Kind,
				Name:       "sts1",
				APIVersion: stsGVK.GroupVersion().String(),
			},
			BusinessIndicator: []apiworkload.ServiceBusinessIndicatorSpec{
				{
					Name: "none-exist-b",
					Indicators: []apiworkload.Indicator{
						{
							IndicatorLevel: apiworkload.IndicatorLevelLowerBound,
							Value:          10.2,
						},
					},
				},
			},
			SystemIndicator: []apiworkload.ServiceSystemIndicatorSpec{
				{
					Name: "none-exist-s",
					Indicators: []apiworkload.Indicator{
						{
							IndicatorLevel: apiworkload.IndicatorLevelUpperBound,
							Value:          10.5,
						},
					},
				},
				{
					Name: "system-3",
					Indicators: []apiworkload.Indicator{
						{
							IndicatorLevel: apiworkload.IndicatorLevelUpperBound,
							Value:          4.5,
						},
					},
				},
			},
		},
		Status: apiworkload.ServiceProfileDescriptorStatus{
			BusinessStatus: []apiworkload.ServiceBusinessIndicatorStatus{
				{
					Name:    "none-exist-status",
					Current: &current,
				},
				{
					Name:    "business-2",
					Current: &current,
				},
			},
		},
	}

	expectedSpd := &apiworkload.ServiceProfileDescriptor{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "spd1",
		},
		Spec: apiworkload.ServiceProfileDescriptorSpec{
			TargetRef: apis.CrossVersionObjectReference{
				Kind:       stsGVK.Kind,
				Name:       "sts1",
				APIVersion: stsGVK.GroupVersion().String(),
			},
			BaselinePercent: pointer.Int32(20),
			ExtendedIndicator: []apiworkload.ServiceExtendedIndicatorSpec{
				{
					Name: "TestExtended",
					Indicators: runtime.RawExtension{
						Raw: func() []byte {
							data, _ := json.Marshal(&apiworkload.TestExtendedIndicators{
								Indicators: &apiworkload.TestIndicators{},
							})
							return data
						}(),
					},
				},
			},
			BusinessIndicator: []apiworkload.ServiceBusinessIndicatorSpec{
				{
					Name: "business-1",
					Indicators: []apiworkload.Indicator{
						{
							IndicatorLevel: apiworkload.IndicatorLevelLowerBound,
							Value:          10.2,
						},
					},
				},
				{
					Name: "business-2",
					Indicators: []apiworkload.Indicator{
						{
							IndicatorLevel: apiworkload.IndicatorLevelUpperBound,
							Value:          18.3,
						},
					},
				},
				{
					Name: "business-3",
					Indicators: []apiworkload.Indicator{
						{
							IndicatorLevel: apiworkload.IndicatorLevelUpperBound,
							Value:          16.8,
						},
					},
				},
			},
			SystemIndicator: []apiworkload.ServiceSystemIndicatorSpec{
				{
					Name: "system-3",
					Indicators: []apiworkload.Indicator{
						{
							IndicatorLevel: apiworkload.IndicatorLevelUpperBound,
							Value:          4.5,
						},
					},
				},
				{
					Name: "system-1",
					Indicators: []apiworkload.Indicator{
						{
							IndicatorLevel: apiworkload.IndicatorLevelLowerBound,
							Value:          10.5,
						},
						{
							IndicatorLevel: apiworkload.IndicatorLevelUpperBound,
							Value:          10.5,
						},
					},
				},
				{
					Name: "system-2",
					Indicators: []apiworkload.Indicator{
						{
							IndicatorLevel: apiworkload.IndicatorLevelUpperBound,
							Value:          10.5,
						},
					},
				},
			},
		},
		Status: apiworkload.ServiceProfileDescriptorStatus{
			BusinessStatus: []apiworkload.ServiceBusinessIndicatorStatus{
				{
					Name:    "business-2",
					Current: &value,
				},
			},
			AggMetrics: []apiworkload.AggPodMetrics{
				{
					Aggregator: apiworkload.Avg,
					Items: []metrics.PodMetrics{
						{
							Timestamp: metav1.NewTime(time.Date(2022, 1, 1, 1, 0, 0, 0, time.Local)),
							Window:    metav1.Duration{Duration: time.Hour},
							Containers: []metrics.ContainerMetrics{
								{
									Name: "c1",
									Usage: map[v1.ResourceName]resource.Quantity{
										apimetricpod.CustomMetricPodCPULoad1Min: resource.MustParse("20"),
									},
								},
							},
						},
					},
				},
				{
					Aggregator: apiworkload.Sum,
				},
			},
		},
	}

	nn := types.NamespacedName{
		Namespace: "default",
		Name:      "spd1",
	}

	d1 := indicator_plugin.DummyIndicatorPlugin{
		ExtendedSpecNames: []string{
			"TestExtended",
		},
		SystemSpecNames: []apiworkload.ServiceSystemIndicatorName{
			"system-1",
		},
		BusinessSpecNames: []apiworkload.ServiceBusinessIndicatorName{
			"business-1",
			"business-2",
		},
		BusinessStatusNames: []apiworkload.ServiceBusinessIndicatorName{
			"business-2",
		},
	}
	d2 := indicator_plugin.DummyIndicatorPlugin{
		SystemSpecNames: []apiworkload.ServiceSystemIndicatorName{
			"system-2",
			"system-3",
		},
		BusinessSpecNames: []apiworkload.ServiceBusinessIndicatorName{
			"business-3",
		},
		BusinessStatusNames: []apiworkload.ServiceBusinessIndicatorName{
			"business-3",
		},
	}

	indicator_plugin.RegisterPluginInitializer("d1", func(_ context.Context, _ *controller.SPDConfig,
		_ interface{}, _ map[schema.GroupVersionResource]native.DynamicInformer, _ *katalystbase.GenericContext,
		_ indicator_plugin.IndicatorUpdater,
	) (indicator_plugin.IndicatorPlugin, error) {
		return d1, nil
	})
	indicator_plugin.RegisterPluginInitializer("d2", func(_ context.Context, _ *controller.SPDConfig,
		_ interface{}, _ map[schema.GroupVersionResource]native.DynamicInformer, _ *katalystbase.GenericContext,
		_ indicator_plugin.IndicatorUpdater,
	) (indicator_plugin.IndicatorPlugin, error) {
		return d2, nil
	})

	spdConfig := &controller.SPDConfig{
		SPDWorkloadGVResources: []string{"statefulsets.v1.apps"},
		IndicatorPlugins:       []string{"d1", "d2"},
	}
	genericConfig := &generic.GenericConfiguration{}
	controllerConf := &controller.GenericControllerConfiguration{
		DynamicGVResources: []string{"statefulsets.v1.apps"},
	}

	ctx := context.TODO()
	controlCtx, err := katalystbase.GenerateFakeGenericContext([]runtime.Object{},
		[]runtime.Object{spd}, []runtime.Object{workload})
	assert.NoError(t, err)

	sc, err := NewSPDController(ctx, controlCtx, genericConfig, controllerConf, spdConfig, nil, struct{}{})
	assert.NoError(t, err)

	controlCtx.StartInformer(ctx)
	go sc.Run()
	synced := cache.WaitForCacheSync(ctx.Done(), sc.syncedFunc...)
	assert.True(t, synced)

	sc.indicatorManager.UpdateExtendedIndicatorSpec(nn, []apiworkload.ServiceExtendedIndicatorSpec{
		{
			Name: "TestExtended",
			Indicators: runtime.RawExtension{
				Object: &apiworkload.TestExtendedIndicators{
					Indicators: &apiworkload.TestIndicators{},
				},
			},
		},
	})

	sc.indicatorManager.UpdateBusinessIndicatorSpec(nn, []apiworkload.ServiceBusinessIndicatorSpec{
		{
			Name: "business-1",
			Indicators: []apiworkload.Indicator{
				{
					IndicatorLevel: apiworkload.IndicatorLevelLowerBound,
					Value:          10.2,
				},
			},
		},
		{
			Name: "business-2",
			Indicators: []apiworkload.Indicator{
				{
					IndicatorLevel: apiworkload.IndicatorLevelUpperBound,
					Value:          18.3,
				},
			},
		},
	})
	sc.indicatorManager.UpdateBusinessIndicatorSpec(nn, []apiworkload.ServiceBusinessIndicatorSpec{
		{
			Name: "business-3",
			Indicators: []apiworkload.Indicator{
				{
					IndicatorLevel: apiworkload.IndicatorLevelUpperBound,
					Value:          13.3,
				},
			},
		},
		{
			Name: "business-3",
			Indicators: []apiworkload.Indicator{
				{
					IndicatorLevel: apiworkload.IndicatorLevelUpperBound,
					Value:          16.8,
				},
			},
		},
	})

	sc.indicatorManager.UpdateSystemIndicatorSpec(nn, []apiworkload.ServiceSystemIndicatorSpec{
		{
			Name: "system-1",
			Indicators: []apiworkload.Indicator{
				{
					IndicatorLevel: apiworkload.IndicatorLevelLowerBound,
					Value:          10.5,
				},
				{
					IndicatorLevel: apiworkload.IndicatorLevelUpperBound,
					Value:          10.5,
				},
			},
		},
	})
	sc.indicatorManager.UpdateSystemIndicatorSpec(nn, []apiworkload.ServiceSystemIndicatorSpec{
		{
			Name: "system-2",
			Indicators: []apiworkload.Indicator{
				{
					IndicatorLevel: apiworkload.IndicatorLevelUpperBound,
					Value:          10.5,
				},
			},
		},
	})

	sc.indicatorManager.UpdateBusinessIndicatorStatus(
		nn, []apiworkload.ServiceBusinessIndicatorStatus{
			{
				Name:    "business-1",
				Current: &value,
			},
			{
				Name:    "business-2",
				Current: &value,
			},
		},
	)
	sc.indicatorManager.UpdateAggMetrics(nn, []apiworkload.AggPodMetrics{
		{
			Aggregator: apiworkload.Avg,
			Items: []metrics.PodMetrics{
				{
					Timestamp: metav1.NewTime(time.Date(2022, 1, 1, 1, 0, 0, 0, time.Local)),
					Window:    metav1.Duration{Duration: time.Hour},
					Containers: []metrics.ContainerMetrics{
						{
							Name: "c1",
							Usage: map[v1.ResourceName]resource.Quantity{
								apimetricpod.CustomMetricPodCPULoad1Min: resource.MustParse("20"),
							},
						},
					},
				},
			},
		},
		{
			Aggregator: apiworkload.Sum,
		},
	})
	time.Sleep(time.Second)
	newSPD, err := controlCtx.Client.InternalClient.WorkloadV1alpha1().
		ServiceProfileDescriptors("default").Get(ctx, "spd1", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, expectedSpd.Spec.ExtendedIndicator, newSPD.Spec.ExtendedIndicator)
	assert.Equal(t, expectedSpd.Spec.BusinessIndicator, newSPD.Spec.BusinessIndicator)
	assert.Equal(t, expectedSpd.Spec.SystemIndicator, newSPD.Spec.SystemIndicator)
	assert.Equal(t, expectedSpd.Status.BusinessStatus, newSPD.Status.BusinessStatus)
	assert.Equal(t, expectedSpd.Status.AggMetrics, newSPD.Status.AggMetrics)
}
