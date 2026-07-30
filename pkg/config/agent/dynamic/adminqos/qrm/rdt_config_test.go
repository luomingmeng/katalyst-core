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

package qrm

import (
	"testing"

	configv1alpha1 "github.com/kubewharf/katalyst-api/pkg/apis/config/v1alpha1"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic/crd"
)

func TestQRMPluginConfigurationApplyRDTConfiguration(t *testing.T) {
	disableRDT := true
	enableCPUList := true
	enableCAT := true
	defaultCATWays := int64(4)

	config := NewQRMPluginConfiguration()
	config.ApplyConfiguration(&crd.DynamicConfigCRD{
		AdminQoSConfiguration: &configv1alpha1.AdminQoSConfiguration{
			Spec: configv1alpha1.AdminQoSConfigurationSpec{
				Config: configv1alpha1.AdminQoSConfig{
					QRMPluginConfig: &configv1alpha1.QRMPluginConfig{
						RDTConfig: &configv1alpha1.RDTConfig{
							DisableRDT: &disableRDT,
						},
						CPUPluginConfig: &configv1alpha1.CPUPluginConfig{
							BulkheadConfig: &configv1alpha1.BulkheadConfig{
								BulkheadRDTConfig: &configv1alpha1.BulkheadRDTConfig{
									EnableCPUList:  &enableCPUList,
									EnableCAT:      &enableCAT,
									DefaultCATWays: &defaultCATWays,
									ClosCATWays:    map[string]int64{"reclaim": 2},
								},
							},
						},
					},
				},
			},
		},
	})

	if !config.RDTConfig.DisableRDT {
		t.Fatal("DisableRDT = false, want true")
	}
	bulkheadRDT := config.CPUPluginConfiguration.BulkheadConfig.BulkheadRDTConfig
	if !bulkheadRDT.EnableCPUList || !bulkheadRDT.EnableCAT {
		t.Fatalf("bulkhead RDT enables = %#v, want CPUList and CAT enabled", bulkheadRDT)
	}
	if bulkheadRDT.DefaultCATWays != 4 {
		t.Fatalf("DefaultCATWays = %d, want 4", bulkheadRDT.DefaultCATWays)
	}
	if bulkheadRDT.ClosCATWays["reclaim"] != 2 {
		t.Fatalf("ClosCATWays[reclaim] = %d, want 2", bulkheadRDT.ClosCATWays["reclaim"])
	}
}
