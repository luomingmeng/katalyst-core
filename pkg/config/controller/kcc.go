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

package controller

import (
	"k8s.io/apimachinery/pkg/util/sets"
)

type KCCConfig struct {
	// ValidAPIGroupSet indicates the api-groups that kcc allows.
	ValidAPIGroupSet sets.String
	// DefaultGVRs indicates the gvr that need to watch by default.
	// value is gvr string, e.g. "nodeprofiledescriptors.v1alpha1.node.katalyst.kubewharf.io"
	DefaultGVRs []string
}

func NewKCCConfig() *KCCConfig {
	return &KCCConfig{}
}
