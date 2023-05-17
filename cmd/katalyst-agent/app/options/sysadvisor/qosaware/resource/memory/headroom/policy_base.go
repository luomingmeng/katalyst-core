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

package headroom

import (
	"github.com/kubewharf/katalyst-core/pkg/config/agent/sysadvisor/qosaware/resource/memory/headroom"
	"github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/util/errors"
)

type MemoryHeadroomPolicyOptions struct {
	MemoryPolicyCanonicalOptions *MemoryPolicyCanonicalOptions
}

func NewMemoryHeadroomPolicyOptions() *MemoryHeadroomPolicyOptions {
	return &MemoryHeadroomPolicyOptions{
		MemoryPolicyCanonicalOptions: NewMemoryPolicyCanonicalOptions(),
	}
}

func (o *MemoryHeadroomPolicyOptions) AddFlags(fs *pflag.FlagSet) {
	o.MemoryPolicyCanonicalOptions.AddFlags(fs)
}

func (o *MemoryHeadroomPolicyOptions) ApplyTo(c *headroom.MemoryHeadroomPolicyConfiguration) error {
	var errList []error
	errList = append(errList, o.MemoryPolicyCanonicalOptions.ApplyTo(c.MemoryPolicyCanonicalConfiguration))
	return errors.NewAggregate(errList)
}
