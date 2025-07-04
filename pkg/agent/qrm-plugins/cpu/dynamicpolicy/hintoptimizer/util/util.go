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

	pkgerrors "github.com/pkg/errors"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/hintoptimizer"
)

var ErrHintOptimizerSkip = pkgerrors.New("hint optimizer skip")

func IsSkipOptimizeHintsError(err error) bool {
	return pkgerrors.Is(err, ErrHintOptimizerSkip)
}

func GetSingleNUMATopologyHintNUMANodes(hints []*pluginapi.TopologyHint) ([]int, error) {
	numaNodes := make([]int, len(hints))
	for i, hint := range hints {
		if len(hint.Nodes) != 1 {
			return nil, fmt.Errorf("hint %d has invalid node count", i)
		}
		numaNodes[i] = int(hint.Nodes[0])
	}
	return numaNodes, nil
}

func GenericOptimizeHintsCheck(
	request hintoptimizer.Request,
	hints *pluginapi.ListOfTopologyHints,
) error {
	if request.ResourceRequest == nil {
		return fmt.Errorf("OptimizeHints got nil req")
	}

	if hints == nil {
		return fmt.Errorf("hints cannot be nil")
	}

	return nil
}
