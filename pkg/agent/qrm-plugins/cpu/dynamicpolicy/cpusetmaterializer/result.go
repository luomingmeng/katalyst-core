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

package cpusetmaterializer

import (
	"errors"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

var ErrCPUSetNotConverged = errors.New("cpuset materialization not converged")

type Result struct {
	Converged bool
	Evidence  Evidence
}

type Evidence struct {
	Executed          bool
	ControlledRels    map[string]RelEvidence
	PendingProtection machine.CPUSet
	FailureReason     string
}

type RelEvidence struct {
	Target   machine.CPUSet
	Observed machine.CPUSet
	Reason   string
}

func (e Evidence) Clone() Evidence {
	return Evidence{
		Executed:          e.Executed,
		ControlledRels:    cloneRelEvidence(e.ControlledRels),
		PendingProtection: cloneCPUSet(e.PendingProtection),
		FailureReason:     e.FailureReason,
	}
}

func cloneRelEvidence(in map[string]RelEvidence) map[string]RelEvidence {
	if in == nil {
		return nil
	}

	out := make(map[string]RelEvidence, len(in))
	for rel, evidence := range in {
		out[rel] = RelEvidence{
			Target:   cloneCPUSet(evidence.Target),
			Observed: cloneCPUSet(evidence.Observed),
			Reason:   evidence.Reason,
		}
	}
	return out
}
