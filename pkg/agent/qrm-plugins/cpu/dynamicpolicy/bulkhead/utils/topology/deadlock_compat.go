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

package topology

import (
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/deadlock"
)

// This file is a compatibility shim: the immutable deadlock result types have
// been extracted into topology/deadlock so that the probe outcome can flow
// back into the parent package without creating an import cycle (the subpackage
// depends only on topology/model, never on this parent package). Type aliases
// keep the historical unqualified names available here and for external callers.
// New code should import topology/deadlock directly.

// ProbeCompleteness reports whether the probe finished exhaustively or early.
type ProbeCompleteness = deadlock.ProbeCompleteness

// DrainAtom is one ordered, atomic CPU transfer between ownership domains.
type DrainAtom = deadlock.DrainAtom

// DrainAtomClass labels why a drain atom exists.
type DrainAtomClass = deadlock.DrainAtomClass

// DeadlockAnalysis is the immutable outcome of a v1 drain deadlock probe.
type DeadlockAnalysis = deadlock.DeadlockAnalysis

// DeadlockProbeStats records probe accounting for logging and budget diagnosis.
type DeadlockProbeStats = deadlock.DeadlockProbeStats

// StructuralV1NonEmptyDeadlock is returned when a structural v1 non-empty
// deadlock cannot be drained safely.
type StructuralV1NonEmptyDeadlock = deadlock.StructuralV1NonEmptyDeadlock

var (
	// ErrIncompleteRequiredCoreReleaseWitness records a required-core release
	// that could not be witnessed before the probe budget was exhausted.
	ErrIncompleteRequiredCoreReleaseWitness = deadlock.ErrIncompleteRequiredCoreReleaseWitness
)

const (
	// ProbeComplete / ProbeIndeterminate label probe completeness.
	ProbeComplete      = deadlock.ProbeComplete
	ProbeIndeterminate = deadlock.ProbeIndeterminate

	DrainAtomClassV1Empty    = deadlock.DrainAtomClassV1Empty
	DrainAtomClassProtected  = deadlock.DrainAtomClassProtected
	DrainAtomClassReleasable = deadlock.DrainAtomClassReleasable
	DrainAtomClassHeld       = deadlock.DrainAtomClassHeld

	// defaultDeadlockProbeBudget bounds canonical drain-atom projections when no
	// explicit budget and no tracker are supplied.
	defaultDeadlockProbeBudget = deadlock.DefaultDeadlockProbeBudget
)
