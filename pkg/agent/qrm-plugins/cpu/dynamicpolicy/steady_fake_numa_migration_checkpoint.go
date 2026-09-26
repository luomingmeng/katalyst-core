/*
Copyright 2026 The Katalyst Authors.

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

package dynamicpolicy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	checkpointutils "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

const (
	steadyFakeNUMAMigrationCheckpointName    = "cpu_steady_fake_numa_migration_target"
	steadyFakeNUMAMigrationCheckpointVersion = 1
)

type steadyFakeNUMAMigrationTarget struct {
	constraintDigest string
	target           machine.CPUSet
}

type steadyFakeNUMAMigrationCheckpointTransitionKind uint8

const (
	steadyFakeNUMAMigrationCheckpointKeep steadyFakeNUMAMigrationCheckpointTransitionKind = iota
	steadyFakeNUMAMigrationCheckpointReplace
	steadyFakeNUMAMigrationCheckpointRemove
)

type steadyFakeNUMAMigrationCheckpointTransition struct {
	kind   steadyFakeNUMAMigrationCheckpointTransitionKind
	target *steadyFakeNUMAMigrationTarget
}

func (kind steadyFakeNUMAMigrationCheckpointTransitionKind) String() string {
	switch kind {
	case steadyFakeNUMAMigrationCheckpointKeep:
		return "keep"
	case steadyFakeNUMAMigrationCheckpointReplace:
		return "replace"
	case steadyFakeNUMAMigrationCheckpointRemove:
		return "remove"
	default:
		return fmt.Sprintf("unknown(%d)", kind)
	}
}

func cloneSteadyFakeNUMAMigrationCheckpointTransition(
	transition steadyFakeNUMAMigrationCheckpointTransition,
) steadyFakeNUMAMigrationCheckpointTransition {
	cloned := steadyFakeNUMAMigrationCheckpointTransition{kind: transition.kind}
	if transition.target != nil {
		cloned.target = &steadyFakeNUMAMigrationTarget{
			constraintDigest: transition.target.constraintDigest,
			target:           transition.target.target.Clone(),
		}
	}
	return cloned
}

type steadyFakeNUMAMigrationCheckpoint struct {
	Version          int    `json:"version"`
	ConstraintDigest string `json:"constraint_digest"`
	TargetCPUs       []int  `json:"target_cpus"`
	Checksum         string `json:"checksum"`
}

func (p *DynamicPolicy) steadyFakeNUMAMigrationCheckpointPath() string {
	if p.advisorPostCommitCheckpointDir == "" {
		return ""
	}
	return filepath.Join(p.advisorPostCommitCheckpointDir, steadyFakeNUMAMigrationCheckpointName)
}

func steadyFakeNUMAMigrationCheckpointChecksum(
	version int,
	digest string,
	targetCPUs []int,
) string {
	hash := sha256.New()
	_, _ = fmt.Fprintf(hash, "%d\n%s\n", version, digest)
	for _, cpu := range targetCPUs {
		_, _ = fmt.Fprintf(hash, "%d,", cpu)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// steadyFakeNUMAMigrationCheckpointCodec adapts the on-disk steady fake-NUMA
// migration target to the generic checkpointutils.CheckpointCodec contract. It
// keeps the exact historical JSON schema and sha256 checksum digest so
// checkpoints written by the legacy writer remain readable (and vice versa) after
// the hand-rolled file I/O was replaced by the shared FileCheckpointStore.
type steadyFakeNUMAMigrationCheckpointCodec struct {
	version          int
	constraintDigest string
	targetCPUs       []int
	checksum         string

	// topology is transient: it is never serialized, but it is required to
	// re-validate domain bounds (CPUs within topology, whole-core alignment) on
	// Unmarshal, exactly as the legacy restore did.
	topology *machine.CPUTopology
}

func (c *steadyFakeNUMAMigrationCheckpointCodec) Marshal() ([]byte, error) {
	c.version = steadyFakeNUMAMigrationCheckpointVersion
	c.checksum = steadyFakeNUMAMigrationCheckpointChecksum(
		c.version, c.constraintDigest, c.targetCPUs)
	record := steadyFakeNUMAMigrationCheckpoint{
		Version:          c.version,
		ConstraintDigest: c.constraintDigest,
		TargetCPUs:       c.targetCPUs,
		Checksum:         c.checksum,
	}
	data, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("marshal steady fake-NUMA migration checkpoint: %w", err)
	}
	return data, nil
}

func (c *steadyFakeNUMAMigrationCheckpointCodec) Unmarshal(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var record steadyFakeNUMAMigrationCheckpoint
	if err := decoder.Decode(&record); err != nil {
		return fmt.Errorf("decode checkpoint: %w", err)
	}
	if err := ensureSteadyFakeNUMACheckpointEOF(decoder); err != nil {
		return err
	}
	if record.Version != steadyFakeNUMAMigrationCheckpointVersion {
		return fmt.Errorf("unsupported checkpoint version %d", record.Version)
	}
	if record.ConstraintDigest == "" {
		return fmt.Errorf("checkpoint constraint digest is empty")
	}
	if record.Checksum != steadyFakeNUMAMigrationCheckpointChecksum(
		record.Version, record.ConstraintDigest, record.TargetCPUs) {
		return fmt.Errorf("checkpoint checksum mismatch")
	}
	target := machine.NewCPUSet(record.TargetCPUs...)
	if target.Size() != len(record.TargetCPUs) {
		return fmt.Errorf("checkpoint target contains duplicate CPUs")
	}
	if c.topology == nil || c.topology.CPUDetails == nil {
		return fmt.Errorf("checkpoint validation requires CPU topology")
	}
	if outside := target.Difference(c.topology.CPUDetails.CPUs()); !outside.IsEmpty() {
		return fmt.Errorf("checkpoint target contains CPUs outside topology: %s", outside.String())
	}
	if err := assertCoreAligned(target, c.topology); err != nil {
		return fmt.Errorf("checkpoint target is not core aligned: %w", err)
	}
	c.version = record.Version
	c.constraintDigest = record.ConstraintDigest
	c.targetCPUs = record.TargetCPUs
	c.checksum = record.Checksum
	return nil
}

func (c *steadyFakeNUMAMigrationCheckpointCodec) New() checkpointutils.CheckpointCodec {
	return &steadyFakeNUMAMigrationCheckpointCodec{topology: c.topology}
}

// steadyFakeNUMACheckpointStore returns the CheckpointStore backing this
// checkpoint. It shares the advisor post-commit checkpoint directory, matching
// the historical file location.
func (p *DynamicPolicy) steadyFakeNUMACheckpointStore() checkpointutils.CheckpointStore {
	return checkpointutils.NewFileCheckpointStore(p.advisorPostCommitCheckpointDir)
}

func (p *DynamicPolicy) steadyFakeNUMACheckpointTopology() *machine.CPUTopology {
	if p.machine.machineInfo == nil {
		return nil
	}
	return p.machine.machineInfo.CPUTopology
}

func (p *DynamicPolicy) storeSteadyFakeNUMAMigrationTarget(
	target *steadyFakeNUMAMigrationTarget,
) error {
	if target == nil || target.constraintDigest == "" {
		return fmt.Errorf("invalid empty steady fake-NUMA migration target")
	}
	codec := &steadyFakeNUMAMigrationCheckpointCodec{
		constraintDigest: target.constraintDigest,
		targetCPUs:       target.target.ToSliceInt(),
		topology:         p.steadyFakeNUMACheckpointTopology(),
	}
	if err := p.steadyFakeNUMACheckpointStore().Store(
		steadyFakeNUMAMigrationCheckpointName, codec); err != nil {
		return err
	}
	p.steadyFakeNUMAMigrationTarget = &steadyFakeNUMAMigrationTarget{
		constraintDigest: target.constraintDigest,
		target:           target.target.Clone(),
	}
	return nil
}

func (p *DynamicPolicy) restoreSteadyFakeNUMAMigrationTarget() error {
	if p.advisorPostCommitCheckpointDir == "" {
		p.steadyFakeNUMAMigrationTarget = nil
		return nil
	}
	factory := &steadyFakeNUMAMigrationCheckpointCodec{
		topology: p.steadyFakeNUMACheckpointTopology(),
	}
	recovered, err := p.steadyFakeNUMACheckpointStore().Recover(
		steadyFakeNUMAMigrationCheckpointName, factory)
	if err != nil {
		return err
	}
	if recovered == nil {
		p.steadyFakeNUMAMigrationTarget = nil
		return nil
	}
	codec, ok := recovered.(*steadyFakeNUMAMigrationCheckpointCodec)
	if !ok {
		return fmt.Errorf("unexpected steady fake-NUMA checkpoint codec type %T", recovered)
	}
	p.steadyFakeNUMAMigrationTarget = &steadyFakeNUMAMigrationTarget{
		constraintDigest: codec.constraintDigest,
		target:           machine.NewCPUSet(codec.targetCPUs...),
	}
	return nil
}

func (p *DynamicPolicy) removeSteadyFakeNUMAMigrationTarget() error {
	if p.advisorPostCommitCheckpointDir != "" {
		if err := p.steadyFakeNUMACheckpointStore().Remove(
			steadyFakeNUMAMigrationCheckpointName); err != nil {
			return err
		}
	}
	p.steadyFakeNUMAMigrationTarget = nil
	return nil
}

func (p *DynamicPolicy) applySteadyFakeNUMAMigrationCheckpointTransition(
	transition steadyFakeNUMAMigrationCheckpointTransition,
) error {
	switch transition.kind {
	case steadyFakeNUMAMigrationCheckpointKeep:
		return nil
	case steadyFakeNUMAMigrationCheckpointReplace:
		return p.storeSteadyFakeNUMAMigrationTarget(transition.target)
	case steadyFakeNUMAMigrationCheckpointRemove:
		return p.removeSteadyFakeNUMAMigrationTarget()
	default:
		return fmt.Errorf(
			"invalid steady fake-NUMA migration checkpoint transition %d",
			transition.kind)
	}
}

func (p *DynamicPolicy) projectSteadyFakeNUMAStageWithCheckpoint(
	demands []partitionDemand,
	fakeKeys []string,
	committed steadyFakeNUMACommittedSnapshot,
	freshDesired map[string]machine.CPUSet,
	floors []partitionCoreFloorConstraint,
) (map[string]machine.CPUSet, error) {
	assignments, transition, err := p.planSteadyFakeNUMAStageWithCheckpoint(
		demands, fakeKeys, committed, freshDesired, floors)
	if err != nil {
		return nil, err
	}
	if err := p.applySteadyFakeNUMAMigrationCheckpointTransition(transition); err != nil {
		return nil, err
	}
	return assignments, nil
}

// planSteadyFakeNUMAStageWithCheckpoint computes one bounded migration stage
// and the checkpoint transition that describes its durable intent. Planning is
// side-effect free: the caller remains the canonical owner of applying the
// returned transition only after the surrounding state transaction is ready.
func (p *DynamicPolicy) planSteadyFakeNUMAStageWithCheckpoint(
	demands []partitionDemand,
	fakeKeys []string,
	committed steadyFakeNUMACommittedSnapshot,
	freshDesired map[string]machine.CPUSet,
	floors []partitionCoreFloorConstraint,
) (
	map[string]machine.CPUSet,
	steadyFakeNUMAMigrationCheckpointTransition,
	error,
) {
	keep := steadyFakeNUMAMigrationCheckpointTransition{
		kind: steadyFakeNUMAMigrationCheckpointKeep,
	}
	if p.machine.machineInfo == nil || p.machine.machineInfo.CPUTopology == nil {
		return nil, keep, fmt.Errorf(
			"cannot project durable steady fake-NUMA migration without topology")
	}
	topology := p.machine.machineInfo.CPUTopology
	digest, err := steadyFakeNUMAConstraintDigest(demands, fakeKeys, floors, topology)
	if err != nil {
		return nil, keep, err
	}
	if err := validateCommittedSteadyFakeNUMASnapshot(committed, floors, topology); err != nil {
		assignments, projectErr := projectSteadyFakeNUMAStage(
			demands, fakeKeys, committed, freshDesired, floors, topology)
		if projectErr != nil {
			return nil, keep, projectErr
		}
		if p.steadyFakeNUMAMigrationTarget == nil {
			return assignments, keep, nil
		}
		return assignments, steadyFakeNUMAMigrationCheckpointTransition{
			kind: steadyFakeNUMAMigrationCheckpointRemove,
		}, nil
	}

	desired := freshDesired
	target := unionPartitionAssignments(freshDesired, fakeKeys)
	transition := keep
	if current := p.steadyFakeNUMAMigrationTarget; current != nil &&
		current.constraintDigest == digest {
		if committed.reclaim.Equals(current.target) {
			transition.kind = steadyFakeNUMAMigrationCheckpointRemove
		} else {
			desired, err = steadyFakeNUMAAssignmentsForTarget(
				demands, fakeKeys, current.target, freshDesired, floors, topology)
			if err != nil {
				return nil, keep, fmt.Errorf(
					"resume steady fake-NUMA migration target: %w", err)
			}
			target = current.target
		}
	} else if steadyFakeNUMAMigrationChurn(committed.reclaim, target) >
		steadyFakeNUMAMaxMigratedCPUs {
		transition = steadyFakeNUMAMigrationCheckpointTransition{
			kind: steadyFakeNUMAMigrationCheckpointReplace,
			target: &steadyFakeNUMAMigrationTarget{
				constraintDigest: digest,
				target:           target.Clone(),
			},
		}
	} else if p.steadyFakeNUMAMigrationTarget != nil {
		transition.kind = steadyFakeNUMAMigrationCheckpointRemove
	}

	assignments, err := projectSteadyFakeNUMAStage(
		demands, fakeKeys, committed, desired, floors, topology)
	if err != nil {
		return nil, keep, err
	}
	stage := unionPartitionAssignments(assignments, fakeKeys)
	general.InfoS("steady fake NUMA migration checkpoint planned",
		"constraintDigest", digest,
		"currentCPUSet", committed.reclaim.String(),
		"frozenTargetCPUSet", target.String(),
		"stageCPUSet", stage.String(),
		"currentDistance", steadyFakeNUMAMigrationChurn(committed.reclaim, target),
		"nextDistance", steadyFakeNUMAMigrationChurn(stage, target),
		"stageChurn", steadyFakeNUMAMigrationChurn(committed.reclaim, stage),
		"checkpointTransition", transition.kind.String())
	return assignments, transition, nil
}

func steadyFakeNUMAAssignmentsForTarget(
	demands []partitionDemand,
	fakeKeys []string,
	target machine.CPUSet,
	preferredDesired map[string]machine.CPUSet,
	floors []partitionCoreFloorConstraint,
	topology *machine.CPUTopology,
) (map[string]machine.CPUSet, error) {
	demandByKey := make(map[string]partitionDemand, len(demands))
	for _, demand := range demands {
		demandByKey[demand.key] = demand
	}
	pins, err := steadyFakeNUMAPinsForUnion(
		target, fakeKeys, demandByKey, preferredDesired, topology)
	if err != nil {
		return nil, err
	}
	attempts := 0
	assignments, err := solveSteadyFakeNUMAWithPins(
		demands, fakeKeys, floors, pins, topology, &attempts)
	if err != nil {
		return nil, err
	}
	if _, err := validateSteadyFakeNUMAFinal(
		demands, fakeKeys, assignments, topology, nil, false); err != nil {
		return nil, err
	}
	if got := unionPartitionAssignments(assignments, fakeKeys); !got.Equals(target) {
		return nil, fmt.Errorf(
			"restored target assignment changed fake union from %s to %s",
			target.String(), got.String())
	}
	return assignments, nil
}

func ensureSteadyFakeNUMACheckpointEOF(decoder *json.Decoder) error {
	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("checkpoint contains trailing JSON value")
		}
		return fmt.Errorf("decode checkpoint trailing data: %w", err)
	}
	return nil
}

func steadyFakeNUMAConstraintDigest(
	demands []partitionDemand,
	fakeKeys []string,
	floors []partitionCoreFloorConstraint,
	topology *machine.CPUTopology,
) (string, error) {
	if topology == nil {
		return "", fmt.Errorf("cannot digest steady fake-NUMA constraints with nil topology")
	}
	sortedDemands := sortedByKey(demands, func(d partitionDemand) string { return d.key })
	sortedFakeKeys := append([]string(nil), fakeKeys...)
	sort.Strings(sortedFakeKeys)
	sortedFloors := append([]partitionCoreFloorConstraint(nil), floors...)
	sort.Slice(sortedFloors, func(i, j int) bool {
		return sortedFloors[i].demandKey < sortedFloors[j].demandKey
	})

	var canonical strings.Builder
	for _, cpu := range topology.CPUDetails.CPUs().ToSliceInt() {
		info := topology.CPUDetails[cpu]
		fmt.Fprintf(&canonical, "t:%d:%d:%d:%d;", cpu, info.NUMANodeID, info.SocketID, info.CoreID)
	}
	for _, demand := range sortedDemands {
		fmt.Fprintf(&canonical, "d:%s:%d:%s:%s:%s;",
			demand.key, demand.quantity, demand.class, demand.requestGroupKey, demand.eligible.String())
	}
	for _, key := range sortedFakeKeys {
		canonical.WriteString("f:")
		canonical.WriteString(strconv.Quote(key))
		canonical.WriteByte(';')
	}
	for _, floor := range sortedFloors {
		fmt.Fprintf(&canonical, "q:%s;", floor.demandKey)
	}
	sum := sha256.Sum256([]byte(canonical.String()))
	return hex.EncodeToString(sum[:]), nil
}
