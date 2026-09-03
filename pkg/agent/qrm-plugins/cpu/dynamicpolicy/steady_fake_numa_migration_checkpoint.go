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
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

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

func (p *DynamicPolicy) storeSteadyFakeNUMAMigrationTarget(
	target *steadyFakeNUMAMigrationTarget,
) error {
	if target == nil || target.constraintDigest == "" {
		return fmt.Errorf("invalid empty steady fake-NUMA migration target")
	}
	path := p.steadyFakeNUMAMigrationCheckpointPath()
	if path == "" {
		return nil
	}
	targetCPUs := target.target.ToSliceInt()
	checkpoint := steadyFakeNUMAMigrationCheckpoint{
		Version:          steadyFakeNUMAMigrationCheckpointVersion,
		ConstraintDigest: target.constraintDigest,
		TargetCPUs:       targetCPUs,
	}
	checkpoint.Checksum = steadyFakeNUMAMigrationCheckpointChecksum(
		checkpoint.Version, checkpoint.ConstraintDigest, checkpoint.TargetCPUs)
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return fmt.Errorf("marshal steady fake-NUMA migration target: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("create steady fake-NUMA migration checkpoint directory: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+steadyFakeNUMAMigrationCheckpointName+"-*")
	if err != nil {
		return fmt.Errorf("create temporary steady fake-NUMA migration checkpoint: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return fmt.Errorf("chmod temporary steady fake-NUMA migration checkpoint: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		return fmt.Errorf("write temporary steady fake-NUMA migration checkpoint: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync temporary steady fake-NUMA migration checkpoint: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary steady fake-NUMA migration checkpoint: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("publish steady fake-NUMA migration checkpoint: %w", err)
	}
	if err := syncAdvisorPostCommitDirectory(path); err != nil {
		return fmt.Errorf("sync steady fake-NUMA migration checkpoint directory: %w", err)
	}
	p.steadyFakeNUMAMigrationTarget = &steadyFakeNUMAMigrationTarget{
		constraintDigest: target.constraintDigest,
		target:           target.target.Clone(),
	}
	return nil
}

func (p *DynamicPolicy) restoreSteadyFakeNUMAMigrationTarget() error {
	path := p.steadyFakeNUMAMigrationCheckpointPath()
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		p.steadyFakeNUMAMigrationTarget = nil
		return nil
	}
	if err != nil {
		return fmt.Errorf("read checkpoint: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var checkpoint steadyFakeNUMAMigrationCheckpoint
	if err := decoder.Decode(&checkpoint); err != nil {
		return fmt.Errorf("decode checkpoint: %w", err)
	}
	if err := ensureSteadyFakeNUMACheckpointEOF(decoder); err != nil {
		return err
	}
	if checkpoint.Version != steadyFakeNUMAMigrationCheckpointVersion {
		return fmt.Errorf("unsupported checkpoint version %d", checkpoint.Version)
	}
	if checkpoint.ConstraintDigest == "" {
		return fmt.Errorf("checkpoint constraint digest is empty")
	}
	if checkpoint.Checksum != steadyFakeNUMAMigrationCheckpointChecksum(
		checkpoint.Version, checkpoint.ConstraintDigest, checkpoint.TargetCPUs) {
		return fmt.Errorf("checkpoint checksum mismatch")
	}
	target := machine.NewCPUSet(checkpoint.TargetCPUs...)
	if target.Size() != len(checkpoint.TargetCPUs) {
		return fmt.Errorf("checkpoint target contains duplicate CPUs")
	}
	if p.machineInfo == nil || p.machineInfo.CPUTopology == nil {
		return fmt.Errorf("checkpoint validation requires CPU topology")
	}
	if outside := target.Difference(p.machineInfo.CPUTopology.CPUDetails.CPUs()); !outside.IsEmpty() {
		return fmt.Errorf("checkpoint target contains CPUs outside topology: %s", outside.String())
	}
	if err := assertCoreAligned(target, p.machineInfo.CPUTopology); err != nil {
		return fmt.Errorf("checkpoint target is not core aligned: %w", err)
	}
	p.steadyFakeNUMAMigrationTarget = &steadyFakeNUMAMigrationTarget{
		constraintDigest: checkpoint.ConstraintDigest,
		target:           target,
	}
	return nil
}

func (p *DynamicPolicy) removeSteadyFakeNUMAMigrationTarget() error {
	path := p.steadyFakeNUMAMigrationCheckpointPath()
	if path != "" {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove steady fake-NUMA migration checkpoint: %w", err)
		}
		if err := syncAdvisorPostCommitDirectory(path); err != nil {
			return fmt.Errorf("sync steady fake-NUMA migration checkpoint directory: %w", err)
		}
	}
	p.steadyFakeNUMAMigrationTarget = nil
	return nil
}

func (p *DynamicPolicy) projectSteadyFakeNUMAStageWithCheckpoint(
	demands []partitionDemand,
	fakeKeys []string,
	committed machine.CPUSet,
	freshDesired map[string]machine.CPUSet,
	floors []partitionCoreFloorConstraint,
) (map[string]machine.CPUSet, error) {
	if p.machineInfo == nil || p.machineInfo.CPUTopology == nil {
		return nil, fmt.Errorf("cannot project durable steady fake-NUMA migration without topology")
	}
	topology := p.machineInfo.CPUTopology
	digest, err := steadyFakeNUMAConstraintDigest(demands, fakeKeys, floors, topology)
	if err != nil {
		return nil, err
	}

	desired := freshDesired
	target := unionPartitionAssignments(freshDesired, fakeKeys)
	var targetToStore *steadyFakeNUMAMigrationTarget
	removeTarget := false
	if current := p.steadyFakeNUMAMigrationTarget; current != nil &&
		current.constraintDigest == digest {
		if committed.Equals(current.target) {
			removeTarget = true
		} else {
			desired, err = steadyFakeNUMAAssignmentsForTarget(
				demands, fakeKeys, current.target, freshDesired, floors, topology)
			if err != nil {
				return nil, fmt.Errorf("resume steady fake-NUMA migration target: %w", err)
			}
			target = current.target
		}
	} else if steadyFakeNUMAMigrationChurn(committed, target) >
		steadyFakeNUMAMaxMigratedCPUs {
		targetToStore = &steadyFakeNUMAMigrationTarget{
			constraintDigest: digest,
			target:           target,
		}
	} else if p.steadyFakeNUMAMigrationTarget != nil {
		removeTarget = true
	}

	assignments, err := projectSteadyFakeNUMAStage(
		demands, fakeKeys, committed, desired, floors, topology)
	if err != nil {
		return nil, err
	}
	if targetToStore != nil {
		if err := p.storeSteadyFakeNUMAMigrationTarget(targetToStore); err != nil {
			return nil, err
		}
	} else if removeTarget {
		if err := p.removeSteadyFakeNUMAMigrationTarget(); err != nil {
			return nil, err
		}
	}
	return assignments, nil
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
	sortedDemands := append([]partitionDemand(nil), demands...)
	sort.Slice(sortedDemands, func(i, j int) bool {
		return sortedDemands[i].key < sortedDemands[j].key
	})
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
