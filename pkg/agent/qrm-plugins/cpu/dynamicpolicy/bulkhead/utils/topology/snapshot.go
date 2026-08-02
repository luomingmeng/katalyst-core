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
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sort"
	"time"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

// SnapshotID fingerprints all observed state and the exact scan boundary.
type SnapshotID [sha256.Size]byte

type ScanPurpose string

const (
	ScanForPlan        ScanPurpose = "plan"
	ScanForPrecheck    ScanPurpose = "precheck"
	ScanForWitness     ScanPurpose = "witness"
	ScanForAppliedView ScanPurpose = "applied_view"
)

// ScanBoundary records the minimum sufficient evidence selected for a purpose.
type ScanBoundary struct {
	Purpose      ScanPurpose
	Roots        []string
	ExpandedRels []string
}

// SnapshotRequest selects a purpose-specific hierarchy boundary.
type SnapshotRequest struct {
	Purpose      ScanPurpose
	AffectedRels []string
	ParentRel    string
	SourceDomain DomainID
	MismatchRels []string
}

// CompleteSnapshot is the only ownership evidence accepted by the coordinator.
type CompleteSnapshot struct {
	ID           SnapshotID
	CapturedAt   time.Time
	Capabilities HierarchyCapabilities
	Entries      map[string]EntryState
	Children     map[string][]ChildRef
	DomainByRel  map[string]DomainID
	DomainUnion  map[DomainID]machine.CPUSet
	ScanBoundary ScanBoundary
	Cost         BudgetUsage
}

// SnapshotError preserves the failed operation and driver classification.
type SnapshotError struct {
	Operation HierarchyOperation
	Rel       string
	Class     HierarchyErrorClass
	Identity  CgroupIdentity
	// EvidenceID fingerprints the successfully observed portion of the failed
	// scan, including cgroup identities and configured/effective state.
	EvidenceID SnapshotID
	Err        error
}

func (e *SnapshotError) Error() string {
	return fmt.Sprintf("complete snapshot %s rel=%q class=%s: %v", e.Operation, e.Rel, e.Class, e.Err)
}

func (e *SnapshotError) Unwrap() error { return e.Err }

type snapshotBuilder struct {
	ctx        context.Context
	driver     HierarchyDriver
	budget     *BudgetTracker
	snapshot   *CompleteSnapshot
	controlled map[string]*TopoNode
}

// BuildCompleteSnapshot returns either complete purpose-scoped evidence or a
// nil snapshot and a typed error. It never publishes partially collected state.
func BuildCompleteSnapshot(
	ctx context.Context,
	driver HierarchyDriver,
	dag *TopoDAG,
	request SnapshotRequest,
	budget *BudgetTracker,
) (*CompleteSnapshot, error) {
	if driver == nil || dag == nil || budget == nil {
		return nil, &SnapshotError{Operation: HierarchyOperationRead, Class: HierarchyErrorInvalid, Err: fmt.Errorf("driver, dag and budget are required")}
	}
	if !driver.Capabilities().StableIdentity {
		return nil, &SnapshotError{
			Operation: HierarchyOperationStat,
			Class:     HierarchyErrorInvalid,
			Err:       fmt.Errorf("stable hierarchy identity is required"),
		}
	}
	if wrapped, ok := driver.(*budgetedHierarchyDriver); !ok || wrapped.budget != budget {
		driver = NewBudgetedHierarchyDriver(driver, budget)
	}
	boundary, _, expand, err := selectSnapshotBoundary(dag, request)
	if err != nil {
		return nil, &SnapshotError{Operation: HierarchyOperationRead, Class: HierarchyErrorInvalid, Err: err}
	}
	builder := &snapshotBuilder{
		ctx:        ctx,
		driver:     driver,
		budget:     budget,
		controlled: make(map[string]*TopoNode, len(dag.index)),
		snapshot: &CompleteSnapshot{
			CapturedAt:   time.Now(),
			Capabilities: driver.Capabilities(),
			Entries:      make(map[string]EntryState),
			Children:     make(map[string][]ChildRef),
			DomainByRel:  make(map[string]DomainID),
			DomainUnion:  make(map[DomainID]machine.CPUSet),
			ScanBoundary: boundary,
		},
	}
	for rel, node := range dag.index {
		builder.controlled[rel] = node
	}
	for _, rel := range boundary.Roots {
		node := dag.index[rel]
		domain := node.Domain
		if err := builder.scan(rel, domain, 1, CgroupIdentity{}, expand[rel], request.Purpose == ScanForPrecheck); err != nil {
			return nil, err
		}
	}
	builder.snapshot.Cost = budget.Usage()
	builder.snapshot.ID = fingerprintSnapshot(builder.snapshot)
	return builder.snapshot, nil
}

// TargetProofCPUs returns the state that proves a target under the snapshot's
// hierarchy semantics. Empty cgroup v2 targets are proved by configured state;
// every other target is proved by effective state.
func (s *CompleteSnapshot) TargetProofCPUs(rel string, target machine.CPUSet) (machine.CPUSet, bool) {
	if s == nil {
		return machine.NewCPUSet(), false
	}
	entry, ok := s.Entries[rel]
	if !ok {
		return machine.NewCPUSet(), false
	}
	return observedCPUsForTargetProof(entry, target, s.Capabilities).Clone(), true
}

func newCompleteSnapshotSource(driver HierarchyDriver, dag *TopoDAG, budget *BudgetTracker) func(context.Context) (*CompleteSnapshot, error) {
	affected := make([]string, 0, len(dag.index))
	for rel, node := range dag.index {
		if node.Domain == "" {
			continue
		}
		affected = append(affected, rel)
	}
	sort.Strings(affected)
	return func(ctx context.Context) (*CompleteSnapshot, error) {
		return BuildCompleteSnapshot(ctx, driver, dag, SnapshotRequest{
			Purpose:      ScanForPlan,
			AffectedRels: affected,
		}, budget)
	}
}

func (b *snapshotBuilder) scan(rel string, domain DomainID, depth int, expected CgroupIdentity, expand, immediateOnly bool) error {
	if _, done := b.snapshot.Entries[rel]; done {
		return nil
	}
	before, err := b.driver.StatIdentity(b.ctx, rel)
	if err != nil {
		return b.fail(HierarchyOperationStat, rel, expected, err)
	}
	if expected != (CgroupIdentity{}) && before != expected {
		return b.fail(HierarchyOperationStat, rel, before, fmt.Errorf("%w: listed=%v stat=%v", ErrCgroupIdentityChanged, expected, before))
	}
	if err := b.budget.VisitNode(rel, before, depth); err != nil {
		return b.fail(HierarchyOperationStat, rel, before, err)
	}
	entry, err := b.driver.ReadEntry(b.ctx, rel)
	if err != nil {
		return b.fail(HierarchyOperationRead, rel, before, err)
	}
	after, err := b.driver.StatIdentity(b.ctx, rel)
	if err != nil {
		return b.fail(HierarchyOperationStat, rel, before, err)
	}
	if before != after || entry.Identity != before {
		return b.fail(HierarchyOperationStat, rel, after, fmt.Errorf("%w: before=%v read=%v after=%v", ErrCgroupIdentityChanged, before, entry.Identity, after))
	}
	entry.Rel = rel
	entry.CPUs = entry.CPUs.Clone()
	entry.ConfiguredCPUs = entry.ConfiguredCPUs.Clone()
	b.snapshot.Entries[rel] = entry
	b.snapshot.DomainByRel[rel] = domain
	b.snapshot.DomainUnion[domain] = b.snapshot.DomainUnion[domain].Union(entry.CPUs)

	if !expand && !immediateOnly {
		return nil
	}
	children, err := b.driver.ListChildren(b.ctx, rel)
	if err != nil {
		return b.fail(HierarchyOperationList, rel, entry.Identity, err)
	}
	parentIdentity, err := b.driver.StatIdentity(b.ctx, rel)
	if err != nil {
		return b.fail(HierarchyOperationStat, rel, entry.Identity, err)
	}
	if parentIdentity != entry.Identity {
		return b.fail(HierarchyOperationStat, rel, parentIdentity, fmt.Errorf(
			"%w: read=%v after-list=%v", ErrCgroupIdentityChanged, entry.Identity, parentIdentity))
	}
	sort.Slice(children, func(i, j int) bool { return children[i].Name < children[j].Name })
	b.snapshot.Children[rel] = append([]ChildRef(nil), children...)
	b.snapshot.ScanBoundary.ExpandedRels = append(b.snapshot.ScanBoundary.ExpandedRels, rel)
	for _, child := range children {
		childRel := filepath.Join(rel, child.Name)
		childNode, isControlled := b.controlled[childRel]
		if isControlled && !immediateOnly {
			continue
		}
		childDomain := domain
		if isControlled {
			childDomain = childNode.Domain
		}
		childExpand := expand && !immediateOnly
		if err := b.scan(childRel, childDomain, depth+1, child.Identity, childExpand, false); err != nil {
			return err
		}
	}
	sort.Strings(b.snapshot.ScanBoundary.ExpandedRels)
	return nil
}

func (b *snapshotBuilder) fail(op HierarchyOperation, rel string, identity CgroupIdentity, err error) error {
	return &SnapshotError{
		Operation:  op,
		Rel:        rel,
		Class:      b.driver.Classify(err, op),
		Identity:   identity,
		EvidenceID: fingerprintSnapshot(b.snapshot),
		Err:        err,
	}
}

func selectSnapshotBoundary(dag *TopoDAG, request SnapshotRequest) (ScanBoundary, map[string]struct{}, map[string]bool, error) {
	selected := make(map[string]struct{})
	expand := make(map[string]bool)
	add := func(rel string, shouldExpand bool) error {
		node := dag.index[rel]
		if node == nil {
			return fmt.Errorf("snapshot boundary rel %q is not controlled", rel)
		}
		if node.Domain == "" {
			return fmt.Errorf("snapshot boundary rel %q has no explicit domain", rel)
		}
		selected[rel] = struct{}{}
		expand[rel] = expand[rel] || shouldExpand
		return nil
	}

	switch request.Purpose {
	case ScanForPlan:
		if len(request.AffectedRels) == 0 {
			return ScanBoundary{}, nil, nil, fmt.Errorf("plan snapshot requires affected rels")
		}
		for _, rel := range request.AffectedRels {
			if err := add(rel, true); err != nil {
				return ScanBoundary{}, nil, nil, err
			}
			for node := dag.index[rel].parent; node != nil; node = node.parent {
				if err := add(node.Rel, false); err != nil {
					return ScanBoundary{}, nil, nil, err
				}
			}
		}
	case ScanForPrecheck:
		if err := add(request.ParentRel, true); err != nil {
			return ScanBoundary{}, nil, nil, err
		}
	case ScanForWitness:
		if request.SourceDomain == "" {
			return ScanBoundary{}, nil, nil, fmt.Errorf("witness snapshot requires source domain")
		}
		for _, node := range dag.Nodes() {
			if node.Domain == request.SourceDomain && node.TrustAnchor {
				if err := add(node.Rel, false); err != nil {
					return ScanBoundary{}, nil, nil, err
				}
			}
		}
		if len(selected) == 0 {
			return ScanBoundary{}, nil, nil, fmt.Errorf("source domain %q has no trust anchors", request.SourceDomain)
		}
	case ScanForAppliedView:
		for _, node := range dag.Nodes() {
			if node.ControlledRoot || node.TrustAnchor || len(node.children) == 0 {
				if err := add(node.Rel, false); err != nil {
					return ScanBoundary{}, nil, nil, err
				}
			}
		}
		for _, rel := range request.MismatchRels {
			if err := add(rel, true); err != nil {
				return ScanBoundary{}, nil, nil, err
			}
		}
	default:
		return ScanBoundary{}, nil, nil, fmt.Errorf("unsupported scan purpose %q", request.Purpose)
	}

	roots := make([]string, 0, len(selected))
	for rel := range selected {
		roots = append(roots, rel)
	}
	sort.Strings(roots)
	return ScanBoundary{Purpose: request.Purpose, Roots: roots}, selected, expand, nil
}

func fingerprintSnapshot(snapshot *CompleteSnapshot) SnapshotID {
	hash := sha256.New()
	writeHashString(hash, string(snapshot.ScanBoundary.Purpose))
	writeHashUint64(hash, hierarchyCapabilitiesBits(snapshot.Capabilities))
	for _, rel := range snapshot.ScanBoundary.Roots {
		writeHashString(hash, rel)
	}
	for _, rel := range snapshot.ScanBoundary.ExpandedRels {
		writeHashString(hash, rel)
	}
	domains := make([]string, 0, len(snapshot.DomainUnion))
	for domain := range snapshot.DomainUnion {
		domains = append(domains, string(domain))
	}
	sort.Strings(domains)
	for _, domain := range domains {
		writeHashString(hash, domain)
		writeHashString(hash, snapshot.DomainUnion[DomainID(domain)].String())
	}
	rels := make([]string, 0, len(snapshot.Entries))
	for rel := range snapshot.Entries {
		rels = append(rels, rel)
	}
	sort.Strings(rels)
	for _, rel := range rels {
		entry := snapshot.Entries[rel]
		writeHashString(hash, rel)
		writeHashString(hash, string(snapshot.DomainByRel[rel]))
		writeHashUint64(hash, entry.Identity.Device)
		writeHashUint64(hash, entry.Identity.Inode)
		writeHashString(hash, entry.CPUs.String())
		writeHashString(hash, entry.Mems)
		writeHashString(hash, entry.ConfiguredCPUs.String())
		writeHashString(hash, entry.ConfiguredMems)
		for _, child := range snapshot.Children[rel] {
			writeHashString(hash, child.Name)
			writeHashUint64(hash, child.Identity.Device)
			writeHashUint64(hash, child.Identity.Inode)
		}
	}
	var id SnapshotID
	copy(id[:], hash.Sum(nil))
	return id
}

func hierarchyCapabilitiesBits(capabilities HierarchyCapabilities) uint64 {
	var bits uint64
	for index, enabled := range []bool{
		capabilities.StableIdentity,
		capabilities.EmptyConfiguredCPUSet,
		capabilities.EffectiveCPUSet,
		capabilities.KernelParentContainment,
		capabilities.PartitionRoots,
	} {
		if enabled {
			bits |= 1 << index
		}
	}
	return bits
}

func writeHashString(hash interface{ Write([]byte) (int, error) }, value string) {
	writeHashUint64(hash, uint64(len(value)))
	_, _ = hash.Write([]byte(value))
}

func writeHashUint64(hash interface{ Write([]byte) (int, error) }, value uint64) {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], value)
	_, _ = hash.Write(encoded[:])
}

func cloneCPUSetMap(in map[string]machine.CPUSet) map[string]machine.CPUSet {
	out := make(map[string]machine.CPUSet, len(in))
	for rel, cpus := range in {
		out[rel] = cpus.Clone()
	}
	return out
}
