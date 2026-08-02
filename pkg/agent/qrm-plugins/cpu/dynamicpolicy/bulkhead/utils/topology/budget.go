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
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

var (
	ErrRoundBudgetExceeded                = errors.New("convergence round budget exceeded")
	ErrHierarchyIOOperationBudgetExceeded = errors.New("hierarchy I/O operation budget exceeded")
	ErrNodeBudgetExceeded                 = errors.New("snapshot node budget exceeded")
	ErrHierarchyDepthBudget               = errors.New("hierarchy depth budget exceeded")
	ErrDomainBudgetExceeded               = errors.New("domain budget exceeded")
	ErrTransferEdgeBudgetExceeded         = errors.New("transfer edge budget exceeded")
	ErrPlanOperationBudgetExceeded        = errors.New("plan operation budget exceeded")
	ErrDeadlockProbeBudgetExceeded        = errors.New("deadlock probe operation budget exceeded")
	ErrConvergenceDeadlineExceeded        = errors.New("convergence deadline exceeded")
)

// ConvergenceBudget bounds all work performed by one coordinator invocation.
// Zero MaxRounds, MaxHierarchyIOOperations, and MaxPlanOperations request
// invocation-scoped automatic limits. Zero means unbounded for other fields.
type ConvergenceBudget struct {
	MaxRounds int
	// MaxHierarchyIOOperations counts logical HierarchyDriver method calls,
	// not implementation syscalls. Retries and explicit verification calls
	// are separate operations.
	MaxHierarchyIOOperations   int
	MaxSnapshotNodes           int
	MaxSnapshotDepth           int
	MaxDomains                 int
	MaxTransferEdges           int
	MaxPlanOperations          int
	MaxDeadlockProbeOperations int
	// DeadlineDuration bounds one invocation when the caller does not provide a
	// tighter context deadline. It is converted to an absolute Deadline when a
	// coordinator invocation starts.
	DeadlineDuration time.Duration
	Deadline         time.Time
}

// BudgetUsage is a point-in-time copy of charged logical work.
type BudgetUsage struct {
	Rounds                  int
	HierarchyIOOperations   int
	Nodes                   int
	MaxDepth                int
	Domains                 int
	Edges                   int
	Operations              int
	DeadlockProbeOperations int
}

const defaultConvergenceDeadlineDuration = 10 * time.Second

// DefaultConvergenceBudget returns defensive structural limits. Cumulative
// round, hierarchy-I/O, and plan limits are derived after the first snapshot.
func DefaultConvergenceBudget() ConvergenceBudget {
	return ConvergenceBudget{
		MaxRounds:                  0,
		MaxHierarchyIOOperations:   0,
		MaxSnapshotNodes:           defaultApplyMaxSnapshotNodes,
		MaxSnapshotDepth:           defaultApplyMaxSnapshotDepth,
		MaxDomains:                 256,
		MaxTransferEdges:           4096,
		MaxPlanOperations:          0,
		MaxDeadlockProbeOperations: defaultDeadlockProbeBudget,
		DeadlineDuration:           defaultConvergenceDeadlineDuration,
	}
}

func NormalizeConvergenceBudget(in ConvergenceBudget) ConvergenceBudget {
	defaults := DefaultConvergenceBudget()
	if in.MaxSnapshotNodes == 0 {
		in.MaxSnapshotNodes = defaults.MaxSnapshotNodes
	}
	if in.MaxSnapshotDepth == 0 {
		in.MaxSnapshotDepth = defaults.MaxSnapshotDepth
	}
	if in.MaxDomains == 0 {
		in.MaxDomains = defaults.MaxDomains
	}
	if in.MaxTransferEdges == 0 {
		in.MaxTransferEdges = defaults.MaxTransferEdges
	}
	if in.MaxDeadlockProbeOperations == 0 {
		in.MaxDeadlockProbeOperations = defaults.MaxDeadlockProbeOperations
	}
	if in.DeadlineDuration == 0 {
		in.DeadlineDuration = defaults.DeadlineDuration
	}
	return in
}

func BudgetWithInvocationDeadline(ctx context.Context, in ConvergenceBudget, now time.Time) ConvergenceBudget {
	out := NormalizeConvergenceBudget(in)
	if out.DeadlineDuration > 0 {
		out.Deadline = earliestDeadline(out.Deadline, now.Add(out.DeadlineDuration))
	}
	if deadline, ok := ctx.Deadline(); ok {
		out.Deadline = earliestDeadline(out.Deadline, deadline)
	}
	return out
}

func earliestDeadline(left, right time.Time) time.Time {
	switch {
	case left.IsZero():
		return right
	case right.IsZero():
		return left
	case right.Before(left):
		return right
	default:
		return left
	}
}

type budgetNodeKey struct {
	rel      string
	identity CgroupIdentity
}

// BudgetTracker owns invocation-scoped usage shared by the driver, snapshot,
// planner, writer, and coordinator.
type BudgetTracker struct {
	mu      sync.Mutex
	limit   ConvergenceBudget
	usage   BudgetUsage
	visited map[budgetNodeKey]struct{}
}

func NewBudgetTracker(limit ConvergenceBudget) *BudgetTracker {
	return &BudgetTracker{
		limit:   limit,
		visited: make(map[budgetNodeKey]struct{}),
	}
}

func (b *BudgetTracker) Usage() BudgetUsage {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.usage
}

func (b *BudgetTracker) configureAutoCumulativeLimits(rounds, snapshotNodes int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.usage.Rounds != 0 {
		return fmt.Errorf("cannot configure cumulative budgets after round use: used=%d", b.usage.Rounds)
	}
	if rounds <= 0 {
		return fmt.Errorf("automatic round budget must be positive: %d", rounds)
	}
	if snapshotNodes <= 0 {
		snapshotNodes = 1
	}
	if b.limit.MaxRounds == 0 {
		b.limit.MaxRounds = rounds
	}
	if b.limit.MaxHierarchyIOOperations == 0 {
		// A fixed-point round may take several complete snapshots plus
		// verification and writes. Size this from the actual controlled
		// hierarchy instead of a machine-independent cumulative constant.
		perRound := saturatingAdd(saturatingMultiply(snapshotNodes, 8), 64)
		b.limit.MaxHierarchyIOOperations = saturatingMultiply(saturatingAdd(rounds, 2), perRound)
	}
	if b.limit.MaxPlanOperations == 0 {
		// Drain and expand planning each walk controlled entries and charge
		// candidate operations. Leave conservative room for both phases.
		perRound := saturatingAdd(saturatingMultiply(snapshotNodes, 4), 32)
		b.limit.MaxPlanOperations = saturatingMultiply(rounds, perRound)
	}
	return nil
}

func saturatingMultiply(left, right int) int {
	if left <= 0 || right <= 0 {
		return 0
	}
	maxInt := int(^uint(0) >> 1)
	if left > maxInt/right {
		return maxInt
	}
	return left * right
}

func saturatingAdd(left, right int) int {
	maxInt := int(^uint(0) >> 1)
	if right > 0 && left > maxInt-right {
		return maxInt
	}
	return left + right
}

func (b *BudgetTracker) beforeHierarchyIOOperation(ctx context.Context) error {
	if err := b.checkContextDeadline(ctx); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.limit.MaxHierarchyIOOperations > 0 && b.usage.HierarchyIOOperations >= b.limit.MaxHierarchyIOOperations {
		return fmt.Errorf("%w: limit=%d used=%d", ErrHierarchyIOOperationBudgetExceeded, b.limit.MaxHierarchyIOOperations, b.usage.HierarchyIOOperations)
	}
	b.usage.HierarchyIOOperations++
	return nil
}

func (b *BudgetTracker) checkContextDeadline(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.limit.Deadline.IsZero() && !time.Now().Before(b.limit.Deadline) {
		return fmt.Errorf("%w: deadline=%s", ErrConvergenceDeadlineExceeded, b.limit.Deadline.Format(time.RFC3339Nano))
	}
	return nil
}

func isConvergenceBudgetError(err error) bool {
	return errors.Is(err, ErrRoundBudgetExceeded) ||
		errors.Is(err, ErrHierarchyIOOperationBudgetExceeded) ||
		errors.Is(err, ErrNodeBudgetExceeded) ||
		errors.Is(err, ErrHierarchyDepthBudget) ||
		errors.Is(err, ErrDomainBudgetExceeded) ||
		errors.Is(err, ErrTransferEdgeBudgetExceeded) ||
		errors.Is(err, ErrPlanOperationBudgetExceeded) ||
		errors.Is(err, ErrDeadlockProbeBudgetExceeded) ||
		errors.Is(err, ErrConvergenceDeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}

func (b *BudgetTracker) ConsumeDeadlockProbeOperations(ctx context.Context, n int) error {
	if n <= 0 {
		return nil
	}
	if err := b.checkContextDeadline(ctx); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.limit.MaxDeadlockProbeOperations > 0 &&
		n > b.limit.MaxDeadlockProbeOperations-b.usage.DeadlockProbeOperations {
		return fmt.Errorf("%w: limit=%d used=%d requested=%d",
			ErrDeadlockProbeBudgetExceeded, b.limit.MaxDeadlockProbeOperations,
			b.usage.DeadlockProbeOperations, n)
	}
	b.usage.DeadlockProbeOperations += n
	return nil
}

func (b *BudgetTracker) VisitNode(rel string, identity CgroupIdentity, depth int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkDepthLocked(depth); err != nil {
		return err
	}
	key := budgetNodeKey{rel: rel, identity: identity}
	if _, ok := b.visited[key]; ok {
		return nil
	}
	if b.limit.MaxSnapshotNodes > 0 && b.usage.Nodes >= b.limit.MaxSnapshotNodes {
		return fmt.Errorf("%w: limit=%d used=%d rel=%q identity=%v", ErrNodeBudgetExceeded, b.limit.MaxSnapshotNodes, b.usage.Nodes, rel, identity)
	}
	b.visited[key] = struct{}{}
	b.usage.Nodes++
	return nil
}

func (b *BudgetTracker) CheckDepth(depth int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.checkDepthLocked(depth)
}

func (b *BudgetTracker) checkDepthLocked(depth int) error {
	if b.limit.MaxSnapshotDepth > 0 && depth > b.limit.MaxSnapshotDepth {
		return fmt.Errorf("%w: limit=%d requested=%d", ErrHierarchyDepthBudget, b.limit.MaxSnapshotDepth, depth)
	}
	if depth > b.usage.MaxDepth {
		b.usage.MaxDepth = depth
	}
	return nil
}

func (b *BudgetTracker) ConsumeRound() error {
	return b.consume(&b.usage.Rounds, b.limit.MaxRounds, 1, ErrRoundBudgetExceeded)
}

func (b *BudgetTracker) ConsumeDomains(count int) error {
	return b.consume(&b.usage.Domains, b.limit.MaxDomains, count, ErrDomainBudgetExceeded)
}

func (b *BudgetTracker) ConsumeTransferEdges(count int) error {
	return b.consume(&b.usage.Edges, b.limit.MaxTransferEdges, count, ErrTransferEdgeBudgetExceeded)
}

func (b *BudgetTracker) ConsumePlanOperations(count int) error {
	return b.consume(&b.usage.Operations, b.limit.MaxPlanOperations, count, ErrPlanOperationBudgetExceeded)
}

// ReserveHierarchyIOOperations charges a known execution upper bound before
// mutation starts. The reserved calls must then use the unbudgeted execution
// path so they are not charged a second time.
func (b *BudgetTracker) ReserveHierarchyIOOperations(count int) error {
	if err := b.checkContextDeadline(context.Background()); err != nil {
		return err
	}
	return b.consume(
		&b.usage.HierarchyIOOperations,
		b.limit.MaxHierarchyIOOperations,
		count,
		ErrHierarchyIOOperationBudgetExceeded,
	)
}

func (b *BudgetTracker) consume(used *int, limit, count int, sentinel error) error {
	if count < 0 {
		return fmt.Errorf("negative budget charge: %d", count)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if limit > 0 && *used+count > limit {
		return fmt.Errorf("%w: limit=%d used=%d requested=%d", sentinel, limit, *used, count)
	}
	*used += count
	return nil
}

type budgetedHierarchyDriver struct {
	driver HierarchyDriver
	budget *BudgetTracker
	// prepaid is the number of real driver calls charged atomically before a
	// mutation phase starts. Each method call still consumes one prepaid slot.
	prepaid int
	close   sync.Once
	err     error
}

func NewBudgetedHierarchyDriver(driver HierarchyDriver, budget *BudgetTracker) HierarchyDriver {
	return &budgetedHierarchyDriver{driver: driver, budget: budget}
}

func newReservedBudgetedHierarchyDriver(driver HierarchyDriver, budget *BudgetTracker, operations int) (HierarchyDriver, error) {
	if err := budget.ReserveHierarchyIOOperations(operations); err != nil {
		return nil, err
	}
	if wrapped, ok := driver.(*budgetedHierarchyDriver); ok && wrapped.budget == budget {
		driver = wrapped.driver
	}
	return &budgetedHierarchyDriver{driver: driver, budget: budget, prepaid: operations}, nil
}

func (d *budgetedHierarchyDriver) Close() error {
	d.close.Do(func() {
		d.err = d.driver.Close()
	})
	return d.err
}

func (d *budgetedHierarchyDriver) context(ctx context.Context) (context.Context, error) {
	if d.prepaid > 0 {
		if err := d.budget.checkContextDeadline(ctx); err != nil {
			return nil, err
		}
		d.prepaid--
		return ctx, nil
	}
	if err := d.budget.beforeHierarchyIOOperation(ctx); err != nil {
		return nil, err
	}
	return ctx, nil
}

func (d *budgetedHierarchyDriver) Roots(ctx context.Context) ([]RootRef, error) {
	ctx, err := d.context(ctx)
	if err != nil {
		return nil, err
	}
	return d.driver.Roots(ctx)
}

func (d *budgetedHierarchyDriver) StatIdentity(ctx context.Context, rel string) (CgroupIdentity, error) {
	ctx, err := d.context(ctx)
	if err != nil {
		return CgroupIdentity{}, err
	}
	return d.driver.StatIdentity(ctx, rel)
}

func (d *budgetedHierarchyDriver) ReadEntry(ctx context.Context, rel string) (EntryState, error) {
	ctx, err := d.context(ctx)
	if err != nil {
		return EntryState{}, err
	}
	return d.driver.ReadEntry(ctx, rel)
}

func (d *budgetedHierarchyDriver) ListChildren(ctx context.Context, rel string) ([]ChildRef, error) {
	ctx, err := d.context(ctx)
	if err != nil {
		return nil, err
	}
	if driver, ok := d.driver.(interface {
		listChildrenWithBudget(context.Context, string, *BudgetTracker) ([]ChildRef, error)
	}); ok {
		return driver.listChildrenWithBudget(ctx, rel, d.budget)
	}
	children, err := d.driver.ListChildren(ctx, rel)
	if err != nil {
		return nil, err
	}
	depth := childDepth(rel)
	for _, child := range children {
		if err := d.budget.checkContextDeadline(ctx); err != nil {
			return nil, err
		}
		if err := d.budget.VisitNode(filepath.Join(rel, child.Name), child.Identity, depth); err != nil {
			return nil, err
		}
	}
	return children, nil
}

func childDepth(parentRel string) int {
	clean := filepath.Clean(parentRel)
	if clean == "." {
		return 1
	}
	return len(strings.Split(clean, string(filepath.Separator))) + 1
}

func (d *budgetedHierarchyDriver) WriteCPUs(ctx context.Context, rel string, expected CgroupIdentity, cpus machine.CPUSet) error {
	ctx, err := d.context(ctx)
	if err != nil {
		return err
	}
	return d.driver.WriteCPUs(ctx, rel, expected, cpus)
}

func (d *budgetedHierarchyDriver) WriteMems(ctx context.Context, rel string, expected CgroupIdentity, mems string) error {
	ctx, err := d.context(ctx)
	if err != nil {
		return err
	}
	return d.driver.WriteMems(ctx, rel, expected, mems)
}

func (d *budgetedHierarchyDriver) Classify(err error, op HierarchyOperation) HierarchyErrorClass {
	switch {
	case err == nil:
		return HierarchyErrorNone
	case isConvergenceBudgetError(err):
		return HierarchyErrorBudget
	default:
		return d.driver.Classify(err, op)
	}
}

func (d *budgetedHierarchyDriver) Capabilities() HierarchyCapabilities {
	return d.driver.Capabilities()
}
