# CPUSet Materializer 与 RampUp 单一所有权实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复 RampUp 完整目标不一致问题，并将 DynamicPolicy、CPUSet 物化和 Bulkhead/CpusetTopology 收口为一个策略 owner、一个 materializer、一个物化入口和一个成功合同。

**Architecture:** `TargetState` 是唯一策略事实源，所有 mutation 由 `DynamicPolicy.transact` 串行执行。中立 `cpusetmaterializer` 包定义不可变 `Target` 与 `Materializer`；Bulkhead Manager 是当前唯一实现，CpusetTopology 只执行显式目标并返回 fresh convergence evidence。同步强收敛下不保存 runtime applied reclaim 缓存。

**Tech Stack:** Go 1.18、Katalyst QRM Dynamic Policy、checkpoint state、Linux cgroup v1/v2、Bulkhead DAG、`testify/require`

---

## 执行约束

- 当前 worktree 已有大量未提交实现，禁止 `git reset --hard`、`git checkout -- .`、覆盖式 cherry-pick。
- 每个任务开始前运行 `git status --short`，只暂存该任务列出的文件。
- 每个任务按红测、最小实现、绿测、提交的顺序执行。
- 不新增 deprecated wrapper、adapter、type alias、handler chain 或第二个 materializer。
- 若确认 Bulkhead 全局开关必须支持无需重启的 `false → true`，停止实施并回到设计评审；不得增加动态代理层。
- 所有注释使用英文，保持项目现有风格。

## 文件职责

| 文件 | 唯一职责 |
|---|---|
| `cpusetmaterializer/interface.go` | 中立 `Materializer` 接口 |
| `cpusetmaterializer/target.go` | 不可变 CPUSet 物化目标 |
| `cpusetmaterializer/result.go` | 收敛结果、证据与 typed error |
| `planner/ramp_up_reclaim_admission.go` | RampUp 完整 target 规划 |
| `planner/target_validator.go` | transaction 提交前策略不变量验证 |
| `materialization_target.go` | `TargetState → cpusetmaterializer.Target` 单向投影 |
| `policy_transaction.go` | 唯一 transaction、materialize 和 Base restore |
| `policy_lifecycle.go` | Start/Stop/readiness/recovery，不承载 shadow planning |
| `bulkhead/manager.go` | 唯一 `Materialize` 入口和私有 plugin 编排 |
| `bulkhead/plugins/cpusettopology/plugin.go` | cgroup topology 执行和 fresh evidence |

### Task 1: 建立中立 CPUSet Materializer 契约

**Files:**
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpusetmaterializer/interface.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpusetmaterializer/target.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpusetmaterializer/result.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpusetmaterializer/target_test.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpusetmaterializer/result_test.go`

- [ ] **Step 1: 写不可变 Target 红测**

```go
func TestTargetOwnsConstructorInputAndGetterResults(t *testing.T) {
	in := TargetInput{
		ReserveCPUSet:       machine.NewCPUSet(0),
		ReclaimCPUSet:       machine.NewCPUSet(2, 3),
		NonReclaimCPUSet:    machine.NewCPUSet(1, 4, 5),
		ReclaimCPUSetByNUMA: map[int]machine.CPUSet{0: machine.NewCPUSet(2, 3)},
		ContainerCPUSetByPod: map[string]map[string]machine.CPUSet{
			"pod": {"main": machine.NewCPUSet(1, 4)},
		},
	}
	target := NewTarget(in)

	in.ReclaimCPUSetByNUMA[0] = machine.NewCPUSet(5)
	in.ContainerCPUSetByPod["pod"]["main"] = machine.NewCPUSet(5)
	gotNUMA := target.ReclaimCPUSetByNUMA()
	gotPods := target.ContainerCPUSetByPod()
	gotNUMA[0] = machine.NewCPUSet(5)
	gotPods["pod"]["main"] = machine.NewCPUSet(5)

	require.True(t, target.ReclaimCPUSet().Equals(machine.NewCPUSet(2, 3)))
	require.True(t, target.ReclaimCPUSetByNUMA()[0].Equals(machine.NewCPUSet(2, 3)))
	require.True(t, target.ContainerCPUSetByPod()["pod"]["main"].Equals(machine.NewCPUSet(1, 4)))
}
```

- [ ] **Step 2: 运行红测**

Run:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpusetmaterializer/... -run TestTargetOwnsConstructorInputAndGetterResults
```

Expected: FAIL，package 或 `TargetInput` 尚不存在。

- [ ] **Step 3: 实现唯一契约**

```go
type Materializer interface {
	Materialize(context.Context, Target) (Result, error)
}

type TargetInput struct {
	ReserveCPUSet        machine.CPUSet
	ReclaimCPUSet        machine.CPUSet
	NonReclaimCPUSet     machine.CPUSet
	ReclaimCPUSetByNUMA  map[int]machine.CPUSet
	ContainerCPUSetByPod map[string]map[string]machine.CPUSet
	AllowReclaimOverlap  bool
}

type Target struct {
	reserveCPUSet        machine.CPUSet
	reclaimCPUSet        machine.CPUSet
	nonReclaimCPUSet     machine.CPUSet
	reclaimCPUSetByNUMA  map[int]machine.CPUSet
	containerCPUSetByPod map[string]map[string]machine.CPUSet
	allowReclaimOverlap  bool
}

type Result struct {
	Converged bool
	Evidence  Evidence
}

type Evidence struct {
	Executed           bool
	ControlledRels     map[string]RelEvidence
	PendingProtection  machine.CPUSet
	FailureReason      string
}

type RelEvidence struct {
	Target   machine.CPUSet
	Observed machine.CPUSet
	Reason   string
}

var ErrCPUSetNotConverged = errors.New("cpuset materialization not converged")
```

`NewTarget` 和所有 getter 必须 defensive clone；`Evidence` 提供 `Clone`，不得暴露 map backing storage。

- [ ] **Step 4: 增加 Result/Evidence ownership 测试**

```go
func TestEvidenceCloneOwnsNestedValues(t *testing.T) {
	evidence := Evidence{
		ControlledRels: map[string]RelEvidence{
			"kubesandbox": {
				Target:   machine.NewCPUSet(1, 2),
				Observed: machine.NewCPUSet(1, 2),
			},
		},
	}
	cloned := evidence.Clone()
	cloned.ControlledRels["kubesandbox"] = RelEvidence{Target: machine.NewCPUSet(3)}
	require.True(t, evidence.ControlledRels["kubesandbox"].Target.Equals(machine.NewCPUSet(1, 2)))
}
```

- [ ] **Step 5: 验证契约包**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpusetmaterializer/...
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpusetmaterializer/...
go vet ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpusetmaterializer/...
```

Expected: PASS。

- [ ] **Step 6: 提交**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpusetmaterializer
git commit -m "feat(cpu): define cpuset materializer contract"
```

### Task 2: 让 RampUp Planner 生成完整 TargetState

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/planner/ramp_up_reclaim_admission.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/planner/ramp_up_reclaim_admission_test.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/planner/target_validator.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/planner/target_validator_test.go`

- [ ] **Step 1: 写完整目标红测**

```go
func TestPlanRampUpReclaimPoolTargetProducesCompleteTarget(t *testing.T) {
	topology, err := machine.GenerateDummyCPUTopology(8, 1, 2)
	require.NoError(t, err)
	base := targetWithReclaimAndShare(
		machine.NewCPUSet(2, 3),
		machine.NewCPUSet(0, 1, 4, 5, 6, 7),
	)
	base.PodEntries["pod-ramp"] = state.ContainerEntries{
		"main": &state.AllocationInfo{
			AllocationResult:        machine.NewCPUSet(0, 1, 2, 3),
			OriginalAllocationResult: machine.NewCPUSet(0, 1, 2, 3),
			RampUp:                  true,
		},
	}

	next, err := PlanRampUpReclaimPoolTarget(base, ReclaimTargetUpdate{
		Mode: ReclaimUpdateFull,
		Target: machine.NewCPUSet(2, 3),
	}, ReclaimHardConstraint{CPUs: machine.NewCPUSet(2, 3)}, topology, true)
	require.NoError(t, err)

	require.True(t, reclaimRaw(next).Equals(machine.NewCPUSet(2, 3)))
	require.Empty(t, reclaimRaw(next).Intersection(poolCPUSet(next, commonstate.PoolNameShare)).ToSliceInt())
	require.Empty(t, reclaimRaw(next).Intersection(
		next.PodEntries["pod-ramp"]["main"].AllocationResult).ToSliceInt())
	require.NoError(t, ValidateTarget(next, topology, machine.NewCPUSet(2, 3)))
}
```

- [ ] **Step 2: 运行红测**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/planner/... -run TestPlanRampUpReclaimPoolTargetProducesCompleteTarget
```

Expected: FAIL，share/container 仍包含 reclaim 或 validator 不存在。

- [ ] **Step 3: 删除重复 floor 输入并实现完整 COW**

将约束收窄为：

```go
type ReclaimHardConstraint struct {
	CPUs          machine.CPUSet
	AffectedNUMAs sets.Int
}
```

`PlanRampUpReclaimPoolTarget` 必须：

```go
finalReclaim := merged.Union(effectiveHardFloor(base, currentFloor, topology, hardPartitionEnabled))
next := base.Clone()
setReclaimPool(next, finalReclaim, topology)
removeCPUSetFromSharePools(next, finalReclaim, topology)
removeCPUSetFromRampUpContainers(next, finalReclaim, topology)
next.MachineState, err = state.GenerateMachineStateFromPodEntries(
	topology, next.PodEntries, base.MachineState)
```

`effectiveHardFloor` 只读取 Base 中所有 `RampUp=true` owner 与 committed reclaim，不跳过本次 owner，不读取 `CommittedRaw` 参数。

- [ ] **Step 4: 实现统一 validator**

```go
func ValidateTarget(
	target *state.TargetState,
	topology *machine.CPUTopology,
	requiredFloor machine.CPUSet,
) error
```

typed errors：

```go
var (
	ErrHardFloorDropped            = errors.New("reclaim hard floor dropped")
	ErrReclaimOverlapsShare        = errors.New("reclaim overlaps share pool")
	ErrReclaimOverlapsRampUp       = errors.New("reclaim overlaps ramp-up allocation")
	ErrTopologyProjectionMismatch  = errors.New("topology assignment projection mismatch")
	ErrMachineStateMismatch        = errors.New("machine state does not match pod entries")
)
```

MachineState 对比使用 `state.GenerateMachineStateFromPodEntries` 生成 expected 后比较，不自动修复。

- [ ] **Step 5: 覆盖 partial、Base 不变和 validator 负例**

新增测试：

```text
TestPlanRampUpReclaimPoolTargetPartialUpdatePreservesUnaffectedNUMAs
TestPlanRampUpReclaimPoolTargetDoesNotMutateBase
TestEffectiveHardFloorDerivesAllActiveOwnersFromBase
TestValidateTargetRejectsReclaimShareOverlap
TestValidateTargetRejectsRampUpContainerOverlap
TestValidateTargetRejectsTopologyProjectionMismatch
TestValidateTargetRejectsMachineStateMismatch
```

- [ ] **Step 6: 验证 planner**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/planner/... -run 'TestPlanRampUp|TestEffectiveHardFloor|TestValidateTarget'
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/planner/...
```

Expected: PASS。

- [ ] **Step 7: 提交**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/planner/ramp_up_reclaim_admission.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/planner/ramp_up_reclaim_admission_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/planner/target_validator.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/planner/target_validator_test.go
git commit -m "fix(cpu): plan complete ramp-up targets"
```

### Task 3: 将 Bulkhead Manager 收口为唯一 Materializer

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/manager.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/manager_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/api/types.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/view.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/view_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/registry/registry.go`

- [ ] **Step 1: 写单入口 contract 红测**

```go
var _ cpusetmaterializer.Materializer = (*Manager)(nil)

func TestManagerMaterializeRejectsNonConvergedBeforeDependentPlugins(t *testing.T) {
	topologyPlugin := &fakeTopologyPlugin{
		result: topology.DAGApplyResult{FullyConverged: false},
	}
	dependent := &fakePlugin{enabled: true}
	manager := newTestManager(topologyPlugin, dependent)

	result, err := manager.Materialize(context.Background(), testMaterializationTarget())
	require.ErrorIs(t, err, cpusetmaterializer.ErrCPUSetNotConverged)
	require.False(t, result.Converged)
	require.Zero(t, dependent.adjustCalls)
}
```

- [ ] **Step 2: 运行红测**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/... -run TestManagerMaterializeRejectsNonConvergedBeforeDependentPlugins
```

Expected: FAIL，`Manager.Materialize` 尚不存在。

- [ ] **Step 3: 构造注入 runtime 依赖并实现 Materialize**

Manager 构造后持有：

```go
type Manager struct {
	mu       sync.Mutex
	plugins  []bulkheadapi.Plugin
	conf     *config.Configuration
	dynamic  *dynamicconfig.Configuration
	emitter  metrics.MetricEmitter
	meta     *metaserver.MetaServer
	topology *machine.CPUTopology
}
```

唯一入口：

```go
func (m *Manager) Materialize(
	ctx context.Context,
	target cpusetmaterializer.Target,
) (cpusetmaterializer.Result, error)
```

固定执行：

```text
build private view from target
→ exactly one topology executor
→ require fresh full convergence
→ dependent plugins
→ aggregate Evidence
```

- [ ] **Step 4: 同一任务删除双入口和 applied cache**

删除：

```text
Manager.Apply
Manager.RunCPUSetAdjustmentHandlers
Manager.PublishAppliedReclaim
Manager.LatestAppliedReclaim
appliedMu
latestAppliedReclaim
appliedReclaimValid
state-based BuildCPUSetPartitionView
CPUSetAdjustmentHandlerCtx embedding
```

不要添加 forwarding wrapper。

- [ ] **Step 5: 适配 Manager 测试**

新增并通过：

```text
TestManagerMaterializeBuildsViewOnlyFromTarget
TestManagerMaterializeRequiresExactlyOneTopologyExecutor
TestManagerMaterializePropagatesDependentPluginFailure
TestManagerMaterializeReturnsCompleteEvidence
TestManagerMaterializeDoesNotMutateTarget
```

删除仅验证旧 `Apply`、`RunCPUSetAdjustmentHandlers` 和 applied cache 的测试。

- [ ] **Step 6: 验证 Bulkhead Manager**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/... -run 'TestManagerMaterialize|TestNewManager|TestRunPeriodical'
```

Expected: PASS。

- [ ] **Step 7: 提交**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead
git commit -m "refactor(bulkhead): expose one materialization entry"
```

### Task 4: 使 CpusetTopology 只执行不可变目标

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/plugin_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology_inputs.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/rels.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/rels_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/convergence.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/writer.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/topology/writer_test.go`

- [ ] **Step 1: 写输入不可变和 pending 冲突红测**

```go
func TestCPUSetTopologyRejectsPendingProtectionOverlappingReclaim(t *testing.T) {
	target := newTestTarget(
		machine.NewCPUSet(2, 3),
		machine.NewCPUSet(0, 1, 4, 5),
	)
	beforeReclaim := target.ReclaimCPUSet()
	beforeNonReclaim := target.NonReclaimCPUSet()
	plugin := newPendingPlugin(machine.NewCPUSet(2))

	result, err := plugin.ReconcileTopology(context.Background(), testContext(target))
	require.Error(t, err)
	require.False(t, result.FullyConverged)
	require.True(t, beforeReclaim.Equals(target.ReclaimCPUSet()))
	require.True(t, beforeNonReclaim.Equals(target.NonReclaimCPUSet()))
	require.Contains(t, result.ConvergenceReport.String(), "pending_reclaim_conflict")
}
```

`newTestTarget` 必须只调用 `cpusetmaterializer.NewTarget`；
`newPendingPlugin` 只注入 fake MetaServer/cgroup lookup 返回的 pending CPU；
`testContext` 只组装 plugin 运行时依赖，不得把 `state.State` 放回 context。

- [ ] **Step 2: 运行红测**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/... -run TestCPUSetTopologyRejectsPendingProtectionOverlappingReclaim
```

Expected: FAIL，当前逻辑会原地缩小 reclaim view。

- [ ] **Step 3: 删除输入 mutation**

删除 `ApplyTransientProtectedNonReclaim` 调用和函数。Pending protection 只作为：

```go
type RuntimeProtection struct {
	Union machine.CPUSet
	ByRel map[string]machine.CPUSet
}
```

进入 `DAGApplyInputs.ProtectedPendingCPUSet/ProtectedCPUSetByRel`。若：

```go
!protection.Union.IsSubsetOf(target.NonReclaimCPUSet()) ||
	!protection.Union.Intersection(target.ReclaimCPUSet()).IsEmpty()
```

则返回不收敛证据，不能构造另一份 CPUSet target。

- [ ] **Step 4: 为 pending cache 增加内部锁**

```go
type CPUSetTopologyPlugin struct {
	// existing fields
	pendingMu          sync.Mutex
	pendingProtections map[string]pendingPodProtection
}
```

所有读取、更新、reset、TTL cleanup 都在 `pendingMu` 下执行；不得依赖 Manager 外部锁。

- [ ] **Step 5: 显式 controlled-rel inventory**

新增：

```go
type ControlledRelTarget struct {
	Rel    string
	Target machine.CPUSet
}

func BuildControlledRelInventory(
	cfg bulkheadconfig.BulkheadConfiguration,
	target cpusetmaterializer.Target,
	siblings []string,
	containerRels map[string]machine.CPUSet,
) []ControlledRelTarget
```

必须包括空 target 的 per-NUMA rel、dynamic siblings 和可解析 leaves。Fresh convergence 遍历 inventory，不以 DAG nodes 替代 inventory。

- [ ] **Step 6: 增加完整收敛测试**

```text
TestCPUSetTopologyPendingProtectionDoesNotMutateInput
TestCPUSetTopologyRejectsPendingProtectionOutsideNonReclaim
TestCPUSetTopologyPendingProtectionCacheIsRaceSafe
TestCPUSetTopologyControlledInventoryIncludesEmptyNUMARel
TestCPUSetTopologyMissingControlledRelIsNotConverged
TestCPUSetTopologyMismatchProducesEvidence
TestCPUSetTopologyPrunesOnlyAfterFreshFullConvergence
```

- [ ] **Step 7: 验证 topology**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology/... -run 'TestCPUSetTopology'
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils/... -run 'TestBuildControlled|Test.*Converg|TestCollectActive'
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/...
```

Expected: PASS。

- [ ] **Step 8: 提交**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/cpusettopology \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/utils
git commit -m "refactor(bulkhead): keep topology targets immutable"
```

### Task 5: 建立 DynamicPolicy 唯一 Transaction

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_lifecycle.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_lifecycle_test.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_transaction.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_transaction_test.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/materialization_target.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/materialization_target_test.go`

- [ ] **Step 1: 写 transaction 红测**

```go
func TestTransactMaterializesBeforeCommit(t *testing.T) {
	events := []string{}
	repo := newRecordingRepository(&events)
	materializer := recordingMaterializer{
		materialize: func(cpusetmaterializer.Target) (cpusetmaterializer.Result, error) {
			events = append(events, "materialize")
			return cpusetmaterializer.Result{Converged: true}, nil
		},
	}
	p := newTransactionTestPolicy(repo, materializer)

	err := p.transact(context.Background(), func(base *state.TargetState) (*state.TargetState, error) {
		next := base.Clone()
		events = append(events, "plan")
		return next, nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"prepare", "plan", "materialize", "commit"}, events)
}
```

- [ ] **Step 2: 运行红测**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy -run TestTransactMaterializesBeforeCommit
```

Expected: FAIL，统一 `transact` 尚不存在。

- [ ] **Step 3: 实现 Target 投影**

```go
func BuildMaterializationTarget(
	target *state.TargetState,
	topology *machine.CPUTopology,
) (cpusetmaterializer.Target, error)
```

只读 owned target，一次性计算 reserve/reclaim/non-reclaim、per-NUMA reclaim 和 container map；不得读取 `p.state` 或 Bulkhead。

- [ ] **Step 4: 实现唯一 transaction**

```go
type TargetPlan func(*state.TargetState) (*state.TargetState, error)

func (p *DynamicPolicy) transact(ctx context.Context, plan TargetPlan) error {
	p.Lock()
	defer p.Unlock()

	if err := p.requireCPUPolicyReady(); err != nil {
		return err
	}
	base, err := p.state.PrepareDurableTarget()
	if err != nil {
		return fmt.Errorf("prepare durable target: %w", err)
	}
	next, err := plan(base)
	if err != nil {
		return err
	}
	if err := planner.ValidateTarget(next, p.machineInfo.CPUTopology, machine.NewCPUSet()); err != nil {
		return err
	}
	target, err := BuildMaterializationTarget(next, p.machineInfo.CPUTopology)
	if err != nil {
		return err
	}
	if err := p.materialize(ctx, target); err != nil {
		return p.restoreBaseOrBlock(ctx, base, err)
	}
	if err := p.state.CommitTarget(next); err != nil {
		return p.restoreBaseOrBlock(ctx, base, fmt.Errorf("commit target: %w", err))
	}
	return nil
}
```

`materialize` 对 nil 直接成功；非 nil 必须 `Converged=true`，否则返回 `ErrCPUSetNotConverged`。
迁移 caller 时删除入口外层已有的重复 `p.Lock()/p.Unlock()`，禁止同一 goroutine
二次获取 `DynamicPolicy` 主锁。

- [ ] **Step 5: 构造阶段只注入一个 materializer**

`DynamicPolicy` 删除具体 `bulkheadManager` 字段，增加：

```go
cpuSetMaterializer cpusetmaterializer.Materializer
```

Bulkhead 全局开关关闭时不创建 Manager；开启时构造一个 Manager 并赋给接口字段。不提供注册函数。

- [ ] **Step 6: 适配 recovery 和 Base restore**

`restoreBaseOrBlock` 只调用同一个 `materialize`，不 publish applied cache。Recovery 在 `ready` 前强收敛 committed target。

- [ ] **Step 7: 覆盖 transaction 失败矩阵**

```text
TestBuildMaterializationTargetUsesOnlyOwnedTarget
TestTransactNilMaterializerCommitsWithoutExecution
TestTransactCallsMaterializerExactlyOnce
TestTransactRejectsNonConvergedWithoutCommit
TestTransactRestoresBaseAfterMaterializerError
TestTransactRestoresBaseAfterCommitError
TestTransactBlocksPolicyWhenBaseRestoreFails
TestRecoverCommittedTargetConvergesBeforeReady
```

- [ ] **Step 8: 验证 transaction**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy -run 'TestBuildMaterializationTarget|TestTransact|TestRecoverCommittedTarget'
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy -run 'TestTransact|TestRecoverCommittedTarget'
```

Expected: PASS。

- [ ] **Step 9: 提交**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_lifecycle.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_lifecycle_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_transaction.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_transaction_test.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/materialization_target.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/materialization_target_test.go
git commit -m "refactor(cpu): centralize target transactions"
```

### Task 6: 删除 Shadow Policy 并迁移所有 Mutation Caller

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_allocation_handlers_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_advisor_handler_reclaim_reuse_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_async_handler.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_async_handler_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_irq_tuner.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_irq_tuner_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_resource_package.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_resource_package_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/resize_test.go`

- [ ] **Step 1: 写“无 Shadow Policy”行为红测**

```go
func TestAllocatePlansOnOwnedTargetWithoutShadowPolicy(t *testing.T) {
	p, repo, materializer := newAllocationTransactionFixture(t)
	_, err := p.Allocate(context.Background(), sharedRampUpRequest(t))
	require.NoError(t, err)
	require.Equal(t, 1, repo.prepareCalls)
	require.Equal(t, 1, materializer.calls)
	require.Equal(t, 1, repo.commitCalls)
}
```

- [ ] **Step 2: 将 allocation/remove/get-allocation helper 改为 owned-target API**

采用显式签名：

```go
func (p *DynamicPolicy) planAllocate(
	base *state.TargetState,
	req *pluginapi.ResourceRequest,
) (*state.TargetState, *pluginapi.ResourceAllocationResponse, error)

func planRemovePod(
	base *state.TargetState,
	req *pluginapi.RemovePodRequest,
) (*state.TargetState, *pluginapi.RemovePodResponse, error)
```

这些函数可以读取 immutable policy configuration，但不能调用 repository、materializer、advisor notification 或递归 RPC。

- [ ] **Step 3: Advisor 使用 committed ReclaimRaw 身份**

删除 `LatestAppliedReclaim` 消费，selector 固定：

```text
Base committed ReclaimRaw
→ fresh eligible CPUs
```

Advisor planner 返回一个完整 `TargetState`，只由统一 `transact` 物化和提交。

- [ ] **Step 4: 迁移 async/IRQ/resource-package**

所有 runtime callback 收窄为：

```go
func(base *state.TargetState) (*state.TargetState, error)
```

删除接受 `*DynamicPolicy` 副本的 callback 和独立 `Set* → StoreState`。

- [ ] **Step 5: 删除 shadow planning 符号**

从 `policy_lifecycle.go`/`policy.go` 删除：

```text
planningState
planningContext
newPlanningPolicy
planningRampUpAdmission
storeRequested
errPlanningStatePersistence
```

删除所有 mode-bit 分支，不保留 alias 或 wrapper。

- [ ] **Step 6: 运行 caller 测试**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/... -run \
  'TestAllocate|TestRemovePod|TestAdvisor|TestSystemExclusive|TestResidual|TestIRQ|TestResourcePackage'
```

Expected: PASS。

- [ ] **Step 7: 静态确认 shadow owner 已消失**

```bash
! git grep -nE 'newPlanningPolicy|planningState|planningContext|planningRampUpAdmission|storeRequested' \
  -- 'pkg/agent/qrm-plugins/cpu/dynamicpolicy/*.go' \
  'pkg/agent/qrm-plugins/cpu/dynamicpolicy/**/*.go'
```

Expected: 无输出。

- [ ] **Step 8: 提交**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy
git commit -m "refactor(cpu): remove shadow policy planning"
```

### Task 7: 收窄 State Repository 并删除 Mutable Adapter

**Files:**
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/state/state.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/state/target_state.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/state/state_mem.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/state/state_checkpoint.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/state/durable_target_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/state/state_test.go`

- [ ] **Step 1: 写最小 State interface contract 红测**

新增编译期 test helper，只接受：

```go
type targetRepository interface {
	state.ReadonlyState
	PrepareDurableTarget() (*state.TargetState, error)
	CommitTarget(*state.TargetState) error
}
```

并用 AST contract test 断言 `State` interface 不含：

```text
SetMachineState
SetNUMAHeadroom
SetPodEntries
SetAllocationInfo
Delete
ClearState
StoreState
```

- [ ] **Step 2: 运行红测**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/state/... -run TestStateInterfaceExposesOnlyReadonlyPrepareAndCommit
```

Expected: FAIL，当前 `State` 仍嵌入 writer。

- [ ] **Step 3: 收窄 repository**

```go
type State interface {
	ReadonlyState
	PrepareDurableTarget() (*TargetState, error)
	CommitTarget(next *TargetState) error
}
```

删除 `TargetState.Set*`、`Delete`、`ClearState`。Planner 直接编辑自己的 owned clone。

- [ ] **Step 4: 保持 checkpoint wire 兼容**

不得改变 `CPUPluginCheckpoint` 字段、checksum 或 decode。Restore 内部可以继续调用 package-private cache primitive，但不能把 writer 暴露给 production caller。

- [ ] **Step 5: 验证 durable transaction**

```text
TestPrepareDurableTargetReturnsOwnedSnapshot
TestCommitTargetAtomicallyReplacesCacheAndCheckpoint
TestCommitTargetFailurePreservesDurableBase
TestTargetStateCloneOwnsAllMutableFields
TestLegacyCheckpointStillLoadsIntoTargetState
TestStateInterfaceExposesOnlyReadonlyPrepareAndCommit
```

Run:

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/state/...
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/state/...
```

Expected: PASS。

- [ ] **Step 6: 提交**

```bash
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/state
git commit -m "refactor(cpu): narrow target state repository"
```

### Task 8: 删除 Legacy、Dead Code 并完成全量验证

**Files:**
- Delete: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpuset_adjustment_handler.go`
- Delete: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/util/cpuset_adjustment.go`
- Delete or rewrite: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/policy_cpuset_adjustment_test.go`
- Modify: all tests whose sole purpose is old handler/Manager/shadow-policy behavior
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/api/types.go`
- Modify: Bulkhead Manager and cpusetmems/systemservice/workqueue/cpusettopology implementations and tests
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/task6_architecture_test.go`
- Modify: `docs/superpowers/specs/2026-08-02-cpuset-materializer-rampup-owner-convergence-design.md` only if implementation proves a documented signature inaccurate

- [ ] **Step 1: 删除旧文件和仅验证旧接口的 fixture**

使用文件删除工具删除 adjustment 文件；删除仅被以下符号使用的 tests/helpers：

```text
legacy cpuset adjustment handler type
RegisterCPUSetAdjustmentHandler
RunCPUSetAdjustmentHandlers
Manager.Apply
PublishAppliedReclaim
LatestAppliedReclaim
ApplyTransientProtectedNonReclaim
```

不得留下 deprecated wrapper。

Bulkhead 私有 `Plugin` 同步收敛为语义化契约：

```go
Reconcile(context.Context, HandlerContext) error
Reset(context.Context, HandlerContext) error
```

Manager 与 cpusetmems、systemservice、workqueue、cpusettopology 的实现和测试必须全部迁移。Topology 的 typed result 边界继续使用 `ReconcileTopology`，不得退化为通用 `Reconcile` 返回值。只删除 sole purpose 是验证旧 no-op handler 的测试；其余行为测试必须迁移后保留。

- [ ] **Step 2: 执行 lingering-reference scan**

```bash
! git grep -nE \
  'newPlanningPolicy|planningState|planningRampUpAdmission|storeRequested|CPUSetAdjustment(Disabled)?Handler|RunCPUSetAdjustmentHandlers|RegisterCPUSetAdjustmentHandler|PublishAppliedReclaim|LatestAppliedReclaim|CommittedRaw|ApplyTransientProtectedNonReclaim' \
  -- 'pkg/agent/qrm-plugins/cpu/dynamicpolicy/*.go' \
  'pkg/agent/qrm-plugins/cpu/dynamicpolicy/**/*.go'
```

Expected: 无输出。

静态 contract test 必须通过 AST 分别覆盖 `TypeSpec`、`FuncDecl`、interface method field 和 `SelectorExpr`，避免旧声明或调用以不同语法形态回流。测试中的 forbidden name 使用分段字符串构造，保证 repository scan 对旧完整符号仍为零输出。

- [ ] **Step 3: 检查 production state writer**

```bash
! git grep -nE \
  'func \(.*\*TargetState\) (Set|Delete|ClearState)|\.StoreState\(' \
  -- 'pkg/agent/qrm-plugins/cpu/dynamicpolicy/**/*.go' \
  ':(exclude)**/*_test.go'
```

Expected: 无输出。

- [ ] **Step 4: 确认 Manager 只有一个物化入口**

```bash
git grep -nE \
  '^func \(m \*Manager\) (Materialize|Apply|RunCPUSetAdjustmentHandlers|PublishAppliedReclaim|LatestAppliedReclaim)' \
  -- 'pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/*.go'
```

Expected: 只命中 `Manager.Materialize`。

- [ ] **Step 5: 检查中立包依赖**

```bash
! git grep -nE \
  'dynamicpolicy|bulkhead|/state' \
  -- 'pkg/agent/qrm-plugins/cpu/dynamicpolicy/cpusetmaterializer/*.go'
```

Expected: 无输出。

- [ ] **Step 6: 全量单测、race、vet**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/...
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/...
go vet ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/...
git diff --check
```

Expected: 全部 PASS。

- [ ] **Step 7: 构建并执行 E2E**

按现有安全脚本和节点隔离规则依次执行：

```text
stable RampUp
→ delete/recreate
→ high churn
→ restart recovery
→ reset cleanup
```

每轮记录并断言：

```text
ValidateTarget success
ReclaimRaw ∩ SharePool = empty
ReclaimRaw ∩ RampUp containers = empty
Materializer Converged = true
checkpoint target == live controlled layout
failure path does not commit
reset 后 RUN_TAG Pod/state = 0
```

- [ ] **Step 8: 完成可达性审计**

对本次触及 package 的 exported symbol 搜索跨仓调用。满足以下条件时直接删除：

```text
production main path 无调用
不是外部 API
不承担 checkpoint wire
不用于反射/生成/build tag
唯一调用方是已删除 legacy 或其测试
```

- [ ] **Step 9: 提交清理与验证**

```bash
git add -A pkg/agent/qrm-plugins/cpu/dynamicpolicy
git commit -m "refactor(cpu): retire legacy cpuset adjustment paths"
```

## 最终验收

- `TargetState` 是唯一策略事实源。
- `DynamicPolicy` 是唯一 transaction owner。
- `cpusetmaterializer.Materializer` 是唯一 CPUSet 外部执行契约。
- Bulkhead Manager 只有 `Materialize` 一个物化入口。
- CpusetTopology 不修改输入 target。
- RampUp planner 一次生成 reclaim/share/container/MachineState 完整目标。
- 不存在 applied reclaim runtime cache。
- 不存在 shadow DynamicPolicy、planning mode、state writer、handler registry 或 legacy wrapper。
- 全量 test、race、vet、diff check 通过。
- stable、recreate、high-churn、restart recovery 和 reset E2E 通过。
