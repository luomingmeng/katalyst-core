# Versioned Frozen Boundary Design

## Status

方案 A 已批准。本设计替代 `ScanBoundary.ExpandedRels` 作为 frozen trace
preflight/finalization 一致性所有者的现状。`ExpandedRels` 只保留为一次快照扫描的诊断信息，
不得再决定后续扫描范围、快照指纹或 trace 是否陈旧。

## Problem

当前 frozen trace 将初始 `ScanBoundary.ExpandedRels` 原样重放，并用完整
`SnapshotID` 判断 preflight/finalization 是否匹配。该模型把“编译时恰好扫描过的动态目录”
误当成执行契约：

- 无关 dynamic sibling 创建、删除或变化会改变 expanded set 或完整快照指纹，导致无意义重编译；
- exact replay 可能因为一个已消失的无关目录直接失败；
- preflight 与 finalization 分别手写比较逻辑，边界语义可能漂移；
- snapshot builder 事实上拥有 admission 边界，而不是 trace compiler。

## Required Outcome

引入由 trace compiler 唯一生成的版本化 `FrozenBoundary`。同一个 evaluator 在首写前和
最终发布前读取当前层级，并只比较执行相关证据：

1. controlled relation 的身份和 CPU/mems 状态；
2. controlled relation 的直接子项集合和身份；
3. 持有 relevant CPU 的动态 relation 的身份和 CPU/mems 状态；
4. 当前新出现的 relevant CPU holder。

无关 dynamic sibling churn 必须有效；direct child、controlled relation 或 relevant CPU
holder 的创建、删除、身份或资源状态变化必须 fail-closed。

## Ownership

唯一所有权链为：

```text
coordinatorRound.compileFixedPointTrace
  -> CompileFrozenBoundary
  -> CompiledPhaseTrace.FrozenBoundary
  -> EvaluateFrozenBoundary（preflight 与 finalization 共用）
```

`snapshotBuilder` 只负责采集证据。它可以记录 `ExpandedRels` 供日志和排障使用，但不得：

- 从 `ExpandedRels` 重放扫描；
- 把 `ExpandedRels` 写入 `SnapshotID`；
- 用 expanded set 相等作为陈旧判据；
- 决定哪些 relation 属于 frozen execution contract。

## Contract

```go
type FrozenBoundaryVersion uint8

const FrozenBoundaryVersionV1 FrozenBoundaryVersion = 1

type FrozenBoundary struct {
    Version             FrozenBoundaryVersion
    Roots               []string
    ControlledRels      []string
    DirectChildrenByRel map[string][]ChildRef
    RelevantCPUHolders  []string
    RelevantCPUs        machine.CPUSet
}
```

V1 语义：

- `Roots` 是 evaluator 每次重新采集的根，来自 compiler 输入快照的 plan roots；
- `ControlledRels` 来自冻结 DAG，且必须全部存在于 expected snapshot；
- `DirectChildrenByRel` 是每个 controlled rel 的直接子项身份集合；
- `RelevantCPUs` 是所有 operation 的 expected/target CPU、required CPU、pending/protected
  CPU、动态 expected CPU 和 parent-safety target CPU 的并集；
- `RelevantCPUHolders` 是 expected snapshot 中非 controlled 且 effective CPU 与
  `RelevantCPUs` 相交的 relation；
- 所有 slice 和 map 必须规范排序并深拷贝；
- 未知版本必须拒绝。

边界由 compiler 产生，并被冻结进 `TraceID`。调用方不能提供或修补边界。

## Shared Evaluator

```go
type FrozenBoundaryEvaluation struct {
    Snapshot *CompleteSnapshot
}

func EvaluateFrozenBoundary(
    ctx context.Context,
    driver HierarchyDriver,
    dag *TopoDAG,
    budget *BudgetTracker,
    boundary FrozenBoundary,
    expected *CompleteSnapshot,
) (FrozenBoundaryEvaluation, error)
```

evaluator 必须：

1. 校验边界版本和规范形态；
2. 从 `Roots` 做 fresh、非 exact-replay 的完整扫描；
3. 比较每个 controlled rel 的存在性、identity、configured/effective CPU 与 mems；
4. 比较每个 controlled rel 的直接 child name+identity 集合；
5. 比较每个冻结 relevant holder 的存在性、identity、configured/effective CPU 与 mems；
6. 在 fresh snapshot 中重新计算 relevant holders，发现新增 holder 时拒绝；
7. 返回 fresh snapshot，供调用者继续做 trace 初态投影或最终 ParentSafe 证明。

preflight 和 finalization 都必须调用此 evaluator。两者只在 expected snapshot
（initial/final）及后续动作上不同，不得各自实现第二套边界比较。

## Churn Semantics

| 变化 | 结果 |
|---|---|
| 非 controlled、非 direct child、且不持有 relevant CPU 的 dynamic sibling 创建/删除/状态变化 | 允许 |
| controlled rel 创建、删除、identity、configured/effective CPU 或 mems 变化 | fail-closed |
| controlled rel 的直接 child 集合或 child identity 变化 | fail-closed |
| 冻结 relevant CPU holder 创建、删除、identity 或 CPU/mems 状态变化 | fail-closed |
| 新 dynamic relation 开始持有 relevant CPU | fail-closed |
| `ExpandedRels` 变化但上述语义证据不变 | 允许 |

## Snapshot Changes

- 删除 `BuildCompleteSnapshotForBoundary`。
- 删除 `snapshotBuilder.exactExpansion` 与
  `ErrSnapshotBoundaryExpansionMismatch`。
- 普通 plan 扫描继续记录 `ScanBoundary.ExpandedRels`，仅用于诊断。
- `fingerprintSnapshot` 不再哈希 `ExpandedRels`，但继续哈希实际 entries、children、
  unavailable evidence、domain 和 capabilities。
- `validateCompleteSnapshotEvidence` 不再根据 `ExpandedRels` 推断 children evidence
  完整性。

## Trace Freeze and Hashing

`CompiledPhaseTrace` 增加 `FrozenBoundary FrozenBoundary`。`FreezePhaseTrace` 必须：

- 拒绝零值或未知版本；
- 深拷贝并规范化边界；
- 验证 controlled rel、direct-child evidence 和 relevant holder 都能由 initial snapshot
  证明；
- 验证边界等于 compiler 从 frozen inputs 推导出的规范结果，防止第二 owner；
- 将完整边界写入 `TraceID`。

## Failure Semantics

- preflight boundary mismatch：首写前返回 `frozenInitialSnapshotDriftError`，零物理写；
- operation 间 drift：维持现有 predecessor 检查和完整前缀 rollback；
- finalization boundary mismatch：完整 rollback 后才允许 replan；
- evaluator 读取失败、版本未知、证据不完整：一律 fail-closed；
- 不引入 WAL、checkpoint/schema、feature flag 或部署步骤。

## Verification

必须通过：

- RED 证明无关 dynamic sibling churn 当前会失败；
- RED 覆盖 direct child、controlled rel、existing/new relevant holder drift；
- preflight 与 finalization 的 evaluator parity 测试；
- trace freeze isolation、版本拒绝、确定性 hash；
- topology 全包；
- cpusettopology 全包；
- topology race；
- scale correctness 与 benchmark；
- `git diff --check`；
- 不部署。

## Anti-Entropy Declaration

- Old owner: `ScanBoundary.ExpandedRels` exact replay 和完整 `SnapshotID` equality。
- New owner: compiler 生成的 `FrozenBoundaryV1`。
- Shared policy owner: `EvaluateFrozenBoundary`。
- Diagnostic-only field: `ScanBoundary.ExpandedRels`。
- Delete-first targets:
  `BuildCompleteSnapshotForBoundary`、`exactExpansion`、
  `ErrSnapshotBoundaryExpansionMismatch` 及对应 exact-replay 测试。
