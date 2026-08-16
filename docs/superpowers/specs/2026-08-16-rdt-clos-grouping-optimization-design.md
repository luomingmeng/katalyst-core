# RDT CLOS ID 分组逻辑优化设计

## 文档状态

- 状态：Core 已实施
- Core 基线分支：`feat/default-share-residual-backfill`
- Core 基线提交：`7f8ccf218`
- API 改动：不需要
- 运行态迁移：不提供

## 目标与唯一契约

1. 默认 shared-core pool 的逻辑名称仍为 `share`，固定物理 CLOS 为
   `shared-50`。
2. 所有未显式映射的 shared-core pool 回落到 `shared-50`。
3. `CPUSetPoolToSharedSubgroup` 仅允许配置非默认 pool；显式 subgroup 使用
   `share-<id>`。
4. `shared-50` 与 `share-50` 是两个独立的物理 CLOS。
5. CAT key `share` 是 `shared-50` 的逻辑别名；物理 key 和合法 pool alias
   使用同一个 resolver。
6. lifecycle API 只传递一个物理 `closID`，不再维护 canonical/physical 双身份。
7. CPUList 按最终 CLOS 聚合后，在任何写入和 applied cache 更新前验证所有非空
   target 两两不相交。
8. MB monitor、advisor、plan 和 allocator 保留 reader 返回的物理 CLOS ID，
   不做全局 `shared-*`/`share-*` 前缀转换。

## 配置硬切

删除 `ResctrlConfig.DefaultSharedSubgroup`、`ResctrlOptions.DefaultSharedSubgroup`
以及 `--resctrl-default-shared-subgroup`。不保留 deprecated/no-op 兼容层。

`CPUSetPoolToSharedSubgroup` 校验规则：

- pool name 不能为空；
- pool name 不能是默认逻辑 pool `share`；
- pool name 不能占用 `dedicated`、`reclaim`、`system`、`shared-50`、
  `share-<数字>` 或 legacy `shared-<数字>` 物理命名空间；
- subgroup 必须非负；
- `share-NUMA*`、isolation pool 和 resource-package 包装 pool 合法；
- 多个 pool 可以映射到同一个 subgroup。

resolver、managed CLOS 和 CAT alias 分类同时防御绕过启动校验的直接配置构造：
`share:<id>`、负 subgroup 和保留物理 key 均不得成为有效 pool mapping。

## 名称解析

| 输入 | 输出 |
|---|---|
| 默认 pool `share` | `shared-50` |
| 未映射非默认 shared pool | `shared-50` |
| `batch:30` 的 pool `batch` | `share-30` |
| CAT key `share` | `shared-50` |
| CAT key `shared-50` | `shared-50` |
| CAT key `share-50` | `share-50` |
| legacy CAT key `shared-30` | `share-30` |

`NormalizeClosID` 必须在通用 legacy prefix 转换前精确保留
`shared-50`。`ResolveCATWayKey` 必须先识别逻辑默认 key 和保留物理 key，再处理
合法 pool alias。

## Lifecycle

```go
EnsurePendingCLOS(ctx context.Context, closID string) (ResolvedCLOS, error)

type ResolvedCLOS struct {
    ID         string
    Identity   DirectoryIdentity
    Generation uint64
    Phase      ActivationPhase
}
```

memory resctrl manager 只传一次已经解析出的物理 ID。`shared-50` 与 `share-50`
分别创建、跟踪和删除各自目录。

## CPUList 终态校验

CPUList handler 固定顺序：

1. 列举现存 CLOS；
2. 将 pool CPUSet 按最终物理 CLOS 聚合；
3. 对排序后的非空 CLOS target 做两两交集校验；
4. 校验通过后格式化 CPUList；
5. 按既有 reconcile 顺序写入；
6. 更新 applied cache。

同一最终 CLOS 内多个 pool 的 overlap 是合法 union。不同最终 CLOS overlap
返回稳定错误，例如：

```text
cpu list targets for clos "dedicated" and "shared-50" overlap on cpus "2-3"
```

target 构建或 overlap 校验失败保证零写入；设备 I/O 阶段仍不提供跨 CLOS rollback。
外部 CLOS 不读取 live CPUList，也不参与本轮预期终态校验。

## CAT

`share` 与 `shared-50` 解析到同一默认物理 CLOS：表达式相同则去重，表达式不同
则整轮冲突。`share-50` 始终是独立显式 subgroup。direct-key 优先级判断只使用
合法显式 mapping helper，非法 `share:30` 不得劫持默认 key。

## MB 物理身份

MB 链路不调用兼容输入使用的 `NormalizeClosID`。`shared-50`、`share-50` 和
legacy `shared-30` 均保留原 map key。

priority 与 advisor 统一使用等价分组键：

```go
type EquivalenceGroupKey struct {
    Weight         int
    PhysicalCLOSID string
}
```

权重先精确匹配完整 CLOS 的 `ExtraGroupPriorities`；未命中时才回落到 major
priority 与 subgroup 数字权重。`share-N` 和 `shared-N` 的 subgroup 权重都直接
使用原始数值 `N`，不通过 `2*N`、`2*N+1` 或其他前缀编码制造差异；逻辑 `share`
继续使用 subgroup 数值 `50`。`GetWeight` 对 `baseWeight + subgroupWeight` 做
显式安全加法，结果超过当前平台最大 `int` 时饱和到最大 `int`，不得发生整数溢出。

完整名称严格匹配 `share-<数字>` 或 `shared-<数字>` 的合法物理 CLOS，将完整物理
名称写入 `PhysicalCLOSID`，因此每个物理 CLOS 独占 combined bucket。物理隔离只由
`EquivalenceGroupKey` 保证，不得再借助权重数值编码。其他 group 的
`PhysicalCLOSID` 为空，仍可按相同权重合并。

`SortGroups` 与 advisor `groupByWeight` 必须调用同一个等价分组键函数，禁止一处按
权重合并、另一处按物理名称拆分。即使 `share-4000` 的计算权重与 `dedicated`
相同，且二者在同一 CCD 有流量，也必须保持两个独立 group；`shared-50` 与
`share-50` 同理。非法近似名称（例如 `share-x`、`shared-1-extra`）不获得物理
CLOS 独占桶。

## 发布与回滚门禁

Core 与 Adapter 必须同批硬切。发布前删除 Adapter 和部署系统中的旧 flag，
审计 CAT、mon_group、DefaultClosIDs、SkipCleanupClosIDs、Sandbox 和所有 MB
精确 CLOS 配置。停止全部 resctrl writer 后排空并删除旧默认 CLOS；确认目标
`shared-50` 所有权和占用状态，不能确认时停止升级。

旧版本无法识别 `shared-50` 与 `share-50` 的新语义。回滚前必须再次停止 writer、
排空并删除 `shared-50`，然后原子恢复旧二进制、参数和配置快照。

## 取代关系

本设计取代
`2026-08-14-cat-restricted-prefix-selector-design.md` 中 shared CLOS naming、
canonical/physical binding、canonical collision 和 CPUList scope 契约；该文档
其他 CAT policy 内容继续作为历史设计背景。
