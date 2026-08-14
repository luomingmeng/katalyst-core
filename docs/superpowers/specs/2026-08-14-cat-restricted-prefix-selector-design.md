# CAT 受限前缀选择器、独占 CLOS 与安全事务设计

## 文档状态

- 状态：设计已确认，待实施
- 日期：2026-08-14
- Core 基线：`feat/default-share-residual-backfill`，`f72add39e`
- API 基线：`feat/default-share-residual-backfill-api`，`fd8edbbf6`
- 范围：`katalyst-api`、`katalyst-core`、`katalyst-adapter`、AQC admission、qrm-plugin 部署仓库和真实 task migration 消费端
- 取代：
  - `2026-08-13-cat-ways-expression-policy-design.md` 中的 allocation group 设计
  - `2026-08-14-cat-non-overlap-constraints.md`
  - 本文档上一版 `NonOverlapConstraints` 设计

当前 Core 已完成用户态 CAT expression operand 硬切换，只接受
`MaxCATWays`、`MinCATWays` 和正整数，不接受 `CBMMask`、`MinCBMBits`
兼容别名。

本设计：

- 将所有 CAT 字段收敛到 `CATPolicy`
- 删除未发布的 `allocationGroups`
- 暂不引入 `NonOverlapConstraints`
- 使用 `exclusiveClosIDs` 表达当前 non-overlap 需求
- ways 和 placement 继续显式配置
- ways/placement map key 支持受限尾部 `*`
- `EnableCAT`、`DefaultCATWays`、`ClosCATWays`、`ExclusiveClosIDs` 和
  `DefaultPlacement` 支持 flags/env startup fallback
- AQC 使用字段级三态覆盖 startup fallback
- 增加 active ownership、activation、safe transition、transaction restore
  和 periodic drift repair

在 API、admission、Core 生命周期、transaction、task migration gate 和
deployment gate 全部完成前，不得下发包含
`share-*` 或 `exclusiveClosIDs` 的 AQC。

## 核心目标

- 一个 `share-*` 规则覆盖静态配置产生的所有 `share-XX` CLOS。
- ways 和 placement 共用同一 selector parser/resolver。
- `exclusiveClosIDs` 中每个 CLOS 与全部其他 configured CLOS 不重叠。
- `exclusiveClosIDs` 只生成 edge，不自动生成 ways 或 placement。
- AQC 可覆盖 flags/env，显式空列表可以关闭 startup exclusive 策略。
- desired target 与 map、目录和 ownership 返回顺序无关。
- configured、owned、active 和 physical 目录严格分层。
- CAT 和 task migration token 共用同一个 immutable
  canonical-to-physical binding。
- enabled 模式中新 CLOS 在 CAT apply/readback 成功前不能承载 task。
- disabled 模式在所有 managed configured/owned non-root CLOS 收敛到
  `CBMMask` 后才发布 reset epoch。
- 在线迁移每一步都保持 active constrained CLOS 无 overlap。
- unsafe transition 在首次真实写入前失败。
- 中途失败恢复每个 CLOS 的精确 before-image。
- 同名重建不能复用旧 identity/generation/activation。
- periodic repair 读取真实 schemata，不能被 target cache 短路。
- root schemata 只读观察，异常时 fail-closed。
- API server、deployment pipeline 和 Core 各自 fail-closed。

## 非目标

- 任意 glob 或正则 selector。
- wildcard pool alias。
- 任意 pairwise constraint graph。
- `exclusiveClosIDs` 自动计算 ways 或 placement。
- 不重启进程动态修改静态 `CPUSetPoolToSharedSubgroup`。
- 跨节点 CAT 协调。
- 依赖内核提供多 CLOS 原子写。
- enabled 或 disabled 模式接管、激活或修改任意 foreign unowned resctrl 目录。
- 由当前 schemata 反向推导 desired allocation。
- 由 CAT plugin 自动修复 root schemata。

当前 schemata 只用于 drift、安全迁移、disabled convergence、before-image、
readback 和 restore。
desired target 是 immutable CAT policy、configured CLOS 和硬件 capability 的
纯函数。

## 用户态与硬件术语

| 层级 | 最大 CAT ways | 最小连续 CAT ways |
|---|---|---|
| AQC/flag expression | `MaxCATWays` | `MinCATWays` |
| Core capability | `CATCapability.CBMMask` | `CATCapability.MinCBMBits` |
| resctrl 文件 | `cbm_mask` | `min_cbm_bits` |

`CBMMask` 和 `MinCBMBits` 不能出现在用户态 expression 中。

## API 模型

### BulkheadRDTConfig

```go
type BulkheadRDTConfig struct {
	EnableCPUList *bool      `json:"enableCPUList,omitempty"`
	CATPolicy     *CATPolicy `json:"catPolicy,omitempty"`
}
```

### CATPolicy

```go
type CATPolicy struct {
	EnableCAT *bool `json:"enableCAT,omitempty"`

	DefaultCATWays *intstr.IntOrString           `json:"defaultCATWays,omitempty"`
	ClosCATWays    *map[string]intstr.IntOrString `json:"closCATWays,omitempty"`

	ExclusiveClosIDs *[]string `json:"exclusiveClosIDs,omitempty"`

	DefaultPlacement *CATPlacementPolicy           `json:"defaultPlacement,omitempty"`
	ClosPlacements   map[string]CATPlacementPolicy `json:"closPlacements,omitempty"`
}
```

所有 CAT-owned 字段都位于 `CATPolicy`。删除旧扁平路径：

```text
bulkheadRDTConfig.enableCAT
bulkheadRDTConfig.defaultCATWays
bulkheadRDTConfig.closCATWays
```

删除：

```text
CATAllocationGroup
CATPolicy.AllocationGroups
CATNonOverlapConstraint
CATPolicy.NonOverlapConstraints
```

该功能仍处于未发布 feature branch，不保留旧 AQC shape 兼容 adapter。实施前
必须清查集群中的持久化 AQC，发现旧字段时先禁用或迁移。

## flags/env 模型

支持 startup fallback：

```text
--enable-bulkhead-cat=<bool>
--bulkhead-default-cat-ways=<expression>
--bulkhead-clos-cat-ways=<selector=expression,...>
--bulkhead-cat-exclusive-clos-ids=<id,id,...>
--bulkhead-cat-default-allowed-bit-usages=<*,S,X,S,X>
--bulkhead-cat-default-direction=<low|high>
```

对应 env：

```text
QRMCPUPluginEnableBulkheadCAT
QRMCPUPluginBulkheadDefaultCATWays
QRMCPUPluginBulkheadClosCATWays
QRMCPUPluginBulkheadCATExclusiveClosIDs
QRMCPUPluginBulkheadCATDefaultAllowedBitUsages
QRMCPUPluginBulkheadCATDefaultDirection
```

Adapter 映射：

```bash
param_map["QRMCPUPluginEnableBulkheadCAT"]="enable-bulkhead-cat"
param_map["QRMCPUPluginBulkheadDefaultCATWays"]="bulkhead-default-cat-ways"
param_map["QRMCPUPluginBulkheadClosCATWays"]="bulkhead-clos-cat-ways"
param_map["QRMCPUPluginBulkheadCATExclusiveClosIDs"]="bulkhead-cat-exclusive-clos-ids"
param_map["QRMCPUPluginBulkheadCATDefaultAllowedBitUsages"]="bulkhead-cat-default-allowed-bit-usages"
param_map["QRMCPUPluginBulkheadCATDefaultDirection"]="bulkhead-cat-default-direction"
```

`ClosCATWays` 使用现有 `selector=expression` 逗号分隔格式支持 flags/env：

```text
--bulkhead-clos-cat-ways='clos-a=MinCATWays,group-*=MaxCATWays-MinCATWays'
QRMCPUPluginBulkheadClosCATWays="clos-a=MinCATWays,group-*=MaxCATWays-MinCATWays"
```

`ClosPlacements` 保持 AQC-only，避免在 env 中表达嵌套 map。

## Startup 默认值

```text
EnableCAT = false
DefaultCATWays = 未设置
ClosCATWays = {}
ExclusiveClosIDs = []
DefaultPlacement.AllowedBitUsages = "*"
DefaultPlacement.Direction = low
```

默认 flag：

```text
--bulkhead-cat-default-allowed-bit-usages='*'
--bulkhead-cat-default-direction=low
```

默认 env：

```text
QRMCPUPluginBulkheadCATDefaultAllowedBitUsages="*"
QRMCPUPluginBulkheadCATDefaultDirection="low"
```

`*` 在 placement 中不是 selector，而是“不按 bit_usage 过滤”：

```text
allowed mask = hardware CBMMask
```

内部规范化为：

```go
CATPlacementPolicy{
	AllowedBitUsages: nil,
	Direction:        CATDirectionLow,
}
```

flag/env 校验：

```text
*     合法
S     合法
X     合法
S,X   合法
*,S   非法
空值  非法
重复  非法
未知  非法
```

`*` 必须单独出现。命令行使用时应引用 `'*'`，避免 shell 展开。

## AQC 覆盖语义

### CATPolicy object

```text
AQC catPolicy 未设置
  -> 继承完整 startup CATPolicy

AQC catPolicy 已设置
  -> 对内部字段执行字段级 optional merge
```

### Scalar/pointer

```text
enableCAT/defaultCATWays 未设置
  -> 继承 startup

已设置
  -> 替换 startup
```

### ExclusiveClosIDs

```text
AQC exclusiveClosIDs 未设置
  -> 继承 flags/env

AQC exclusiveClosIDs 非空
  -> 完整替换 flags/env

AQC exclusiveClosIDs: []
  -> 关闭 exclusive 策略
```

不做 union。

### ClosCATWays

```text
AQC closCATWays 未设置
  -> 继承 flags/env map

AQC closCATWays 非空
  -> 完整替换 flags/env map

AQC closCATWays: {}
  -> 清除 flags/env per-CLOS map，仅使用 defaultCATWays fallback
```

不做按 key merge，防止 startup 中已删除的 selector 无法通过 AQC 移除。

API 使用 pointer-to-map 保留三态：

```text
nil pointer              -> 字段未设置
pointer to non-empty map -> 显式替换
pointer to empty map     -> 显式清除
```

不能使用 `map[string]intstr.IntOrString,omitempty`，否则显式空 map 在序列化
链路中可能被省略。

### DefaultPlacement

```text
AQC defaultPlacement 未设置
  -> 继承 flags/env

AQC defaultPlacement 已设置
  -> 整体替换 flags/env
```

AQC object 内缺失子字段使用内建默认：

```text
allowedBitUsages = "*"
direction = low
```

因此：

```yaml
defaultPlacement: {}
```

等价于：

```yaml
defaultPlacement:
  allowedBitUsages: ["*"]
  direction: low
```

AQC 允许：

```yaml
allowedBitUsages: ["*"]
allowedBitUsages: []
allowedBitUsages: ["S"]
allowedBitUsages: ["S", "X"]
```

`["*"]` 和 `[]` 都规范化为不做 bit_usage 过滤；`"*"` 与其他值组合非法。

### ClosPlacements

```text
closPlacements 未设置
  -> 使用空 map

已设置
  -> 使用 AQC map
```

startup 不提供 per-CLOS placement map。

## 内部配置与原子转换

```go
type OptionalExclusiveClosIDs struct {
	Specified bool
	Values    []string
}

type OptionalCATPlacement struct {
	Specified bool
	Value     CATPlacementPolicy
}

type OptionalClosCATWays struct {
	Specified bool
	Values    map[string]CATWaysExpression
}
```

API 使用 pointer-to-slice 和 pointer-to-map 保留三态：

```text
nil pointer                  -> 字段未设置
pointer to non-empty value   -> 显式替换
pointer to empty slice/map   -> 显式关闭或清除
```

不能对这两个字段使用普通 `[]string,omitempty` 或
`map[string]intstr.IntOrString,omitempty`，否则显式空值在序列化链路中可能
被省略，丢失“关闭 exclusive 策略”或“清除 startup ways map”的语义。

```go
func ConvertBulkheadRDTConfig(
	base StartupBulkheadRDTConfig,
	aqc *configv1alpha1.BulkheadRDTConfig,
) (DynamicBulkheadRDTConfiguration, error)
```

转换顺序：

1. 复制 startup CATPolicy。
2. merge AQC optional 字段。
3. 解析 default/per-CLOS expression。
4. 解析 default/per-CLOS placement。
5. 解析 `exclusiveClosIDs`。
6. 校验完整 policy。
7. 一次性替换 runtime snapshot。

任一失败：

- 不修改旧 snapshot。
- 保存完整 conversion error。
- reconcile 继续使用上一份有效 snapshot。
- AQC status/health 反映失败。

禁止逐字段部分提交。

## Selector 语法

selector 只用于：

- `closCATWays` map key
- `closPlacements` map key

语法：

```text
exact selector  := 不含空白和 * 的非空字符串
prefix selector := exact selector 后追加一个 *
```

合法：

```text
dedicated
aa
share-50
share-*
```

非法：

```text
*
*-50
share-**
share-*foo
foo*bar
```

不支持完整 glob 或正则。

通用 parser：

```go
type TrailingPrefixSelectorKind string

const (
	TrailingPrefixSelectorExact  TrailingPrefixSelectorKind = "exact"
	TrailingPrefixSelectorPrefix TrailingPrefixSelectorKind = "prefix"
)

type TrailingPrefixSelector struct {
	Raw   string
	Kind  TrailingPrefixSelectorKind
	Value string
}

func ParseTrailingPrefixSelector(raw string) (TrailingPrefixSelector, error)
```

CAT conversion 使用强类型 wrapper。

## `exclusiveClosIDs` 语义

只接受 exact canonical configured CLOS ID：

```text
sandbox
dedicated
share-40
xxx
```

不接受：

```text
share-*
pool alias
obsolete physical ID
foreign directory
/
```

若 configured CLOS 为：

```text
sandbox
xxx
dedicated
share-40
share-50
```

且：

```yaml
exclusiveClosIDs:
  - sandbox
  - xxx
```

生成：

```text
sandbox -- xxx
sandbox -- dedicated
sandbox -- share-40
sandbox -- share-50
xxx -- dedicated
xxx -- share-40
xxx -- share-50
```

不生成：

```text
dedicated -- share-40
dedicated -- share-50
share-40 -- share-50
```

算法：

```go
for _, exclusive := range exclusiveClosIDs {
	for _, other := range sortedConfiguredClosIDs {
		if exclusive == other {
			continue
		}
		addCanonicalEdge(exclusive, other)
	}
}
```

每条 edge 规范化为 `(min(a,b), max(a,b))` 并自动去重。多个 exclusive
CLOS 之间自然互相 non-overlap。

`exclusiveClosIDs` 声明顺序作为 graph priority 的第一部分，其余 configured
CLOS 按 canonical ID 排序。

`exclusiveClosIDs` 不修改 ways 或 placement。没有足够 ways 时 policy
整体失败并保持上一份 active target。

## Configured、owned 与 active CLOS

### Configured canonical CLOS

```go
func BuildConfiguredClosIDs(config *qrmresctrl.ResctrlConfig) sets.Set[string]
```

来源：

- `EnabledQoS`
- `DefaultClosIDs`
- 非负 `DefaultSharedSubgroup`
- `CPUSetPoolToSharedSubgroup` 的唯一 subgroup

SharedCores 归一化：

```text
DefaultSharedSubgroup >= 0
  -> share-<subgroup>

DefaultSharedSubgroup < 0
  -> share
```

不能同时生成 `share` 和 `share-50`。

示例：

```text
EnabledQoS = dedicated_cores,shared_cores
DefaultSharedSubgroup = 50
CPUSetPoolToSharedSubgroup = {"aa": 40, "bb": 40}
DefaultClosIDs = {"sandbox"}
```

得到：

```text
dedicated
sandbox
share-40
share-50
```

### 集合分层

```text
configured CLOS = 仅由静态配置推导
observed CLOS   = 一次物理目录 snapshot 中的 physical ID/identity
pending CLOS    = ownership v5 中 phase=pending 且 identity/generation 有效
active CLOS     = ownership v5 中 phase=active 且 identity/generation 有效
target universe = configured CLOS
activation set  = configured CLOS ∩ pending CLOS
repair set      = configured CLOS ∩ active CLOS
```

prefix 只匹配 configured canonical CLOS，不扫描目录、不匹配 alias、不扩大
ownership。

### Physical 与 canonical

```go
type ResolvedCLOS struct {
	CanonicalID string
	PhysicalID  string
	Identity    rdt.DirectoryIdentity
	Generation  uint64
	Phase       ActivationPhase
}
```

resolver 必须从一次固定的 observed snapshot 构造：

```text
canonical ID -> ResolvedCLOS
physical ID  -> canonical ID
```

若两个 physical 目录映射到同一 canonical ID，整轮 fail-closed。transaction
开始后不得重新按字符串解析 physical ID。

Kubelet 创建的 shared subgroup 物理目录使用 `shared-XX`，用户配置和策略使用
canonical `share-XX`：

```text
canonical ID = share-50
physical ID  = shared-50
```

将现有命名从“obsolete compatibility”改为明确的物理边界：

```go
const (
	ResctrlShareSubgroupPrefix       = "share-"
	ResctrlPhysicalSharedGroupPrefix = "shared-"
)

func CanonicalClosIDFromPhysical(physicalID string) string {
	if strings.HasPrefix(physicalID, ResctrlPhysicalSharedGroupPrefix) {
		return ResctrlShareSubgroupPrefix +
			strings.TrimPrefix(physicalID, ResctrlPhysicalSharedGroupPrefix)
	}
	return physicalID
}

func PreferredPhysicalClosID(canonicalID string) string {
	if strings.HasPrefix(canonicalID, ResctrlShareSubgroupPrefix) {
		return ResctrlPhysicalSharedGroupPrefix +
			strings.TrimPrefix(canonicalID, ResctrlShareSubgroupPrefix)
	}
	return canonicalID
}
```

规则：

- AQC、flags/env、selector、graph 和 ownership canonical ID 只使用 `share-*`。
- `shared-*` 只允许作为 kubelet 创建的 physical ID。
- `enableCAT=true` 通过 `ResolvedCLOS` 写 `shared-50`。
- graph、selector、target 和 ownership key 使用 canonical ID。
- lock、directory FD、schemata、tasks、readback 和 restore 使用绑定的
  physical ID/identity。
- 同时存在 `share-50` 和 `shared-50` 时产生 canonical collision，enabled
  reconcile fail-closed，不能激活任意一个。
- 不保留 `ResctrlObsoleteSharedSubgroupPrefix`、`NormalizeClosID` 这类把物理
  边界描述为 legacy fallback 的命名和双向兼容路径。

### CPUListPlugin scope

本期不实现 CPUListPlugin 的 canonical `share-XX` 到 physical `shared-XX/cpus`
兼容。发布门禁要求最终 AQC `enableCPUList=false`；若目标配置启用 CPUList，
selector/exclusive CAT AQC 必须拒绝发布。

CPUListPlugin 的 physical binding 和 lifecycle 统一留作独立后续方案，不在本期
CAT 验收中保留半完成代码。

缺失目录只能通过共享 lifecycle API 创建：

```go
type CLOSLifecycleService interface {
	EnsurePendingCLOS(
		ctx context.Context,
		canonicalID string,
		preferredPhysicalID string,
	) (ResolvedCLOS, error)
	DeleteCLOS(ctx context.Context, canonicalID string) error
	Recover(ctx context.Context) error
}
```

该 API 在统一 coordinator 锁内执行 pending 落盘、mkdir/外部目录观察、identity
绑定，CAT plugin 不得自行 `Mkdir`。memory resctrl manager 和 CAT plugin 复用
同一个 lifecycle service。

现有 memory resctrl manager 的 `Create`/`ReconcileClos` 也必须委托同一个
lifecycle service，并删除私有 `createClosLocked` 创建事务。生产代码只允许
lifecycle service 执行 CLOS mkdir、identity bind 和 phase transition。

`DeleteCLOS` 必须持久化 pending-delete，验证 `LifecycleOwned`、
identity/generation，并确认 `tasks`、`cpus`、`mon_groups` 全部为空后才能删除
同一 incarnation，再原子移除 record。
外部/kubelet CLOS 不得删除，只撤销 activation 或 quarantine。`Recover` 覆盖
pending-create/pending-delete 的 crash recovery；恢复 pending-delete 时必须重新
执行完整 emptiness predicate，不能沿用崩溃前结果。

## Selector 优先级

ways：

```text
direct canonical
  > exact pool alias
  > one prefix selector
  > defaultCATWays
```

placement：

```text
direct canonical
  > exact pool alias
  > one prefix selector
  > defaultPlacement
```

统一 candidate resolver：

- direct canonical 覆盖 alias 和 prefix。
- 被覆盖 candidate 不参与冲突判断。
- 同级多个 alias 解析到同一 canonical：
  - 配置相同合并
  - 配置不同拒绝
- 一个 CLOS 命中多个同级 prefix 时拒绝。
- prefix 在 configured 集合必须至少命中一个。
- 展开结果按 canonical ID 排序。

`exclusiveClosIDs` 不经过 alias resolver。

## DefaultCATWays

`defaultCATWays` 是未命中 `closCATWays` 的 non-root configured CLOS fallback。

例如：

```yaml
defaultCATWays: MaxCATWays-MinCATWays
closCATWays:
  sandbox: MinCATWays
```

得到：

```text
sandbox   -> MinCATWays
dedicated -> MaxCATWays-MinCATWays
share-40  -> MaxCATWays-MinCATWays
share-50  -> MaxCATWays-MinCATWays
```

约束：

- root 不使用 `defaultCATWays`。
- `defaultCATWays` 不生成 edge。
- `defaultCATWays` 不设置 placement。
- transaction restore 不使用 `defaultCATWays`。
- `resetCATPolicy` 不使用 `defaultCATWays`。
- 缺少 default 且某 CLOS 未命中 per-CLOS rule 时 policy fail-closed。

## Conflict graph

```go
type CATConflictGraph struct {
	Neighbors map[string]sets.Set[string]
	Priority  []string
}
```

构建：

1. 按声明顺序校验 exclusive IDs。
2. 每个 exclusive 与所有其他 configured CLOS 建 edge。
3. edge 规范化并去重。
4. priority 先追加 exclusive 声明顺序。
5. 其余 configured CLOS 按 canonical ID 排序追加。

没有 `exclusiveClosIDs` 时 graph 无 edge，CLOS 之间允许 overlap。

## Desired target builder

target builder 是纯函数，不写 resctrl。

每个 domain：

1. 从 `CATCapability.CBMMask` 开始。
2. 按 placement 的 bit usage 过滤。
3. 移除已分配 neighbor mask。
4. 计算 way expression。
5. 按 direction 选择连续 mask。
6. 保存 desired target。

`allowedBitUsages="*"` 或规范化后的 nil 表示不执行步骤 2 的过滤。

构建后验证：

```text
targets[a][domain] & targets[b][domain] == 0
```

future configured CLOS 在目录创建前已进入 target universe，避免新增
`share-40` 改变 existing target。

## Root 安全语义

Core 不写 root schemata。

transaction 前置：

```text
observed root L3 == hardware CBMMask
```

不相等时：

- CAT health not-ready
- 不开始 non-root transaction
- 不激活新 CLOS
- 记录 observed/expected

Core 不自动修复 root。所有 selector 和 exclusive ID 拒绝 root。

## API 准入

目标集群已实测不执行当前 CRD 的 `x-kubernetes-validations`，CEL 不能作为
唯一 enforcement boundary。

### OpenAPI 基础校验

- item pattern
- min/max items
- `uniqueItems`
- enum
- required
- scalar range

### Validating admission

- selector map key 语法
- exclusive canonical exact 语法
- nil/empty/omitted 三态合法性
- `*` 与其他 bit usage 组合拒绝
- allocation group/旧 flat CAT shape 拒绝
- expression 和 placement 复杂校验

admission 要求：

- fail-closed
- 部署和证书进入实施范围
- readiness 可观测
- server-side dry-run 覆盖
- admission 不可用时 selector AQC 无法提交

admission 不判断 `exclusiveClosIDs` 是否属于某个节点的 configured CLOS，因为
节点静态配置不在 AQC 请求中。membership、prefix zero/multi-match 和 physical
collision 由每个节点 Core 基于本机 snapshot fail-closed；若 deployment gate
需要下发前证明 membership，必须按目标节点配置族运行离线 resolver。

Core 重复 defensive validation，覆盖 flags/env 和内部调用。

## Stateless lifecycle 与 activation

```go
type ActivationPhase string

const (
	ActivationActive ActivationPhase = "active"
)

type ResolvedCLOS struct {
	CanonicalID string
	PhysicalID  string
	Identity    rdt.DirectoryIdentity
	Generation  uint64
	Phase       ActivationPhase
}
```

`ResolvedCLOS` 只描述本轮 reconcile 观测到的 physical directory binding。
不持久化 CLOS ownership，不写额外 lifecycle state 文件，不从通用 QRM state
directory 派生 lifecycle path。终态由 Katalyst configured/default CLOS 集合和当前
resctrl snapshot 共同决定。

创建状态机：

1. 获取统一 transaction/lifecycle 上下文。
2. Core lifecycle service 创建缺失 CLOS，或观察已存在目录。
3. 绑定 canonical ID、physical ID 和 directory identity。
4. 获取预计算 CAT target。
5. 写 CAT。
6. readback。
7. 本轮 reconcile 发布 migration token。

失败时不发布 token；下一轮通过 fresh resctrl read-back 重新收敛。删除 CLOS 时
使用 physical ID，并确认 `tasks`、`cpus`、`mon_groups` 全部为空；
`disableRDT=true` 路径按 Katalyst 终态删除所有非 skip CLOS。

启动恢复：

- 不读取额外 lifecycle state
- 枚举 configured/default/observed CLOS
- 重新绑定 identity 并执行 readback

## Coordinator、锁序与 directory FD

复用并扩展真正的 RDT coordinator：

```text
pkg/util/external/rdt/schemata.go
```

cpuset `bulkhead/utils/topology/TopologyCoordinator` 不参与 CAT transaction。
CAT、MBA、CPUList、lifecycle、readback 和 restore 必须复用
`SchemataCoordinator.closMu` 和 cache invalidation。

唯一锁序：

```text
global CAT reconcile lock
  -> immutable config snapshot
  -> sorted physical CLOS locks
  -> ownership reload/validation
  -> directory FD
  -> read/apply/readback/restore
```

禁止从已持有单 CLOS lifecycle lock 的 callback 再进入 global transaction。
activation 使用 already-locked primitive。

transaction：

1. 获取全部 sorted physical CLOS locks。
2. 调用上层 `validate` closure。
3. 通过后打开 physical CLOS directory FD。
4. `fstat` 复核 dev/inode。
5. 通过 `openat` 打开 `schemata`。
6. read/apply/readback/restore 使用 FD。
7. 写前和写后复核 identity。

最终实现不引入独立 physical transaction API。CAT 的 canonical/physical 绑定由
`ResolvedCLOS` 和当前 resctrl 快照在每轮 reconcile 中重新计算；schemata 写入继续
通过 `SchemataCoordinator` 的 per-CLOS RMW 路径完成。该选择避免在无状态设计中
保留未接入的第二事务层。

## Safe transition

### 纯规划

```go
func PlanCATTransition(
	current CATTargets,
	desired CATTargets,
	graph CATConflictGraph,
) ([]string, error)
```

只在 current 副本模拟完整顺序。没有完整安全顺序时：

```text
ApplyCAT 调用次数 == 0
```

规划阶段禁止写入、activation 或 cache mutation。

### 事务执行

1. 获取全部 physical locks 后、打开 CLOS FD 前执行 ownership/phase/generation
   validator。
2. 通过后打开 handle 并读取全部 before-image。
3. 复核 handle identity。
4. 验证 current 与 planner 输入一致。
5. 按 immutable order 写入。
6. 每步 readback。
7. 失败时停止。
8. 对成功修改项逆序执行 compare-before-restore。
9. 仅当 identity/generation 未变化且 current L3 等于该项 last-written L3 时
   恢复 before-image；否则跳过并报告 concurrent mutation。
10. restore 只修改 L3，保留并发 MB 更新。
11. 聚合 apply/restore/concurrent mutation error。

`restoreCATBeforeImages` 与 disabled managed reset 分离。

## Disabled managed reset

`enableCAT=false` 时不构建 ways、placement、exclusive graph 或 configured
target。Core 枚举 `/sys/fs/resctrl` 下一级 non-root control group，但只将
managed configured/owned candidate 的每个 L3 domain 重置到硬件 `CBMMask`。

一次 convergence session 在首轮前固定 root directory FD，并跨所有轮次持有。
每轮开始和发布 disabled-ready epoch 前都验证：

```text
每轮 reset 从当前 resctrl 快照重新计算
当前 root path/mount identity 与 pinned handle 相同
所有 root L3 domain == capability CBMMask
```

首轮写入前失败时零写 non-root。reset 后或发布前失败时撤销 epoch并 not-ready，
不能再宣称零写入；关闭旧 session，下一次 reconcile 从新的 root handle 重试。

candidate 集合：

```text
observed physical CLOS
∩
(
  IsManagedClosID(physicalID, staticConfig)
  ∪
  valid ownership v5 physical record
)
```

其中：

```text
configured set = canonical ID，例如 share-50
observed set   = physical ID，例如 shared-50
owned record   = canonical + physical + identity + generation
```

`IsManagedClosID` 必须复用 `CanonicalClosIDFromPhysical`，因此配置包含 subgroup
`50` 时 physical `shared-50` 会归一化为 canonical `share-50` 并进入 candidate。
已从静态配置删除但仍有有效 ownership record 的 obsolete CLOS 也进入 candidate。

排除：

```text
root
foreign unowned CLOS
info
mon_groups
mon_data
非一级目录
没有 schemata 的目录
```

reset 只修改 CAT L3 字段，保留 MB 等其他 schemata：

```text
before:
L3:0=0001;1=0001
MB:0=80;1=80

after:
L3:0=7fff;1=7fff
MB:0=80;1=80
```

事务：

1. 枚举当前 resctrl 一级 non-root CLOS。
2. 对每个 CLOS 读取 schemata before-image。
3. 仅将 L3 domain 更新为 capability `CBMMask`。
4. 保留 MB 和其他 schemata 行。
5. 写入失败由下一轮 reconcile 重新从当前硬件状态收敛。

最终实现不保留额外 reset session、managed reset candidate 或 physical DTO。
reset 逻辑直接以当前 resctrl 快照为事实来源。

`ResetOnce` 的 validator closure 只复核仅由 ownership 命中的 obsolete
candidate；由 immutable static config 命中的 candidate 不依赖 ownership 存续。
初次 validator 调用发生在全部 candidate physical locks 获取后、任何 CLOS FD
打开前，复核本轮 `ResolvedCLOS` 的 physical identity 仍有效；
失效则整轮零 FD open、零写入并重新计算 candidate。写入后若需 rollback，则在
每个待恢复 CLOS 前再次调用同一个 ownership validator；复核失败时跳过 stale
before-image 并聚合 concurrent validation error。rollback 复核独立于已取消的
operation context，避免取消请求阻断安全恢复。

该 reset 不建立新 ownership，不修改 foreign unowned CLOS。

### 并发收敛

Kubelet 可能并发创建 `dedicated`、`shared-50`。单次目录快照不能证明
disabled 状态已收敛，因此使用有界循环：

1. 枚举一级 control group，计算 managed candidate 并记录 physical ID/identity。
2. reset/readback。
3. 重新枚举。
4. 连续两轮 candidate 集合和 identity 稳定，且全部 candidate L3 mask 等于
   `CBMMask`，才 ready；foreign 集合变化不影响 ready。
5. 目录删除时重新枚举，不恢复已删除 incarnation。
6. 同名重建时重新打开 FD，不能复用旧 handle。
7. 超过 max rounds/deadline 时 CAT health not-ready。

disabled periodic reconcile 持续执行该收敛过程，保证后创建的 managed
`dedicated`/`shared-50` 最终恢复最大 ways。root 仍然只读验证为 `CBMMask`。

若同时存在 `share-50` 和 `shared-50`，managed reset 可以将两个物理目录都恢复为
`CBMMask`，但必须报告 canonical collision；再次启用 CAT 前必须消除 collision。

## Activation 与真实 task migration

这是实现前硬阻塞项。必须先定位真实 kubelet/runtime `resctrl/tasks` 消费端、
仓库、owner 和部署 revision。当前 Core `RDTManager.ApplyTasks` 没有生产调用方，
实现该方法不能代表 gate 完成。

migration token 至少携带：

```text
CAT mode
reset/activation epoch
canonical ID
physical ID
directory identity
generation
activation token
```

enabled 模式下，消费端写 `tasks` 时复核：

```text
phase == active
identity unchanged
generation unchanged
CAT readback == desired
```

disabled 模式不要求 per-CLOS active ownership，但必须满足：

```text
managed reset phase == ready
token reset epoch == current reset epoch
目标 physical CLOS directory identity 未变化
目标 CLOS 当前 L3 readback == CBMMask
```

managed reset 未收敛或后续发现新 managed 目录/drift 时，先撤销 disabled-ready epoch；
重新收敛后发布新 epoch。这样关闭 CAT 不会永久阻断 kubelet 创建的
`dedicated`/`shared-50` task migration，同时不会在 reset 进行中提前迁移。

消费端无法改造时禁止发布 selector/exclusive AQC。`RDTManager.ApplyTasks`
未实现且无生产调用方，不能当作已存在控制点。只识别旧 CLOS 字符串 annotation
的 consumer 与新 producer 不兼容，deployment gate 必须拒绝该组合。

## Reconcile

```text
读取 immutable policy
  -> enableCAT=true
       -> configured CLOS
       -> ways/placement selector
       -> exclusive graph
       -> desired target
       -> root precondition
       -> observed physical CLOS
       -> EnsurePendingCLOS for missing configured CLOS
       -> immutable ResolvedCLOS binding
       -> pending activation set -> transaction -> active
       -> active repair set -> drift transaction
  -> enableCAT=false
       -> fixed root FD precondition
       -> enumerate first-level control groups
       -> filter managed-by-config or valid-owned candidates
       -> directory FD/before-image
       -> L3-only CBMMask reset/readback
       -> bounded convergence
       -> root pre-publish validation
       -> publish disabled-ready epoch
```

AQC reconcile、periodic repair 和同步 activation 复用同一服务。

PeriodicalHandler：

- 不创建/激活 CLOS
- 强制读取真实 schemata
- enabled 时修复安全 policy drift
- disabled 时持续收敛所有 managed configured/owned non-root CLOS 到 `CBMMask`
- unsafe drift health not-ready

target cache 不能替代 observation。

## Admission 与 deployment gate

发布条件机器化：

```text
admission ready
AND canonical CRD digest matches
AND Core API revision matches
AND task migration consumer compatible
AND qrm-plugin image contains target Core
```

条件不满足时 pipeline 拒绝 selector/exclusive AQC。

真实 deployment 仓库当前未挂载，相关任务在仓库挂载前 blocked。

## qrm-plugin resctrl mount

```yaml
bytedance:
  mounts:
    - hostPath: /sys/fs/resctrl
      mountPath: /sys/fs/resctrl
      mountPropagation: HostToContainer
      mountType: Directory
      readOnly: false
```

禁止 `Bidirectional`、`DirectoryOrCreate` 和整个 `/sys/fs` 挂载。

## 最终 AQC

```yaml
bulkheadRDTConfig:
  enableCPUList: false
  catPolicy:
    enableCAT: true
    defaultCATWays: "MaxCATWays-MinCATWays"
    closCATWays:
      sandbox: "MinCATWays"
      xxx: "MinCATWays"
      "share-*": "MaxCATWays-MinCATWays"
    exclusiveClosIDs:
      - sandbox
      - xxx
    defaultPlacement:
      allowedBitUsages: ["*"]
      direction: low
    closPlacements:
      sandbox:
        allowedBitUsages: ["S"]
        direction: low
      xxx:
        allowedBitUsages: ["S"]
        direction: high
      "share-*":
        direction: high
```

若 AQC 不设置 `defaultPlacement`，则继承 flags/env；flags/env 缺省即
`allowedBitUsages="*"`、`direction=low`。

## 发布顺序

1. 定位真实 task consumer、写 `tasks` 入口、仓库和部署 revision。
2. 清查并迁移旧 flat CAT/allocationGroups AQC。
3. 上线 validating admission 和 deployment gate。
4. 合并 canonical API/CRD。
5. Core 立即切换 canonical API。
6. 实现 stateless lifecycle 和 `ResolvedCLOS` binding。
7. 实现 configured/observed/`ResolvedCLOS` binding。
8. 实现 selector/config merge/graph/target。
9. 扩展真实 RDT `SchemataCoordinator`、planner、transaction 和 restore。
10. 实现 reconcile、pending activation 和 active drift repair。
11. 先实现并部署真实 task consumer gate，再发布 producer token。
12. 增加 flags/env/adapter。
13. 升级 canonical CRD。
14. 升级 qrm-plugin。
16. 验证 readiness/revision/consumer。
17. 最后下发 AQC。
18. 再增加 `aa=40` 等 pool mapping。

## 测试矩阵

### 测试命名门禁

所有新增或修改的 CAT 单元测试禁止出现 `sandbox`。统一使用中性名称：

```text
clos-a
clos-b
peer-a
peer-b
group-40
group-*
exclusive-a
exclusive-b
```

该限制适用于 API、Core、admission 和 adapter 的 CAT 单元测试。真实 deployment
manifest 和节点验收若实际 CLOS 名为 `sandbox`，可以保留真实配置。

### API/admission

- 新嵌套 CAT shape round-trip
- 新 property 集合与生成 CRD schema 一致
- selector 合法/非法
- exclusive omitted/non-empty/empty
- exclusive duplicate/alias/wildcard/root 拒绝
- placement `*` 单独接受，与其他 usage 组合拒绝
- target cluster server-side dry-run
- admission unavailable fail-closed

旧 shape 使用运行时生成的临时 payload 做 target-cluster server-side dry-run，
不在 checked-in 单测中保留 legacy fixture、legacy type 或 legacy 字段名。

### flags/env/AQC merge

- built-in `*`/low 默认值
- startup override
- AQC policy omitted 继承
- closCATWays omitted 继承/non-empty 替换/empty map 清除
- exclusive omitted 继承/non-empty 替换/empty 关闭
- defaultPlacement omitted 继承/object 替换/empty object 回到 `*`/low
- invalid AQC 保持上一完整 snapshot

### selector/configured CLOS

- canonical/alias/prefix 优先级
- canonical `share-50` 到 physical `shared-50` 映射
- immutable canonical→physical/physical→canonical binding
- 用户配置拒绝 `shared-*`
- `share-50`/`shared-50` physical collision
- shared subgroup 不产生幽灵 `share`
- future CLOS
- physical collision
- prefix zero match/multi-prefix

### graph/target

- 单/多 exclusive star graph
- exclusive 互相 non-overlap
- non-exclusive 之间无 edge
- defaultCATWays fallback
- explicit per-CLOS override
- `*` 不过滤 bit usage
- low 默认方向
- per-domain/capacity/root
- 顺序确定性

### ownership/lifecycle

- 每个 create 落盘点 crash
- pending/quarantined inactive
- pending 进入 activation set，active 进入 repair set
- 缺失目录通过唯一 `EnsurePendingCLOS` 创建和绑定
- memory-first、CAT-first 复用同一 pending generation
- CATReady 后才 active
- active 重启复核
- same-name replacement

### transaction

- planner 成功后才写
- partial-safe/no-complete-order 零写入
- first/middle/last failure restore
- enabled rollback 前 external writer 修改时不覆盖新 L3
- enabled rollback 只恢复 L3，不覆盖并发 MB
- identity/generation/FD
- deadlock/concurrency
- drift bypass cache

### disabled managed reset

- configured managed 和 valid-owned 一级 CLOS reset
- physical `shared-50` 通过 canonical `share-50` 配置进入 managed set
- obsolete valid-owned CLOS 即使不再 configured 仍 reset
- foreign unowned CLOS 保持原值且不进入锁集
- root 初始异常时零写 non-root
- 发布 epoch 前 root identity/mask 改变时不 ready
- 每轮 convergence 从当前 resctrl 快照重新计算
- reset 与 CAT/MBA/lifecycle 共用 physical `closMu`
- root/info/mon_groups/mon_data/非一级目录排除
- 只修改 L3，保留 MB 和其他 schemata
- kubelet 并发创建 `dedicated`/`shared-50`
- 连续两轮 physical ID/identity 稳定后 ready
- 同名重建重新打开 FD
- 删除中的目录不恢复旧 incarnation
- 中途失败精确 before-image restore
- 其他 writer 在 rollback 前修改时不覆盖其新值
- max rounds/deadline 超限 not-ready
- periodic reconcile 收敛后创建目录
- collision 两个物理目录均 reset，但 enabled 前保持 not-ready

### activation/task

- CAT 前 inactive
- readback 后 active
- failure 不发布 token
- unknown token version 拒绝
- 旧纯 CLOS string consumer 与新 producer 混用被 deployment gate 拒绝
- consumer 写 tasks 前复核
- publication 后 recreate 拒绝

### 部署/E2E

- adapter env/flag mapping
- chart mount
- canonical revision/digest gate
- flags/env/AQC 三态
- `aa=40` 不修改 AQC
- create/delete/recreate `share-40`
- foreign CLOS
- drift repair
- unsafe zero-write
- standard 3 rounds
- high-churn 5 rounds
- old agent rollback
- final restore

### Legacy 与死代码门禁

生产代码、测试、生成代码和 CRD 中不得残留：

```text
CATAllocationGroup
AllocationGroups
allocationGroups
CATNonOverlapConstraint
NonOverlapConstraints
nonOverlapConstraints
ResctrlObsoleteSharedSubgroupPrefix
NormalizeClosID 旧兼容入口
旧 flat BulkheadRDTConfig CAT 字段
旧 group packing/rollback
旧 constraint graph builder
旧 operand compatibility alias/parser
```

不保留 deprecated type、dual-read/dual-write、fallback conversion、legacy
feature gate 或注释掉的旧实现。

`CATCapability.CBMMask`、`CATCapability.MinCBMBits` 和 resctrl 原始字段属于
硬件/内核边界，不是 legacy。

最终运行：

```bash
go test ./... -count=1
go vet ./...
golangci-lint run ./...
```

同时对 CAT 单测执行 `sandbox` 零匹配扫描，对旧符号执行代码/CRD 零匹配扫描。

## 验收标准

- 所有 CAT-owned 字段位于 `CATPolicy`。
- 不存在 active allocation-group/non-overlap-constraint 代码。
- API/admission/Core 语义一致。
- 目标集群真实拒绝非法 AQC。
- flags/env 默认 `allowedBitUsages="*"`、`direction=low`。
- `EnableCAT`、`DefaultCATWays`、`ClosCATWays`、`ExclusiveClosIDs`、`DefaultPlacement`
  支持 startup fallback。
- AQC closCATWays 空 map 可清除 startup per-CLOS ways。
- AQC exclusive 空列表可关闭 startup exclusive。
- 每个 exclusive 与全部其他 configured CLOS 不重叠。
- ways/placement 保持显式或明确 fallback。
- 新 CLOS CAT ready 前不能承载 task。
- memory resctrl manager 的 Create/ReconcileClos 与 CAT plugin 必须调用同一
  lifecycle service，CAT-enabled 路径只保留一个
  CLOS mkdir/identity-bind/phase owner。
- 本期 deployment gate 要求 `enableCPUList=false`；CPUListPlugin 的
  `shared-XX/cpus` 兼容留待独立方案。
- `pkg/util/external/rdt` 只暴露 physical transaction DTO/API，不依赖 agent
  resctrl package。
- unsafe transition 零写入。
- enabled/disabled 失败均使用 compare-before-restore，且不覆盖外部新值或
  非 CAT schemata。
- 无额外 lifecycle state。
- CAT activation 不进入 CPUPlugin state。
- periodic drift 不被 cache 短路。
- root 异常 fail-closed。
- `enableCAT=false` 将所有 managed configured/owned non-root control group 的
  L3 mask 收敛到 `CBMMask`，同时保留非 CAT schemata。
- disabled reset 不修改 foreign unowned 目录，也不创建新 ownership。
- kubelet physical `shared-*` 只通过 canonical-to-physical mapper 使用，用户
  配置只接受 `share-*`。
- `share-XX`/`shared-XX` collision 在 enabled 前必须消除。
- deployment gate 阻止提前 AQC。
- standard/high-churn/rollback/final restore 全部通过。
- 新增和修改的 CAT 单测不出现 `sandbox`。
- legacy symbol、compatibility path 和死代码扫描零残留。
