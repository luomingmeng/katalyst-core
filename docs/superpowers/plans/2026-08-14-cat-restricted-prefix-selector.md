# CAT 受限前缀选择器与独占 CLOS 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 CAT 配置收敛到 `CATPolicy`，支持 ways/placement 尾部前缀 selector、AQC/flags/env 三态覆盖和 `exclusiveClosIDs` 自动隔离，并通过 admission、safe transaction、activation 和 drift repair 保证节点安全。

**Architecture:** API 只描述 CAT policy；Core 将 startup fallback 与 AQC 原子合并，再把静态配置解析为 canonical configured CLOS、exclusive star-graph 和 immutable desired targets。所有 resctrl 读写通过 identity/generation、directory FD、纯 transition planner、多 CLOS transaction、before-image restore 和 activation gate 执行。

**Tech Stack:** Go、Kubernetes CRD/OpenAPI、Kubernetes validating webhook、resctrl、Katalyst QRM、Helm。

---

## 执行约束

- [ ] 不恢复 `CBMMask`、`MinCBMBits` 用户态 alias。
- [ ] 不保留旧 flat CAT AQC、`allocationGroups` 或 `NonOverlapConstraints`。
- [ ] `exclusiveClosIDs` 只生成 edge，不自动生成 ways/placement。
- [ ] flags/env 默认 `allowedBitUsages="*"`、`direction=low`。
- [ ] flags/env 支持 `closCATWays`；AQC 使用 omitted 继承、non-empty 替换、empty map 清除。
- [ ] 所有新增或修改的 CAT 单测使用中性 CLOS 名称，禁止出现 `sandbox`。
- [ ] 完成后删除全部 legacy path 和死代码，不保留 compatibility alias、dual-read/dual-write 或注释旧实现。
- [ ] admission、canonical CRD、task migration gate 和 deployment gate 完成前，不下发 selector/exclusive AQC。
- [ ] CAT 和 task token 必须共享同一个 immutable
  `ResolvedCLOS` canonical-to-physical binding。
- [ ] 本期不实现 CPUListPlugin 的 `shared-XX/cpus` 兼容；deployment gate 要求
  `enableCPUList=false`。
- [ ] API 先合入 canonical commit，Core 随后立即切换；不使用个人 fork 作为最终依赖。
- [ ] 每个任务按失败测试、最小实现、通过测试、独立提交执行。

## 实施前阻塞检查

- [ ] 清查所有持久化 AQC：

```bash
kubectl get adminqosconfigurations -A -o yaml \
  | grep -nE 'allocationGroups|nonOverlapConstraints|enableCAT:|defaultCATWays:|closCATWays:'
```

Expected：没有旧 flat CAT、allocation group 或 constraint；若有，先记录对象名并停止发布。

- [ ] 确认并挂载真实 task migration 消费端仓库，记录 owner、写
  `resctrl/tasks` 的具体入口和当前部署 revision。当前
  `pkg/util/external/rdt/manager_linux.go` 的 `ApplyTasks` 未实现，不能把它当成
  已存在 gate；没有生产调用方时也不能把“实现 ApplyTasks”当成完成。
- [ ] 挂载 qrm-plugin deployment 仓库。仓库未挂载时 Task 15 和 Task 16
  标记 blocked，禁止猜测 chart 路径或发布 AQC。
- [ ] 确认 API worktree 与 Core worktree：

```bash
git -C ../../../katalyst-api/.worktrees/default-share-residual-backfill-api status --short
git status --short
```

Expected：除已知文档外无非预期修改。

## 依赖顺序

任务编号沿用历史文档，不代表执行顺序；必须按下列 DAG 执行。特别是 Task 9
先定义底层 physical transaction DTO/API，Task 8 再定义
lifecycle/`ResolvedCLOS`，Task 5 最后实现 binding 和迁移
CAT 调用方，禁止按数字顺序直接执行。

```text
0 real task consumer contract + repository

1 API contract
  -> 2 canonical API dependency
  -> 3 startup flags/env
  -> 4 atomic AQC merge

9 physical RDT DTO + directory FD + transaction interface
  -> 8 stateless lifecycle service
  -> 5 selector + configured/resolved CLOS
  -> 6 resolver + exclusive graph
  -> 7 target builder
  -> 10 pure planner + transaction
  -> 12 plugin reconcile

0 + 8 + 9 + 12
  -> 11 producer + real task consumer gate

1 -> 13 validating admission
3 + 12 -> 14 adapter
13 + 14 -> 15 deploy gate
15 -> 16 migration/E2E/rollback
```

---

### Task 0: 真实 task consumer 契约

本任务是硬阻塞，不允许用 Core `RDTManager.ApplyTasks` 代替真实消费者。

**Required evidence:**

- 真实 kubelet/runtime consumer 仓库和 commit
- 写 `/sys/fs/resctrl/<physical>/tasks` 的函数路径
- 当前 annotation schema
- 新 token schema 和版本协商
- consumer deployment revision 查询方式
- 旧 consumer 拒绝门禁

- [ ] **Step 1: 跟踪现有 annotation**

从 `pkg/agent/qrm-plugins/memory/dynamicpolicy/resctrl_hinter.go` 的
`AnnotationRdtClosID` 开始，定位最终写 `tasks` 的外部调用链。结果写入设计文档，
不得只记录推测。

- [ ] **Step 2: 固化 token**

```go
type CLOSMigrationToken struct {
	Version      uint32
	CATMode      CATMode
	PolicyEpoch  uint64
	CanonicalID  string
	PhysicalID   string
	Identity     DirectoryIdentity
	Generation   uint64
	TargetDigest string
}
```

- [ ] **Step 3: 定义 consumer 拒绝语义**

```text
未知 token version -> 拒绝
enabled 且 phase/identity/generation/digest 不匹配 -> 拒绝
disabled 且 reset epoch/identity/CBMMask 不匹配 -> 拒绝
旧纯 CLOS 字符串 producer/consumer 与新模式混用 -> deployment gate 拒绝
```

- [ ] **Step 4: 建立独立外部 consumer 实施计划**

挂载真实仓库后必须给出精确文件、测试和提交边界。该计划合入前，Task 11、
Task 15 和 Task 16 保持 blocked。

---

### Task 1: API、CRD 与旧模型删除

**Files:**

- Modify: `katalyst-api/pkg/apis/config/v1alpha1/bulkhead.go`
- Modify: `katalyst-api/pkg/apis/config/v1alpha1/adminqos_test.go`
- Modify: `katalyst-api/pkg/apis/config/v1alpha1/bulkhead_schema_test.go`
- Generate: `katalyst-api/pkg/apis/config/v1alpha1/zz_generated.deepcopy.go`
- Generate: `katalyst-api/config/crd/bases/config.katalyst.kubewharf.io_adminqosconfigurations.yaml`

- [ ] **Step 1: 写新嵌套 shape 的失败测试**

```go
func TestBulkheadCATPolicyRoundTrip(t *testing.T) {
	raw := []byte(`{"bulkheadRDTConfig":{"catPolicy":{
	  "enableCAT":true,
	  "defaultCATWays":"MaxCATWays-MinCATWays",
	  "closCATWays":{"clos-a":"MinCATWays","group-*":"MaxCATWays-MinCATWays"},
	  "exclusiveClosIDs":["clos-a","clos-b"],
	  "defaultPlacement":{"allowedBitUsages":["*"],"direction":"low"},
	  "closPlacements":{"group-*":{"direction":"high"}}
	}}}`)
	var got AdminQoSConfigurationSpec
	require.NoError(t, json.Unmarshal(raw, &got))
	require.NotNil(t, got.BulkheadRDTConfig.CATPolicy.ExclusiveClosIDs)
	require.Equal(t, []string{"clos-a", "clos-b"},
		*got.BulkheadRDTConfig.CATPolicy.ExclusiveClosIDs)
}

func TestBulkheadCATPolicyPreservesExplicitEmptyClosCATWays(t *testing.T) {
	raw := []byte(`{"bulkheadRDTConfig":{"catPolicy":{"closCATWays":{}}}}`)
	var got AdminQoSConfigurationSpec
	require.NoError(t, json.Unmarshal(raw, &got))
	require.NotNil(t, got.BulkheadRDTConfig.CATPolicy.ClosCATWays)
	require.Empty(t, *got.BulkheadRDTConfig.CATPolicy.ClosCATWays)
	encoded, err := json.Marshal(got)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"closCATWays":{}`)
}
```

- [ ] **Step 2: 验证测试失败**

```bash
go test ./pkg/apis/config/v1alpha1 -run 'BulkheadCATPolicyRoundTrip' -count=1
```

Expected：FAIL，旧 API 没有嵌套字段或 `ExclusiveClosIDs`。

- [ ] **Step 3: 修改 API 类型**

```go
type BulkheadRDTConfig struct {
	EnableCPUList *bool      `json:"enableCPUList,omitempty"`
	CATPolicy     *CATPolicy `json:"catPolicy,omitempty"`
}

type CATPolicy struct {
	EnableCAT         *bool                           `json:"enableCAT,omitempty"`
	DefaultCATWays    *intstr.IntOrString             `json:"defaultCATWays,omitempty"`
	ClosCATWays       *map[string]intstr.IntOrString  `json:"closCATWays,omitempty"`
	ExclusiveClosIDs  *[]string                       `json:"exclusiveClosIDs,omitempty"`
	DefaultPlacement  *CATPlacementPolicy             `json:"defaultPlacement,omitempty"`
	ClosPlacements    map[string]CATPlacementPolicy   `json:"closPlacements,omitempty"`
}
```

pointer-to-slice 和 pointer-to-map 保留三态：nil pointer 表示继承、non-empty
pointer 表示替换、empty pointer value 表示关闭或清除。不能改回普通
`[]string,omitempty` 或 `map[string]intstr.IntOrString,omitempty`。

同时删除 `CATAllocationGroup`、`AllocationGroups` 和上一版
`CATNonOverlapConstraint`/`NonOverlapConstraints`。

- [ ] **Step 4: 加 schema/round-trip 测试**

断言：

```text
CAT 字段只出现在 catPolicy
exclusiveClosIDs uniqueItems=true
direction enum=low|high
allowedBitUsages item enum=*|S|X
CATPolicy property 集合严格等于新模型字段集合
生成 CRD 不包含已删除模型的 property
```

schema 测试只比较新 property 集合，不把已删除字段名写入测试源码。

- [ ] **Step 5: 生成并验证**

```bash
make generate
go test ./pkg/apis/config/v1alpha1 -count=1
if grep -R -n \
  --include='*.go' --include='*.yaml' --include='*.yml' \
  -E 'CATAllocationGroup|AllocationGroups|allocationGroups|CATNonOverlapConstraint|NonOverlapConstraints|nonOverlapConstraints' \
  pkg/apis/config/v1alpha1 config/crd; then
  exit 1
fi
git diff --check
```

Expected：PASS；生成 CRD 只含新嵌套 shape。

- [ ] **Step 6: 提交 API**

```bash
git add pkg/apis/config/v1alpha1 \
  config/crd/bases/config.katalyst.kubewharf.io_adminqosconfigurations.yaml
git commit -m "feat(config): nest cat policy and add exclusive clos ids"
```

记录 canonical API commit SHA。

---

### Task 2: Core 切换 canonical API revision

**Files:**

- Modify: `go.mod`
- Modify: `go.sum`

- [ ] **Step 1: 删除临时 replace**

```bash
grep -n 'katalyst-api' go.mod
```

Expected：确认当前依赖和任何临时 replace。

- [ ] **Step 2: 切换 Task 1 canonical commit**

```bash
API_COMMIT="$(
  git -C ../../../katalyst-api/.worktrees/default-share-residual-backfill-api \
    rev-parse HEAD
)"
go get "github.com/kubewharf/katalyst-api@${API_COMMIT}"
go mod tidy
```

`API_COMMIT` 必须来自 Task 1 的 canonical API worktree，不能使用个人 fork
pseudo-version。

- [ ] **Step 3: 验证依赖**

```bash
go list -m github.com/kubewharf/katalyst-api
grep -n 'replace .*katalyst-api' go.mod || true
git diff --check
```

Expected：输出 canonical revision；不存在 API replace。

- [ ] **Step 4: 提交**

```bash
git add go.mod go.sum
git commit -m "build: update canonical cat policy api"
```

---

### Task 3: Startup flags/env 与 placement 默认值

**Files:**

- Modify: `pkg/config/agent/dynamic/adminqos/qrm/cpu_plugin.go`
- Modify: `pkg/config/agent/dynamic/adminqos/qrm/rdt_config_test.go`
- Modify: `cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/cpu_plugin.go`
- Modify: `cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/qrm_base_test.go`

- [ ] **Step 1: 写默认值和解析失败测试**

```go
func TestCATStartupDefaults(t *testing.T) {
	o := NewCPUPluginOptions()
	require.Equal(t, "*", o.BulkheadCATDefaultAllowedBitUsages)
	require.Equal(t, "low", o.BulkheadCATDefaultDirection)
	require.Empty(t, o.BulkheadCATExclusiveClosIDs)
	require.Empty(t, o.BulkheadClosCATWays)
}

func TestParseCATAllowedBitUsages(t *testing.T) {
	for _, tc := range []struct {
		raw string
		ok  bool
	}{
		{"*", true}, {"S", true}, {"X", true}, {"S,X", true},
		{"*,S", false}, {"", false}, {"S,S", false}, {"H", false},
	} {
		_, err := parseCATAllowedBitUsages(tc.raw)
		require.Equal(t, tc.ok, err == nil, tc.raw)
	}
}
```

- [ ] **Step 2: 验证失败**

```bash
go test ./cmd/katalyst-agent/app/options/dynamic/adminqos/qrm \
  ./pkg/config/agent/dynamic/adminqos/qrm \
  -run 'CATStartup|CATAllowedBit' -count=1
```

- [ ] **Step 3: 增加 options**

```go
type CPUPluginOptions struct {
	EnableBulkheadCAT                     bool
	BulkheadDefaultCATWays                utilflag.ExplicitValue[string]
	BulkheadClosCATWays                   map[string]string
	BulkheadCATExclusiveClosIDs           []string
	BulkheadCATDefaultAllowedBitUsages    string
	BulkheadCATDefaultDirection           string
}
```

绑定：

```go
fs.BoolVar(&o.EnableBulkheadCAT, "enable-bulkhead-cat", false, "...")
fs.StringVar(&o.BulkheadDefaultCATWays.Value,
	"bulkhead-default-cat-ways", o.BulkheadDefaultCATWays.Value, "...")
fs.StringToStringVar(&o.BulkheadClosCATWays,
	"bulkhead-clos-cat-ways", o.BulkheadClosCATWays, "...")
fs.StringSliceVar(&o.BulkheadCATExclusiveClosIDs,
	"bulkhead-cat-exclusive-clos-ids", nil, "...")
fs.StringVar(&o.BulkheadCATDefaultAllowedBitUsages,
	"bulkhead-cat-default-allowed-bit-usages", "*", "...")
fs.StringVar(&o.BulkheadCATDefaultDirection,
	"bulkhead-cat-default-direction", "low", "...")
```

- [ ] **Step 4: 保留并扩展 `closCATWays` flag parser**

```text
--bulkhead-clos-cat-ways='clos-a=MinCATWays,group-*=MaxCATWays-MinCATWays'
```

对每个 key 调用 trailing-prefix selector parser，对每个 value 调用
`ParseCATWaysExpression`。拒绝空 key、非法 wildcard、旧 operand 和 duplicate
selector。

- [ ] **Step 5: 规范化 `*`**

```go
func parseCATAllowedBitUsages(raw string) ([]string, error) {
	if raw == "*" {
		return nil, nil
	}
	// split、trim；只接受唯一 S/X；出现 *、空值、重复或未知值时返回 error。
}
```

`nil` 表示不按 bit usage 过滤。

- [ ] **Step 6: 验证**

```bash
go test ./cmd/katalyst-agent/app/options/dynamic/adminqos/qrm \
  ./pkg/config/agent/dynamic/adminqos/qrm -count=1
```

- [ ] **Step 7: 提交**

```bash
git add cmd/katalyst-agent/app/options/dynamic/adminqos/qrm \
  pkg/config/agent/dynamic/adminqos/qrm
git commit -m "feat(qrm): add cat startup policy flags"
```

---

### Task 4: AQC 三态合并与原子 snapshot

**Files:**

- Modify: `pkg/config/agent/dynamic/adminqos/qrm/cat_policy.go`
- Modify: `pkg/config/agent/dynamic/adminqos/qrm/cpu_plugin.go`
- Modify: `pkg/config/agent/dynamic/adminqos/qrm/rdt_config_test.go`
- Modify: `cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/cpu_plugin.go`
- Modify: `cmd/katalyst-agent/app/options/dynamic/adminqos/qrm/qrm_base_test.go`

- [ ] **Step 1: 写 merge table test**

覆盖：

```text
catPolicy nil                     -> 继承 startup
closCATWays omitted               -> 继承 startup map
closCATWays {clos-a:MinCATWays}   -> 完整替换 startup map
closCATWays {}                    -> 清除 startup map
exclusive omitted                -> 继承 startup
exclusive [clos-a]               -> 替换 startup
exclusive []                     -> 关闭
defaultPlacement omitted         -> 继承 startup
defaultPlacement {}              -> */low
defaultPlacement {direction:high}-> */high
非法新 AQC                       -> 保留上一完整 snapshot
```

测试必须检查 slice/map 的 nil pointer、non-empty pointer 和 empty pointer。

- [ ] **Step 2: 验证失败**

```bash
go test ./pkg/config/agent/dynamic/adminqos/qrm \
  ./cmd/katalyst-agent/app/options/dynamic/adminqos/qrm \
  -run 'CATPolicy|ExclusiveClos|DefaultPlacement' -count=1
```

- [ ] **Step 3: 定义内部 optional 类型**

```go
type OptionalExclusiveClosIDs struct {
	Specified bool
	Values    []string
}

type OptionalClosCATWays struct {
	Specified bool
	Values    map[string]CATWaysExpression
}

type OptionalCATPlacement struct {
	Specified bool
	Value     CATPlacementPolicy
}
```

- [ ] **Step 4: 实现原子 conversion**

```go
func ConvertBulkheadRDTConfig(
	base StartupBulkheadRDTConfig,
	aqc *configv1alpha1.BulkheadRDTConfig,
) (DynamicBulkheadRDTConfiguration, error)
```

先构造局部结果，完成 expression、placement、exclusive 和 selector 全部校验后
一次性替换 snapshot。任何 error 都不能修改旧值。

- [ ] **Step 5: 删除旧 flat/allocation/constraint conversion**

搜索必须只剩迁移文档或拒绝测试：

```bash
rg 'AllocationGroups|NonOverlapConstraints|bulkheadRDTConfig.*EnableCAT'
```

- [ ] **Step 6: 验证并提交**

```bash
go test ./pkg/config/agent/dynamic/adminqos/qrm \
  ./cmd/katalyst-agent/app/options/dynamic/adminqos/qrm -count=1
git add pkg/config/agent/dynamic/adminqos/qrm \
  cmd/katalyst-agent/app/options/dynamic/adminqos/qrm
git commit -m "feat(qrm): merge cat startup and aqc policy"
```

---

### Task 5: 通用 selector 与 configured CLOS

**Files:**

- Create: `pkg/util/general/trailing_prefix_selector.go`
- Create: `pkg/util/general/trailing_prefix_selector_test.go`
- Modify: `pkg/consts/resctrl.go`
- Modify: `pkg/util/resctrl/clos.go`
- Modify: `pkg/util/resctrl/clos_test.go`
- Create: `pkg/agent/qrm-plugins/resctrl/clos_resolver.go`
- Create: `pkg/agent/qrm-plugins/resctrl/clos_resolver_test.go`

- [ ] **Step 1: 写 selector parser 失败测试**

合法 exact/prefix：

```text
dedicated
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
空白
```

- [ ] **Step 2: 实现 parser**

```go
type TrailingPrefixSelector struct {
	Raw   string
	Kind  TrailingPrefixSelectorKind
	Value string
}

func ParseTrailingPrefixSelector(raw string) (TrailingPrefixSelector, error)
```

parser 不包含 CAT、pool 或 share 业务知识。

- [ ] **Step 3: 写 configured CLOS 测试**

输入：

```go
EnabledQoS = dedicated_cores,shared_cores
DefaultSharedSubgroup = 50
CPUSetPoolToSharedSubgroup = {"aa": 40, "bb": 40}
DefaultClosIDs = {"clos-a"}
```

期望：

```text
clos-a,dedicated,share-40,share-50
```

不得额外生成 `share`。

- [ ] **Step 4: 实现 configured builder**

```go
func BuildConfiguredClosIDs(config *ResctrlConfig) sets.Set[string]
```

只读取静态配置，不读取目录或 ownership。

- [ ] **Step 5: 明确 canonical-to-physical shared mapping**

```go
const (
	ResctrlShareSubgroupPrefix       = "share-"
	ResctrlPhysicalSharedGroupPrefix = "shared-"
)

func CanonicalClosIDFromPhysical(physicalID string) string

func PreferredPhysicalClosID(canonicalID string) string
```

测试：

```text
share-50  -> share-50
shared-50 -> share-50
dedicated -> dedicated
```

用户 selector/config 只接受 canonical `share-*`；`shared-*` 只能来自真实物理
目录。删除 `ResctrlObsoleteSharedSubgroupPrefix` 和含糊的 compatibility 命名。

同时存在 physical `share-50` 和 `shared-50` 时返回 canonical collision，不能
静默选择。

- [ ] **Step 6: 构建 immutable 双向 binding**

```go
func ResolveCLOSBindings(
	configured sets.Set[string],
	observed []CPUListClos,
	resolved map[string]ResolvedCLOS,
) (map[string]ResolvedCLOS, error)
```

`ResolvedCLOS` 使用 Task 8 在 `pkg/agent/qrm-plugins/resctrl` 定义的基础类型，Task 5
不重复定义 lifecycle/ownership 类型。

一次 snapshot 内构建 canonical→resolved 和 physical→canonical。一个 canonical
对应多个 physical、record identity 不匹配或 generation 不一致时整轮失败。
transaction 开始后禁止再次按名称解析。

- [ ] **Step 7: 验证并提交**

```bash
go test ./pkg/util/general ./pkg/util/resctrl \
  ./pkg/agent/qrm-plugins/resctrl \
  -count=1
git add pkg/util/general pkg/util/resctrl pkg/consts/resctrl.go \
  pkg/agent/qrm-plugins/resctrl
git commit -m "feat(resctrl): bind canonical clos to physical resources"
```

---

### Task 6: Selector resolver 与 exclusive graph

**Files:**

- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat/selector.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat/selector_test.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat/conflict_graph.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat/conflict_graph_test.go`

- [ ] **Step 1: 写统一优先级测试**

```text
direct canonical > exact alias > one prefix > default
direct 存在时低级 candidate 被忽略
同级 alias 相同配置合并、不同配置拒绝
多 prefix 命中拒绝
prefix zero match 拒绝
```

- [ ] **Step 2: 实现 generic candidate resolver**

ways 和 placement 必须调用同一个 candidate resolution helper，不能复制优先级。

- [ ] **Step 3: 写 exclusive graph 测试**

```go
configured := []string{"clos-a", "clos-b", "peer-a", "peer-b", "peer-c"}
exclusive := []string{"clos-a", "clos-b"}
```

断言：

```text
clos-a 与全部其他 CLOS 有 edge
clos-b 与全部其他 CLOS 有 edge
peer-a/peer-b/peer-c 之间无 edge
priority 先 clos-a、clos-b，再按 canonical ID
输入 map/set 顺序不影响结果
duplicate/alias/wildcard/root/unknown exclusive 拒绝
```

- [ ] **Step 4: 实现 graph**

```go
func BuildExclusiveCATConflictGraph(
	configured []string,
	exclusive []string,
) (CATConflictGraph, error)
```

edge 使用 `(min,max)` 规范化并自动去重。

- [ ] **Step 5: 验证并提交**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat \
  -run 'Selector|ConflictGraph|Exclusive' -count=1
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat
git commit -m "feat(qrm): resolve cat policy and exclusive graph"
```

---

### Task 7: Desired target 纯函数

**Files:**

- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat/target_builder.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat/target_builder_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat/policy.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat/policy_test.go`

- [ ] **Step 1: 写 target table tests**

覆盖：

```text
MaxCATWays=15, MinCATWays=1
defaultCATWays fallback
per-CLOS exact/prefix override
allowedBitUsages=nil/* 不过滤
direction 默认 low
S-only low/high
多 domain
exclusive edge non-overlap
non-exclusive overlap
capacity failure
root 不进入 target
future configured CLOS 稳定 existing target
```

- [ ] **Step 2: 验证失败**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat \
  -run 'Target|Placement|DefaultCATWays' -count=1
```

- [ ] **Step 3: 实现 builder**

```go
func BuildDesiredCATTargets(input CATTargetBuildInput) (CATTargets, error)
```

每个 domain：

```text
CBMMask
-> optional bit_usage filter
-> remove allocated neighbor masks
-> evaluate ways
-> select contiguous low/high mask
```

- [ ] **Step 4: 删除 allocation-group packing**

生产代码不能再出现 group membership、group mask 或 group rollback。

- [ ] **Step 5: 验证并提交**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat -count=1
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat
git commit -m "feat(qrm): build deterministic cat targets"
```

---

### Task 8: Stateless lifecycle 与 `ResolvedCLOS`

**Files:**

- Modify: `pkg/agent/qrm-plugins/resctrl/ownership.go`
- Modify: `pkg/agent/qrm-plugins/resctrl/lifecycle.go`
- Modify: `pkg/agent/qrm-plugins/resctrl/lifecycle_test.go`
- Modify: `pkg/agent/qrm-plugins/memory/dynamicpolicy/resctrl/manager.go`
- Modify: `pkg/agent/qrm-plugins/memory/dynamicpolicy/resctrl/manager_test.go`

- [ ] **Step 1: 写 lifecycle table tests**

```text
existing CLOS -> bind physical identity
missing CLOS -> mkdir then bind physical identity
non-empty CLOS -> delete rejected
empty CLOS -> delete succeeds
parallel ensure -> same physical identity
```

- [ ] **Step 2: 定义本轮绑定类型**

```go
type ResolvedCLOS struct {
	CanonicalID string
	PhysicalID  string
	Identity    rdt.DirectoryIdentity
	Generation  uint64
	Phase       ActivationPhase
}
```

`ResolvedCLOS` 只描述本轮 reconcile 中观测到的 physical directory binding。
不持久化 CLOS ownership，不写额外 state 文件，不从通用 QRM state directory 派生
lifecycle path。终态由 Katalyst configured/default CLOS 集合和当前 resctrl snapshot
共同决定。

- [ ] **Step 3: 实现唯一 lifecycle service**

```go
type CLOSLifecycleService interface {
	EnsurePendingCLOS(
		ctx context.Context,
		canonicalID string,
		preferredPhysicalID string,
	) (ResolvedCLOS, error)
	DeleteCLOS(ctx context.Context, physicalID string) error
	Recover(ctx context.Context) error
}
```

在统一 coordinator lock 内执行 mkdir 或现存目录观察，并绑定 physical ID 与
directory identity。`Recover` 不读取额外 lifecycle state；启动后通过 fresh resctrl
read-back 重新收敛。

`DeleteCLOS` 使用 physical ID，并要求 `tasks`、`cpus`、`mon_groups` 全部为空；
`disableRDT=true` 路径按 Katalyst 终态删除所有非 skip CLOS。

同时删除或改写 memory resctrl manager 的 `createClosLocked`：

```text
memory Manager.Create
memory Manager.ReconcileClos
CAT plugin
```

三条路径全部复用同一个 `CLOSLifecycleService`，不得保留第二个 mkdir/delete
实现，也不得保留额外 lifecycle state 或转发型 CLOS manager。

- [ ] **Step 4: 验证**

```bash
go test ./pkg/agent/qrm-plugins/resctrl \
  ./pkg/agent/qrm-plugins/memory/dynamicpolicy/resctrl -count=1
```

- [ ] **Step 5: 提交**

```bash
git add pkg/agent/qrm-plugins/resctrl \
  pkg/agent/qrm-plugins/memory/dynamicpolicy/resctrl
git commit -m "feat(qrm): converge clos lifecycle statelessly"
```

---

### Task 9: Directory FD 与多 CLOS coordinator

**Files:**

- Modify: `pkg/util/external/rdt/manager.go`
- Modify: `pkg/util/external/rdt/manager_linux.go`
- Modify: `pkg/util/external/rdt/manager_linux_test.go`
- Modify: `pkg/util/external/rdt/manager_unsupported.go`
- Modify: `pkg/util/external/rdt/schemata.go`
- Modify: `pkg/util/external/rdt/schemata_test.go`

- [ ] **Step 1: 写 same-name replacement 和锁序测试**

测试必须模拟：

```text
path stat 后目录被删建
directory FD identity 保持旧 inode
generation 变化拒绝
反向 lifecycle lock 请求仍按 physical ID 排序
CAT/MBA/lifecycle 共享同一锁域
```

- [ ] **Step 2: 保持 path-based lifecycle 边界**

最终实现不再引入独立 directory-handle transaction/reset session 层。这套抽象在无状态终态收敛后没有生产调用方，
保留只会形成未接入的第二事务系统。

CLOS 创建/删除保留在 `pkg/agent/qrm-plugins/resctrl` 的
`CLOSLifecycleService` 中，并通过 `SchemataCoordinator.RunClosLifecycle`
与 schemata RMW 共享同一 per-CLOS 锁域。CPUList 写入继续通过
`RunClosResourceUpdate` 串行化。

- [ ] **Step 3: 验证并提交**

```bash
go test ./pkg/util/external/rdt \
  ./pkg/agent/qrm-plugins/resctrl \
  -count=1
git add pkg/util/external/rdt \
  pkg/agent/qrm-plugins/resctrl
git commit -m "feat(resctrl): serialize clos lifecycle updates"

---

### Task 10: 纯 transition planner 与精确 restore

**Files:**

- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat/transition.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat/transition_test.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat/transaction.go`
- Create: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat/transaction_test.go`

- [ ] **Step 1: 写 planner 零写入测试**

构造：

```text
前一个 candidate 可安全模拟
剩余 pending 没有完整安全顺序
```

断言：

```text
PlanCATTransition 返回 error
ApplyCAT 调用次数 == 0
```

- [ ] **Step 2: 实现纯 planner**

```go
func PlanCATTransition(
	current CATTargets,
	desired CATTargets,
	graph CATConflictGraph,
) ([]string, error)
```

只修改 current 副本；返回完整 immutable order 后才允许真实写入。

- [ ] **Step 3: 写 transaction restore tests**

覆盖 first/middle/last apply/readback failure，断言每个已修改 CLOS 恢复自己的
before-image，而不是统一 default。

- [ ] **Step 4: 实现 transaction**

```go
func ApplyCATTargets(
	ctx context.Context,
	clos map[string]ResolvedCLOS,
	desired CATTargets,
	graph CATConflictGraph,
) error
```

最终实现不保留额外 physical transaction 层。上层先做无副作用预校验，
再按 resolved CLOS 与 graph 计算 immutable 写入顺序；真实写入通过现有
`SchemataCoordinator.ApplyL3` 的 per-CLOS RMW 路径完成。

顺序：

```text
after all physical locks, before opening CLOS FDs:
verify physical identity from resolved bindings
verify handle identity/root
read all before-images
plan
apply/readback in order
on failure, compare-before-restore in reverse
restore only when identity/generation unchanged and current L3 == last-written L3
preserve concurrent MB changes
aggregate apply/restore/concurrent mutation errors
```

测试必须模拟外部 writer 在 apply 后、rollback 前修改 L3，断言 Core 不覆盖其新值。
同时模拟初始 snapshot 后、获取 physical lock 前 generation/phase 改变，断言
callback 零写入失败。

- [ ] **Step 5: 写 disabled managed reset 失败测试**

构造一级目录：

```text
dedicated
shared-50
foreign-a
share-50
obsolete-owned
info
mon_groups
dedicated/mon_groups/child
```

断言：

```text
dedicated/shared-50/share-50 reset
obsolete-owned 虽不再 configured 但 ownership 有效，因此 reset
foreign-a unowned 保持原值且不进入 transaction lock set
info/mon_groups/非一级目录不写
L3 全部等于 CBMMask
MB 和其他 schemata 保持 before-image
不新增或修改额外 lifecycle state
中途失败逆序恢复仍存在且 identity 未变化的目录
restore 前其他 writer 已修改 L3 -> 跳过 restore 并报告 concurrent mutation
root 初始不等于 CBMMask -> 零写 non-root
reset 后发布前 root 改变/ABA -> 撤销 epoch 且不发布 ready
reset 与 MBA/lifecycle 并发 -> 共享 physical closMu 串行
两轮 reset 都从当前 resctrl 快照重新计算
```

- [ ] **Step 6: 实现单轮 managed reset transaction**

disabled reset 最终不保留 managed reset candidate/session 抽象。`enableCAT=false`
按当前 resctrl 一级 non-root CLOS 快照执行全局 reset，仅更新 L3 schemata 为硬件
`CBMMask`，保留 MB 等其他 schemata 行；收敛由周期性 reconcile 保证。

- [ ] **Step 7: 验证并提交**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat \
  -run 'Transition|Transaction|Restore|Reset' -count=1
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat
git commit -m "feat(qrm): apply cat with safe transactions"
```

---

### Task 11: Activation gate 与真实 tasks 写入

**Files:**

- Modify: `pkg/agent/qrm-plugins/memory/dynamicpolicy/resctrl/manager.go`
- Modify: `pkg/agent/qrm-plugins/memory/dynamicpolicy/resctrl/manager_test.go`
- Modify: `pkg/agent/qrm-plugins/memory/dynamicpolicy/resctrl_hinter.go`
- Modify: `pkg/agent/qrm-plugins/memory/dynamicpolicy/resctrl_hinter_test.go`
- Modify: Task 0 固化的真实 consumer 文件

- [ ] **Step 1: 写 tasks gate 失败测试**

在真实 consumer 仓库断言 pending/quarantined、unknown token version、identity
mismatch、generation mismatch、target digest mismatch、CAT readback mismatch
都不能写 `tasks`。

- [ ] **Step 2: 定义 migration token**

```go
type CLOSMigrationToken struct {
	Version      uint32
	CATMode     CATMode
	PolicyEpoch uint64
	CanonicalID string
	PhysicalID  string
	Identity    DirectoryIdentity
	Generation  uint64
	TargetDigest string
}
```

- [ ] **Step 3: 先实现真实 consumer gate**

在 Task 0 定位的真实写 `resctrl/tasks` 函数内复核 token。Core
`RDTManager.ApplyTasks` 没有生产调用方时不作为交付项。

enabled token 要求 per-CLOS active identity/generation/CAT target；disabled token
要求 current managed reset epoch 为 ready、目标 physical identity 未变化且当前
L3 readback 为 `CBMMask`。reset 失稳时立即撤销旧 disabled-ready epoch。

- [ ] **Step 4: consumer 合入部署后再 gate producer publication**

memory resctrl manager/hinter 仅对 active generation 或 current disabled-ready
epoch 发布版本化 annotation/token。
publication 后同名 recreate 必须导致 consumer 拒绝旧 token。

- [ ] **Step 5: 验证并提交**

```bash
执行 Task 0 固化的真实 consumer test command
go test ./pkg/agent/qrm-plugins/memory/dynamicpolicy/resctrl -count=1
git add pkg/agent/qrm-plugins/memory/dynamicpolicy/resctrl \
  pkg/agent/qrm-plugins/memory/dynamicpolicy/resctrl_hinter.go \
  pkg/agent/qrm-plugins/memory/dynamicpolicy/resctrl_hinter_test.go
git commit -m "feat(resctrl): gate task migration on cat activation"
```

真实 consumer commit 和部署 revision 必须记录到 delivery evidence；不能仅修改
producer 后继续发布。

---

### Task 12: CAT plugin reconcile 与 drift repair

**Files:**

- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat/plugin.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat/plugin_test.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat/policy.go`
- Modify: `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat/policy_test.go`

- [ ] **Step 1: 写完整 reconcile tests**

覆盖：

```text
policy invalid -> previous active snapshot/target unchanged
root != CBMMask -> not-ready/zero write
new CLOS pending -> CAT/readback 后 active
pending 进入 activation set，不能被 active-only repair set 过滤
缺失 CLOS -> EnsurePendingCLOS 后形成 binding
memory-first/CAT-first 都复用同一 pending generation
periodic drift -> 强制读取真实 schemata
target cache 命中不能跳过 observation
unsafe drift -> not-ready/zero write
CAT disabled -> reset managed configured/owned non-root CLOS to CBMMask
CAT disabled -> shared-50 通过 configured share-50 命中
CAT disabled -> obsolete valid-owned reset
CAT disabled -> foreign unowned 保持不变
CAT disabled -> 只修改 L3，MB 保持不变
CAT disabled -> kubelet 并发创建 dedicated/shared-50 后重新收敛
CAT disabled -> 连续两轮 managed candidate ID/identity 稳定才 ready
CAT disabled -> foreign 集合变化不撤销 ready epoch
CAT disabled -> deadline/round limit 超限 not-ready
CAT disabled -> stable 后发布新 reset epoch
CAT disabled -> root 初始异常零写 non-root
CAT disabled -> 发布前 root identity/mask 改变则撤销 epoch
```

- [ ] **Step 2: 实现统一服务**

```go
func (p *Plugin) ReconcileCAT(ctx context.Context, reason ReconcileReason) error
```

AQC reconcile、sync activation 和 periodic repair 共用；分支：

```text
enableCAT=true
  -> configured -> resolver -> graph -> desired -> root
  -> observed -> EnsurePendingCLOS for configured missing physical directories
  -> immutable ResolvedCLOS binding
  -> activation set(configured ∩ pending)
  -> CAT transaction/readback -> CATReady -> active
  -> repair set(configured ∩ active) -> drift transaction

enableCAT=false
  -> list current first-level non-root CLOS groups
  -> reset each group's L3 schemata to hardware CBMMask
  -> preserve MB and other schemata resources
  -> retry from a fresh snapshot on the next reconcile if convergence is incomplete
```

disabled convergence 使用 `DefaultCATResetMaxRounds=8` 和现有
`DefaultTopologyConvergenceDeadline=10s`。本轮未收敛时 periodic reconcile
继续重试，但 health 保持 not-ready。

- [ ] **Step 3: 移除旧 group/update path**

确保所有 CAT 写入只通过 coordinator/transaction，不再直接调用
`UpdateCAT` 或统一 default rollback。

- [ ] **Step 4: 验证并提交**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat -count=1
git add pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat
git commit -m "feat(qrm): reconcile cat policy and drift"
```

---

### Task 13: Validating admission

**Files:**

- Create: `pkg/webhook/validating/adminqosconfiguration/cat_policy.go`
- Create: `pkg/webhook/validating/adminqosconfiguration/cat_policy_test.go`
- Create: `cmd/katalyst-webhook/app/webhook/validating/adminqosconfiguration.go`
- Modify: `cmd/katalyst-webhook/app/webhook.go`
- Modify: `cmd/katalyst-webhook/app/webhook/consts.go`

- [ ] **Step 1: 写 admission table tests**

接受：

```text
exact/prefix ways/placement
exclusive omitted/non-empty/empty
allowedBitUsages */S/X/S,X
defaultPlacement {}
```

拒绝：

```text
非法 selector
duplicate/alias/wildcard/root exclusive
["*","S"]
非法 expression/direction
```

旧 shape 由 structural CRD unknown-field 校验和 Task 16 的临时 server-side dry-run
验证，不在 checked-in 单测中保留 legacy fixture 或 legacy 字段名。

- [ ] **Step 2: 实现 validator**

```go
func ValidateCATPolicy(
	oldObj, newObj *configv1alpha1.AdminQoSConfiguration,
) field.ErrorList
```

复用 Core-independent parser/validation helper，避免 CEL-only enforcement。
webhook 只校验对象内语法和结构；configured CLOS 来自节点静态配置，
`unknown exclusive` 必须由每个节点 Core fail-closed。若 deployment gate 要在
下发前验证 membership，必须按目标节点配置族运行离线 resolver。

- [ ] **Step 3: 注册 webhook**

为 AQC create/update 注册 validating path；failure policy 在部署清单中必须为
`Fail`。

- [ ] **Step 4: 验证**

```bash
go test ./pkg/webhook/validating/adminqosconfiguration \
  ./cmd/katalyst-webhook/app/webhook/... -count=1
```

- [ ] **Step 5: 提交**

```bash
git add pkg/webhook/validating/adminqosconfiguration \
  cmd/katalyst-webhook/app/webhook
git commit -m "feat(webhook): validate cat policy"
```

---

### Task 14: Adapter flags/env

**Files:**

- Modify: `katalyst-adapter/launch/qrm_plugin/adapters/args-passthrough/entrypoint.sh`
- Modify: `katalyst-adapter/launch/qrm_plugin/adapters/args-passthrough/entrypoint_test.sh`

- [ ] **Step 1: 写 env mapping 失败测试**

输入：

```text
QRMCPUPluginEnableBulkheadCAT=true
QRMCPUPluginBulkheadDefaultCATWays=MaxCATWays-MinCATWays
QRMCPUPluginBulkheadClosCATWays=clos-a=MinCATWays,group-*=MaxCATWays-MinCATWays
QRMCPUPluginBulkheadCATExclusiveClosIDs=clos-a,clos-b
QRMCPUPluginBulkheadCATDefaultAllowedBitUsages=*
QRMCPUPluginBulkheadCATDefaultDirection=low
```

期望 argv：

```text
--enable-bulkhead-cat=true
--bulkhead-default-cat-ways=MaxCATWays-MinCATWays
--bulkhead-clos-cat-ways=clos-a=MinCATWays,group-*=MaxCATWays-MinCATWays
--bulkhead-cat-exclusive-clos-ids=clos-a,clos-b
--bulkhead-cat-default-allowed-bit-usages=*
--bulkhead-cat-default-direction=low
```

- [ ] **Step 2: 增加映射**

```bash
param_map["QRMCPUPluginEnableBulkheadCAT"]="enable-bulkhead-cat"
param_map["QRMCPUPluginBulkheadDefaultCATWays"]="bulkhead-default-cat-ways"
param_map["QRMCPUPluginBulkheadClosCATWays"]="bulkhead-clos-cat-ways"
param_map["QRMCPUPluginBulkheadCATExclusiveClosIDs"]="bulkhead-cat-exclusive-clos-ids"
param_map["QRMCPUPluginBulkheadCATDefaultAllowedBitUsages"]="bulkhead-cat-default-allowed-bit-usages"
param_map["QRMCPUPluginBulkheadCATDefaultDirection"]="bulkhead-cat-default-direction"
```

- [ ] **Step 3: 验证并提交**

```bash
bash launch/qrm_plugin/adapters/args-passthrough/entrypoint_test.sh
git add launch/qrm_plugin/adapters/args-passthrough
git commit -m "feat(adapter): pass cat policy startup flags"
```

---

### Task 15: Deployment gate、CRD、mount 与 AQC

**Files:**

- Modify in mounted qrm-plugin deployment repository: canonical AQC CRD source
- Modify in mounted qrm-plugin deployment repository: validating webhook configuration
- Modify in mounted qrm-plugin deployment repository: qrm-plugin chart values/template
- Modify in mounted qrm-plugin deployment repository: final AQC manifest
- Add in mounted qrm-plugin deployment repository: CAT compatibility gate script/test

本任务在 deployment 仓库未挂载时为硬阻塞；不得创建猜测路径。

- [ ] **Step 1: 写 gate 失败测试**

gate 输入：

```text
webhook readiness
CRD digest
Core API revision
task consumer revision
qrm-plugin image revision
effective AQC enableCPUList=false
```

任一不匹配时退出非零并禁止 apply selector/exclusive AQC。

- [ ] **Step 2: 使用 canonical CRD**

删除 chart 中手写旧 CRD；生成/校验 digest 必须来自 Task 1 canonical API。

- [ ] **Step 3: 配置 webhook fail-closed**

```yaml
failurePolicy: Fail
sideEffects: None
admissionReviewVersions: ["v1"]
```

- [ ] **Step 4: 配置 resctrl mount**

```yaml
- hostPath: /sys/fs/resctrl
  mountPath: /sys/fs/resctrl
  mountPropagation: HostToContainer
  mountType: Directory
  readOnly: false
```

- [ ] **Step 5: 更新最终 AQC**

```yaml
bulkheadRDTConfig:
  enableCPUList: false
  catPolicy:
    enableCAT: true
    defaultCATWays: MaxCATWays-MinCATWays
    closCATWays:
      sandbox: MinCATWays
      xxx: MinCATWays
      "share-*": MaxCATWays-MinCATWays
    exclusiveClosIDs: [sandbox, xxx]
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

- [ ] **Step 6: Helm/render 验证并提交**

运行该仓库既有 lint/render/test 命令，断言：

```text
CRD digest canonical
failurePolicy Fail
resctrl mount 精确
旧字段不存在
flags/env 默认存在
final AQC 是嵌套 CATPolicy
final AQC enableCPUList=false
```

---

### Task 16: 全量验证、迁移、节点 E2E 与回滚

**Files:**

- Update: `qrm-bulkhead-test-artifacts/cat-policy-2605-20260814/summary.md`
- Create in test artifact directory: admission dry-run evidence
- Create in test artifact directory: standard/high-churn/rollback evidence

- [ ] **Step 1: API/Core/Adapter 本地验证**

```bash
# katalyst-api
go test ./pkg/apis/config/v1alpha1 -count=1

# katalyst-core
go test ./pkg/util/general ./pkg/util/resctrl \
  ./pkg/config/agent/qrm/bulkhead \
  ./pkg/config/agent/dynamic/adminqos/qrm \
  ./pkg/util/external/rdt \
  ./pkg/agent/qrm-plugins/resctrl \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cpulist \
  ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat \
  ./pkg/agent/qrm-plugins/memory/dynamicpolicy/resctrl \
  ./pkg/webhook/validating/adminqosconfiguration -count=1
go build ./cmd/katalyst-agent ./cmd/katalyst-webhook

# katalyst-adapter
bash launch/qrm_plugin/adapters/args-passthrough/entrypoint_test.sh
```

- [ ] **Step 2: 目标集群 admission dry-run**

验证：

```text
合法 nested CATPolicy 接受
旧 flat CAT 拒绝
非法 selector 拒绝
duplicate/wildcard/root exclusive 拒绝
["*","S"] 拒绝
admission unavailable 时请求失败
对象均未持久化
```

`unknown exclusive` 不作为 admission 断言；使用目标节点静态配置执行 Core
resolver，验证节点 not-ready 且 CAT 零写入。

- [ ] **Step 3: standard 3 rounds**

每轮：

```text
创建 share-40
CAT apply/readback 后才 active
sandbox/xxx 与所有其他 configured CLOS overlap=0
non-exclusive overlap 允许
删除 share-40
恢复并 fresh read-back
```

- [ ] **Step 5: high-churn 5 rounds**

删除/重建同名 `share-40`，验证 identity/generation 变化；旧 token 不能写 tasks。

- [ ] **Step 6: placement 默认值**

只配置 startup 默认，验证：

```text
allowedBitUsages=* -> CBMMask
direction=low
```

AQC：

```text
defaultPlacement omitted -> 继承 startup
defaultPlacement {} -> */low
defaultPlacement high/S -> 整体替换
```

- [ ] **Step 7: exclusive 三态**

```text
AQC omitted -> 继承 flags/env
AQC [sandbox,xxx] -> 替换
AQC [] -> 无 conflict edge
```

- [ ] **Step 8: closCATWays 三态**

```text
AQC omitted -> 继承 flags/env map
AQC {clos-a:MinCATWays} -> 完整替换
AQC {} -> 清除 per-CLOS map，仅使用 defaultCATWays
```

- [ ] **Step 9: unsafe/drift/restore**

```text
无完整安全顺序 -> 零写入
中途 apply/readback 失败 -> 精确 before-image restore
周期 drift 绕过 cache
root != CBMMask -> not-ready/零写入
enabled rollback 前 foreign writer 修改 -> 不覆盖其新 L3
```

- [ ] **Step 10: disabled managed reset**

准备：

```text
configured CLOS
owned obsolete CLOS
kubelet physical dedicated/shared-50
foreign-a
带 MB 配置的 CLOS
嵌套 mon_groups
```

切换 `enableCAT=false` 后验证：

```text
所有 managed configured/owned non-root CLOS 的每个 L3 domain == CBMMask
foreign-a unowned 保持原 L3
MB 和其他非 CAT schemata 保持原值
root 未写
无额外 lifecycle state 文件
并发新建目录后撤销旧 reset epoch
连续两轮 physical ID/identity 稳定后发布新 disabled-ready epoch
task consumer 使用新 epoch 后才允许写 tasks
root 初始不等于 CBMMask -> 零写 non-root
发布前 root identity/mask 改变 -> 撤销 epoch且不 ready
```

注入 middle write/readback failure，验证所有已修改且 identity 未变化的目录恢复
精确 before-image。rollback 前模拟 foreign writer 修改 L3，验证 Core 不覆盖其
新值并报告 concurrent mutation。注入持续目录 churn，验证 8 rounds/10s 超限后
not-ready。

- [ ] **Step 11: physical shared subgroup compatibility**

```text
AQC/flag share-* 命中 canonical share-50
实际写入 kubelet physical shared-50
用户配置 shared-* 被拒绝
share-50 与 shared-50 同时存在时 enabled not-ready
disabled reset 同时将两个物理目录恢复为 CBMMask
```

- [ ] **Step 12: final restore**

```text
临时 AQC 删除
原 CRD/webhook 恢复
原 agent/entrypoint/schemata 恢复
CNC 恢复
临时文件清理
fresh read-back
```

- [ ] **Step 13: 单测中性命名和 legacy/dead-code 门禁**

```bash
if grep -R -n --include='*_test.go' 'sandbox' \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/rdt/cat \
  pkg/agent/qrm-plugins/resctrl \
  pkg/config/agent/dynamic/adminqos/qrm \
  pkg/util/resctrl \
  pkg/webhook/validating/adminqosconfiguration; then
  exit 1
fi

if grep -R -n \
  --include='*.go' --include='*.yaml' --include='*.yml' \
  -E 'CATAllocationGroup|AllocationGroups|allocationGroups|CATNonOverlapConstraint|NonOverlapConstraints|nonOverlapConstraints|ResctrlObsoleteSharedSubgroupPrefix|func NormalizeClosID' \
  pkg cmd config; then
  exit 1
fi

go test ./... -count=1
go vet ./...
golangci-lint run ./...
```

人工核对并删除旧 flat CAT converter、group packing、group rollback、constraint
builder、compatibility alias/parser、legacy feature gate 和仅被旧路径引用的 helper。
不允许通过 allowlist 跳过新产生的 unused/dead-code 报告。

- [ ] **Step 14: 更新报告并提交**

报告必须区分：

```text
通过
失败
外部阻塞
恢复状态
unrelated baseline failure
```

```bash
git add qrm-bulkhead-test-artifacts/cat-policy-2605-20260814
git commit -m "test(qrm): verify cat exclusive policy"
```

## 最终验收

- [ ] 所有 CAT-owned API 字段位于 `CATPolicy`。
- [ ] `defaultCATWays` 保留为 non-root fallback。
- [ ] `closCATWays` 支持 flags/env，AQC omitted/non-empty/empty-map 三态正确。
- [ ] `DefaultPlacement` flags/env 默认 `*`/`low`。
- [ ] AQC exclusive omitted/non-empty/empty 三态正确。
- [ ] 不存在 active allocation-group/constraint 代码。
- [ ] selector 只支持 exact/trailing-prefix。
- [ ] exclusive 只接受 canonical exact ID。
- [ ] exclusive 与所有其他 configured CLOS non-overlap。
- [ ] ways/placement 由显式 per-CLOS 或 default fallback 决定。
- [ ] API server 真实拒绝非法 policy，不只依赖 CEL 单测。
- [ ] unsafe transition 零写入。
- [ ] enabled/disabled transaction 均 compare-before-restore，不覆盖外部新 L3 或
  非 CAT schemata。
- [ ] 无额外 lifecycle state；CPUPlugin state
  不保存 CAT activation。
- [ ] pending activation set 和 active repair set 分离。
- [ ] memory/CAT 缺失目录统一通过 `EnsurePendingCLOS` 创建并复用同一
  generation，不存在 CAT plugin 私有 `Mkdir`。
- [ ] memory resctrl Create/ReconcileClos 也委托同一 lifecycle service，生产
  代码只保留一个 CLOS mkdir/identity-bind/phase owner。
- [ ] 底层 `pkg/util/external/rdt` transaction API 只依赖 physical DTO，不 import
  agent resctrl package，不产生 Go import cycle。
- [ ] task migration 在真实外部 consumer 写 tasks 时复核版本化 token。
- [ ] deployment gate 拒绝旧纯 CLOS string consumer 与新 producer 混用。
- [ ] periodic repair 强制读取真实 schemata。
- [ ] `enableCAT=false` 收敛所有 managed configured/owned non-root CLOS 的 L3
  mask 到 `CBMMask`。
- [ ] disabled managed reset 保留 MB/其他 schemata，不写 root、不创建 ownership、
  不修改 foreign unowned CLOS。
- [ ] disabled reset 每轮从当前 resctrl 快照重新计算，并通过真实
  `SchemataCoordinator` physical lock 与 MBA/lifecycle 串行。
- [ ] kubelet physical `shared-*` 映射到 canonical `share-*`，用户配置拒绝
  `shared-*`。
- [ ] deployment gate 强制 `enableCPUList=false`；本期不验收 CPUListPlugin 的
  `shared-XX/cpus` 兼容。
- [ ] disabled reset 开始和发布 epoch 前都验证 root `CBMMask`。
- [ ] disabled-ready reset epoch 防止 reset 期间提前迁移 tasks。
- [ ] `share-XX`/`shared-XX` collision 在 enabled 模式 fail-closed。
- [ ] deployment gate 阻止不兼容 AQC。
- [ ] standard 3 rounds、high-churn 5 rounds、旧 agent rollback 和 final restore 全部通过。
- [ ] 新增和修改的 CAT 单测不包含 `sandbox`。
- [ ] legacy symbol、compatibility path、unused helper 和死代码零残留。
