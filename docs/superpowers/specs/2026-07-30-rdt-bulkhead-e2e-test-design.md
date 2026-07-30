# RDT Bulkhead E2E 测试方案

## 目标

本方案验证 `design/rdt-bulkhead-resource-plugins` 中新增的 RDT bulkhead 能力在真实 cgroup v1 + resctrl 节点上的组合行为。测试复用现有 QRM bulkhead E2E 主链路，在 `reset`、`target`、标准 3 轮和 high-churn 5 轮中增加 RDT 专项断言。

测试边界采用共享节点安全模式：

- 允许脚本创建临时节点级 `AdminQoSConfiguration`，并让目标节点在 E2E 期间命中该临时对象。
- 如果已有其他节点级 AQC 覆盖目标节点，脚本先创建临时 AQC，再从这些节点级 AQC 的 `ephemeralSelector.nodeNames` 中移除目标节点，并在恢复阶段完整还原。
- 只覆盖 `BulkheadRDTConfig` 中 CPUList 和 CAT 开关转换，以及 CAT ways 配置。
- 不执行 `RDTConfig.DisableRDT=true`。
- 不 cordon 节点。
- 不删除、重命名或手工创建业务 CLOS。
- 不手工写 `tasks`、`mon_groups` 或迁移 task。
- 结束时恢复完整原始 AQC 对象。

## 代码事实

RDT 控制面由三个 owner 组成：

- `resctrl.Manager` 是 CLOS 生命周期 owner，负责默认 CLOS、期望 CLOS、常规清理和 `DisableRDT` 强制撤销。
- `rdt_cpulist` 插件写 CLOS 的 `cpu_list`；非空 target 的 CLOS 尚未存在时，可通过共享 per-CLOS 锁按需创建，但不删除或重命名 CLOS。
- `rdt_cat` 插件只写 schemata 的 `L3:` 行，使用 `SchemataCoordinator` 与 CLOS 生命周期共享 per-CLOS 锁，避免旧 schemata 快照覆盖重建后的 CLOS。

本 E2E 不触发 `DisableRDT`，因此只读观察 CLOS 生命周期是否稳定，不把 CLOS 强制删除作为通过条件。

`rdt_cpulist` 的目标来自 `CPUSetPartitionView`：

- `dedicated` CLOS 使用 dedicated CPUSet。
- `share-*` CLOS 使用静态 `CPUSetPoolToSharedSubgroup` 和 `DefaultSharedSubgroup` 解析后的 share pool CPUSet 并集；旧式 `shared-*` 仅作为兼容输入识别并归一化。
- `reclaim` 不作为 CPUList 写入对象。

`rdt_cat` 对当前识别出的全部受管 CLOS 生效：

- `dedicated`
- `reclaim`
- 本轮存在并受配置管理的 `share-*`

普通 L3 CAT 节点验证 `L3:` mask。CDP 或 no-RDT 节点验证安全降级，不要求写 `L3CODE`、`L3DATA` 或伪造 resctrl 目录。

## 环境前提

节点必须满足现有 bulkhead E2E 前提：

- cgroup v1 cpuset 可读写。
- QRM agent 可由 adapter 构建产物替换，或用户明确选择验证节点现有 agent。
- 可访问 Kubernetes API，并可创建、删除测试 Pod。
- 可读取 QRM `runsv`、agent pid、`PORT0`、cmdline、env、healthz、QRM 日志和 resctrl 文件。

RDT 能力不固定。脚本必须在 preflight 阶段分类：

```text
RDT_CAPABILITY=cat
RDT_CAPABILITY=cdp
RDT_CAPABILITY=no_rdt
RDT_CAPABILITY=invalid
```

分类规则：

- `cat`：`/sys/fs/resctrl` 已挂载，存在 `info/L3/cbm_mask`、`info/L3/min_cbm_bits`，根 `schemata` 含 `L3:` domain 列表，且不存在 CDP 的 `L3CODE/L3DATA` 替代模式。
- `cdp`：存在 `info/L3CODE/cbm_mask` 和 `info/L3DATA/cbm_mask`，普通 `L3:` 更新不适用。
- `no_rdt`：resctrl 未挂载，或没有 L3 CAT 能力文件。
- `invalid`：能力文件存在但无法解析，例如 `cbm_mask=0`、`min_cbm_bits<=0`、根 `schemata` 缺少 domain 或格式非法。

`invalid` 直接阻断 RDT 专项测试。`cdp` 和 `no_rdt` 可继续跑既有 bulkhead 主链路，但 RDT CAT 判定走降级断言。

## CRD 准备

RDT AQC 字段依赖最新 `AdminQoSConfiguration` CRD。E2E 不允许在旧 CRD 上继续执行，因为旧 schema 可能拒绝 patch，也可能通过 pruning 丢弃 `bulkheadRDTConfig` 字段，导致 agent 实际看不到 CPUList/CAT 配置。

### 生成 CRD

在 `katalyst-api` 的 RDT 分支生成 CRD：

```bash
hack/tools/bin/controller-gen \
  paths=./pkg/apis/... \
  crd:crdVersions=v1,allowDangerousTypes=true \
  output:crd:dir=./config/crd/bases
```

如果仓库提供 `make generate-manifests`，也可以使用仓库命令，但最终产物必须包含：

```text
config/crd/bases/config.katalyst.kubewharf.io_adminqosconfigurations.yaml
```

本地生成后必须确认字段和校验规则：

```bash
grep -n "bulkheadRDTConfig\|enableCPUList\|enableCAT\|defaultCATWays\|closCATWays\|disableRDT" \
  config/crd/bases/config.katalyst.kubewharf.io_adminqosconfigurations.yaml

grep -n "defaultCATWays must be specified\|all CLOS CAT ways must be greater than 0\|minimum: 1" \
  config/crd/bases/config.katalyst.kubewharf.io_adminqosconfigurations.yaml
```

### 同步 deploy CRD

将 API 生成的 AQC CRD 同步到 deploy chart：

```bash
cp \
  <katalyst-api>/config/crd/bases/config.katalyst.kubewharf.io_adminqosconfigurations.yaml \
  <katalyst-deploy>/helm/controller/controller-manager/crds/kcc/config.katalyst.kubewharf.io_adminqosconfigurations.yaml
```

检查 deploy diff 只包含 AQC CRD schema 更新：

```bash
git diff -- helm/controller/controller-manager/crds/kcc/config.katalyst.kubewharf.io_adminqosconfigurations.yaml
```

### 更新测试集群 CRD

E2E 测试集群可直接 apply 更新后的 CRD：

```bash
kubectl apply -f \
  <katalyst-deploy>/helm/controller/controller-manager/crds/kcc/config.katalyst.kubewharf.io_adminqosconfigurations.yaml
```

如果集群只能通过 Helm 或发布系统更新 controller-manager chart，则必须先完成该发布步骤，再执行 E2E。

### 集群 CRD 校验

E2E preflight 前必须验证集群 CRD 已更新：

```bash
kubectl get crd adminqosconfigurations.config.katalyst.kubewharf.io -o yaml \
  | grep -n "bulkheadRDTConfig\|enableCPUList\|enableCAT\|defaultCATWays\|closCATWays\|disableRDT"

kubectl get crd adminqosconfigurations.config.katalyst.kubewharf.io -o yaml \
  | grep -n "defaultCATWays must be specified\|all CLOS CAT ways must be greater than 0\|minimum: 1"
```

字段或校验规则缺失时，脚本必须输出：

```text
RDT_AQC_CRD_NOT_READY
```

并以非零退出。

### Server-side dry-run

正式创建临时 AQC 前必须做 server-side dry-run。合法配置应通过：

```bash
kubectl apply --dry-run=server -f rdt-e2e-aqc.valid.yaml -o yaml >/dev/null
```

以下非法配置必须被拒绝：

```yaml
bulkheadRDTConfig:
  enableCAT: true
```

```yaml
bulkheadRDTConfig:
  enableCAT: true
  defaultCATWays: 0
```

```yaml
bulkheadRDTConfig:
  enableCAT: true
  defaultCATWays: 4
  closCATWays:
    reclaim: 0
```

如果非法配置没有被 CRD validation 拒绝，脚本必须输出：

```text
RDT_AQC_CRD_VALIDATION_NOT_ENFORCED
```

并停止 E2E。该失败说明集群 CRD 仍不是期望版本，不能继续通过 patch AQC 验证 runtime 行为。

## 脚本扩展

在现有 `qrm-bulkhead-e2e` 脚本集上新增或扩展以下能力。

### AQC patch helper

新增 `scripts/patch_rdt_aqc.sh`，负责：

- 验证集群 `AdminQoSConfiguration` CRD 已包含 RDT 字段和 CAT ways 校验规则。
- 通过 server-side dry-run 验证合法临时 AQC 可创建、非法 CAT 配置会被拒绝。
- 定位当前节点生效的 `AdminQoSConfiguration`，并保存原始 CNC target。
- 保存原始生效 AQC 到远端日志目录：`aqc.original.yaml`。
- 创建临时节点级 AQC：`rdt-e2e-<node>-<run_tag>`。
- 发现所有 `ephemeralSelector.nodeNames` 包含目标节点的其他节点级 AQC，保存完整备份，并在临时 AQC 创建后让这些对象不再覆盖目标节点。
- 按阶段更新临时 AQC：
  - `rdt-plugin-reset`
  - `rdt-target`
  - `rdt-cpulist-off-cat-on`
  - `rdt-cpulist-on-cat-off`
  - `rdt-restore-original`
- 每次 patch 后等待 dynamic config observer 生效。
- 输出 patch 前后摘要，不输出完整敏感对象。

临时 AQC 的选择字段：

```yaml
spec:
  ephemeralSelector:
    nodeNames:
    - <node_name>
    lastDuration: 2h
  priority: <higher_than_existing_node_level_aqc>
```

临时 AQC 中需要更新的 RDT 字段：

```yaml
spec:
  config:
    qrmPluginConfig:
      rdtConfig:
        disableRDT: false
      cpuPluginConfig:
        bulkheadConfig:
          bulkheadRDTConfig:
            enableCPUList: <bool>
            enableCAT: <bool>
            defaultCATWays: <positive int>
            closCATWays:
              dedicated: <positive int>
              reclaim: <positive int>
              share-xx: <positive int>
```

如果原生效 AQC 中 `rdtConfig.disableRDT=true`，脚本必须失败并要求人工确认。共享节点流程不能在原始 RDT 被全局禁用的节点上自动启用。

节点级 AQC 冲突处理规则：

1. 先读取 `cnc/<node>` 的 `status.katalystCustomConfigList`，记录当前 `adminqosconfigurations` target 的 namespace、name 和 hash。
2. 列出所有 AQC，筛选 `spec.ephemeralSelector.nodeNames` 包含目标节点且不是本轮临时 AQC 的对象。
3. 将这些对象完整保存到 `aqc.conflict.<namespace>.<name>.yaml`。
4. 创建临时 AQC，priority 必须大于已发现节点级 AQC 的最大 priority。
5. 等待 `cnc/<node>` 指向临时 AQC；如果超时，再按备份逐个 patch 冲突 AQC，使其不再包含目标节点。
6. 对包含多个 nodeName 的冲突 AQC，只删除目标节点，保留其他 nodeName。
7. 对只包含目标节点的冲突 AQC，不允许把 `ephemeralSelector` 清空成可能扩大匹配面的对象；脚本必须采用安全让路方式：保留完整备份，并将 `nodeNames` 替换为本轮唯一的不可命中占位节点名，例如 `__rdt-e2e-disabled-<run_tag>-<node>`。恢复阶段用完整备份还原。
8. 每次修改冲突 AQC 后，都必须等待 `cnc/<node>` 的 adminqos target hash 或 target name 变化；如果仍未指向临时 AQC，停止测试并进入恢复。

恢复顺序：

1. 停止新增 workload，清理本轮测试 Pod 并等待 state drain。
2. 应用所有 `aqc.conflict.*.yaml` 备份，先恢复其他节点级 AQC。
3. 删除本轮临时 AQC。
4. 等待 `cnc/<node>` 回到 preflight 记录的原始 adminqos target，或回到恢复后的节点级 AQC target。
5. 运行 final node check 和 RDT restore check。
6. 任一恢复步骤失败时，保留临时 AQC、冲突 AQC 备份、CNC snapshot 和 QRM 日志，整体判失败。

### RDT node check

新增 `scripts/check_rdt_state.sh`，由 `qrm_node_check.sh` 调用或由阶段脚本显式调用。它输出结构化行：

```text
RDT_CAPABILITY=<cat|cdp|no_rdt|invalid>
RDT_CLOS_LIST clos=<name> exists=<true|false> tasks=<n> cpu_list=<value>
RDT_SCHEMATA clos=<name> resource=L3 value=<domains>
RDT_CPULIST_OK clos=<name> expected=<mask> actual=<mask>
RDT_CAT_OK clos=<name> ways=<n> expected=<domain-mask> actual=<domain-mask>
RDT_CAT_SKIP reason=<cdp|no_rdt>
RDT_MB_UNCHANGED clos=<name>
RDT_AQC_OBSERVED cpuList=<bool> cat=<bool> defaultWays=<n>
RDT_RESTORE_OK
```

它必须读取并保存：

- 每个受管 CLOS 的 `cpu_list`
- 每个受管 CLOS 的 `tasks`
- 每个受管 CLOS 的完整 `schemata`
- 根 `schemata`
- `info/L3/cbm_mask`
- `info/L3/min_cbm_bits`
- CDP 能力文件是否存在

受管 CLOS 集合由现场和配置共同决定：

- 固定候选：`dedicated`、`reclaim`
- 现场存在的 `share-*`
- 由 `CPUSetPoolToSharedSubgroup`、`DefaultSharedSubgroup` 和测试 workload 推导出的 `share-*`

### 预期值计算

CPUList 预期值：

- `dedicated`：从本轮 dedicated 测试 Pod 或 CPU state 对应的 dedicated CPUSet 推导。
- `share-*`：把映射到同一 subgroup 的 share pool CPUSet 做 union。
- 当 CPUList disabled 时，受管 `dedicated` 与 `share-*` 的 `cpu_list` 预期为空字符串。

CAT 预期值：

- 从 `cbm_mask` 和 `min_cbm_bits` 读取每个 L3 domain 的能力。
- `ways` 必须满足 `min_cbm_bits <= ways <= bitcount(cbm_mask)`。
- 选择连续低位可用 mask，格式与 core 中 `contiguousMask` 和 `formatSchemataValues` 一致。
- `closCATWays` 优先于 `defaultCATWays`。
- 直接 CLOS key 优先于 pool key。

如果任一 CAT ways 超出节点能力，`rdt-target` 阶段必须失败，不继续压测。

## 阶段流程

### Preflight

执行前置采集：

1. 验证集群 AQC CRD 已包含 RDT 字段和 CAT validation。
2. 执行 server-side dry-run，确认合法 AQC 通过、非法 CAT 配置被拒绝。
3. 记录 agent pid、runsv pid、`PORT0`、cmdline、env。
4. 记录 `/proc/<agent_pid>/exe` 和 `/proc/<runsv_pid>/root/opt/tiger/katalyst/agent` SHA。
5. 备份 `real_run.sh`，确认既有 bulkhead baseline flags 完整。
6. 备份原始 AQC 到远端日志目录。
7. 备份所有覆盖目标节点的其他节点级 AQC。
8. 创建临时节点级 AQC，并确认 `cnc/<node>` 指向它；如被其他节点级 AQC 覆盖，则按冲突处理规则让路。
9. 运行 RDT capability 探测。
10. 保存所有受管 CLOS 的 `tasks`、`cpu_list`、`schemata`。
11. 运行既有 `qrm_node_check.sh`，要求 healthz ready。

Preflight 在临时 AQC 创建前失败时不修改 AQC；在临时 AQC 创建后失败时必须进入 Restore。

### Plugin reset

Patch AQC：

```text
enableCPUList=false
enableCAT=false
disableRDT=false
```

预期：

- `rdt_cpulist` disabled handler 执行后，`dedicated` 与受管 `share-*` 的 `cpu_list` 为空。
- 普通 CAT 节点上，曾被 CAT 插件 active 的 CLOS 回滚到 `defaultCATWays` 对应 mask；如果插件此前未 active，只要求不报错并保持 healthz ready。
- CDP/no-RDT 节点输出 `RDT_CAT_SKIP`，不阻断既有 bulkhead reset。
- CLOS 目录集合不减少。
- preflight 中存在的非测试 task 不丢失。
- `MB:` 行不变。

如果 CPUList 未在重试窗口内清空，阶段失败。

### Target

Patch AQC：

```text
enableCPUList=true
enableCAT=true
defaultCATWays=<DEFAULT_WAYS>
closCATWays:
  dedicated: <DEDICATED_WAYS>
  reclaim: <RECLAIM_WAYS>
  share-xx: <SHARED_WAYS>
```

其中 `share-xx` 来自现场受管 shared CLOS；旧式 `shared-*` 仅作为兼容输入识别并归一化。若没有可识别 shared CLOS，阶段输出 `RDT_SHARED_SKIP reason=no_managed_shared_clos`，但 dedicated 与 reclaim 仍必须验证。

预期：

- `dedicated/cpu_list` 等于 dedicated CPUSet。
- 每个受管 `share-*/cpu_list` 等于对应 share pool CPUSet union。
- 普通 CAT 节点上，`dedicated`、`reclaim`、受管 `share-*` 的 `L3:` 均等于对应 ways mask。
- `MB:` 行不变。
- 既有 cpuset topology、cpuset_mems、workqueue、system_service 检查仍通过。
- healthz ready。

### CPUList off / CAT on

Patch AQC：

```text
enableCPUList=false
enableCAT=true
```

预期：

- `dedicated` 和受管 `share-*` 的 `cpu_list` 清空。
- 普通 CAT 节点上，`dedicated`、`reclaim`、受管 `share-*` 的 `L3:` 仍保持 target mask。
- 目录和 tasks 不变。
- healthz ready。

该阶段证明 CPUList disabled 不影响 CAT。

### CPUList on / CAT off

Patch AQC：

```text
enableCPUList=true
enableCAT=false
```

预期：

- `dedicated` 与受管 `share-*` 的 `cpu_list` 恢复为当前 CPUSet view 目标。
- 普通 CAT 节点上，所有曾 active 的受管 CLOS 回滚到 default ways。
- `MB:` 行不变。
- healthz ready。

该阶段证明 CAT disabled 不影响 CPUList。

### Both on

Patch AQC 回到 target：

```text
enableCPUList=true
enableCAT=true
```

预期：

- CPUList 与 CAT 均恢复 target 状态。
- 进入 workload 压测前必须连续两次 node check 通过，避免 observer 和 periodical handler 短暂未收敛。

### 标准 3 轮

复用现有 `standard_e2e_3rounds.sh`：

```text
stable_12 -> delete -> recreate_12 -> delete -> early_12 -> delete -> postcheck
```

每个 node check 追加 RDT 断言：

- CPUList 随当前 CPUSet view 收敛。
- 普通 CAT 节点上 CAT mask 保持 target。
- CDP/no-RDT 节点保持 `RDT_CAT_SKIP` 且无 plugin error。
- CLOS 目录不被误删。
- `MB:` 行不变。

标准阶段失败时不进入 high-churn，直接 restore。

### High-churn 5 轮

复用现有 `high_churn_5rounds.sh`：

```text
stable_12 -> delete -> immediate recreate_12 -> delete -> early_12 -> sleep 3 -> delete
```

重点观察：

- Pod delete/recreate 高频过程中 `cpu_list` 不保留旧 epoch 的目标。
- `share-*` CPUSet union 变化后能在重试窗口内收敛。
- CAT 在 CLOS 目录保持存在时不重复写入错误目标。
- healthz 不出现由 `rdt_cpulist` 或 `rdt_cat` 导致的 not ready。

如果出现 `NODE_CHECK_ATTEMPT_FAIL`，但后续重试内出现 `NODE_CHECK_OK strict=true`，报告归类为短暂收敛重试。若最终 `NODE_CHECK_FAIL` 或阶段 `rc!=0`，阶段失败。

### Restore

恢复原始 AQC 选择关系，而不是只 patch 回两个字段。

恢复后必须确认：

- 所有被让路的节点级 AQC 已由完整备份恢复。
- 本轮临时 AQC 已删除。
- `cnc/<node>` 已回到 preflight 记录的原始 adminqos target，或回到恢复后的节点级 AQC target。
- dynamic config 已观测到恢复后的 AQC。
- healthz ready。
- CLOS 目录集合不比 preflight 少。
- preflight 中记录的非测试 tasks 仍在原 CLOS 或按原配置可解释。
- 如果原始 AQC 未启用 CAT，普通 CAT 节点上 `schemata` 回到 preflight 记录值或 default rollback 值，两者差异必须解释。
- 如果原始 AQC 已启用 CAT，普通 CAT 节点上 `schemata` 回到原 AQC 对应目标。
- `MB:` 行与 preflight 一致。
- 测试 Pod remaining=0，QRM state drain 完成。

Restore 失败时整体失败，即使标准和 high-churn 已通过。

## 通过标准

只有同时满足以下条件，才能报告 RDT 组合 E2E 通过：

- Preflight、Plugin reset、Target、两个独立开关转换、Both on、标准 3 轮、high-churn 5 轮和 Restore 均 `rc=0`。
- 集群 CRD 准备检查通过，server-side dry-run 证明合法配置可创建且非法 CAT 配置会被拒绝。
- 所有阶段 healthz ready。
- 既有 cpuset topology、cpuset_mems、workqueue、system_service 检查通过。
- 标准和 high-churn 的 stable/recreate 阶段没有业务 Pod `Failed`。
- 普通 CAT 节点上所有受管 CLOS 的 CAT 目标均匹配。
- CDP/no-RDT 节点上出现明确 `RDT_CAT_SKIP`，且无 CAT plugin 错误、无 healthz 失败。
- CPUList 在 enabled、disabled、re-enabled 三类状态均符合预期。
- `MB:` 行不被 CAT 测试修改。
- CLOS 目录没有被 RDT 插件误删。
- Restore 成功，临时 AQC 已删除，其他节点级 AQC 已恢复。
- 日志包远端和本地 sha256 校验一致。

## 失败判定

以下任一情况直接判失败：

- 脚本尝试或实际设置 `disableRDT=true`。
- 集群 AQC CRD 缺少 `bulkheadRDTConfig`、`defaultCATWays`、`closCATWays` 或 CAT validation。
- server-side dry-run 未拒绝非法 CAT 配置。
- AQC patch 后 observer 未在超时内生效。
- `rdt_cpulist` 或 `rdt_cat` 导致 healthz not ready。
- CPUList disabled 后 `cpu_list` 未清空。
- CPUList enabled 后 `cpu_list` 与目标 CPUSet 不一致，且重试耗尽。
- 普通 CAT 节点上 `L3:` mask 与预期不一致。
- CAT 更新修改了 `MB:` 行。
- CDP 节点上脚本尝试写 `L3CODE` 或 `L3DATA`。
- no-RDT 节点上脚本创建伪 resctrl 目录或把缺失能力当作普通通过。
- CLOS 目录减少，且不是测试前已声明的外部清理行为。
- Restore 失败、临时 AQC 未删除、冲突节点级 AQC 未恢复，或 `cnc/<node>` 未回到可解释 target。

## 日志包

日志包必须包含：

```text
summary.log
crd.preflight.yaml
crd.validation.log
aqc.original.yaml
aqc.temporary.yaml
aqc.conflict.*.yaml
cnc.preflight.yaml
cnc.phase-*.yaml
cnc.restore.yaml
aqc.phase-*.yaml
rdt_capability.log
rdt_preflight_snapshot/
rdt_phase_snapshots/
rdt_restore_diff/
qrm_node_check_*.log
standard_3rounds.log
high_churn_5rounds.log
failed_pod_diagnostics/
agent_sha.txt
real_run_sha.txt
script_sha.txt
```

每个 RDT snapshot 至少包含：

```text
clos.list
<clos>/tasks
<clos>/cpu_list
<clos>/schemata
root.schemata
info_L3_cbm_mask
info_L3_min_cbm_bits
capability.json
expected.json
actual.json
diff.txt
```

日志打包继续使用 `package_e2e_logs.sh`。拉回本地后必须执行：

```bash
tar -tzf qrm_bulkhead_<RUN_TAG>_logs.tgz >/dev/null
shasum -a 256 qrm_bulkhead_<RUN_TAG>_logs.tgz
```

本地 sha 必须等于远端 `REMOTE_LOG_SHA`。

## 报告模板

最终报告必须包含：

```text
agent sha256
core commit
adapter commit
api commit
RUN_TAG / STD_PREFIX / CH_PREFIX
RDT_CAPABILITY
CRD 版本与 validation 证据
原始 AQC 摘要
临时 AQC 名称、priority、lastDuration
被让路的节点级 AQC 列表及恢复结果
每个 PHASE_DONE ... rc=<N>
CPUList dedicated/share-* enabled/disabled/re-enabled 证据
CAT dedicated/share-*/reclaim target/rollback 证据
MB_UNCHANGED 证据
标准 3 轮结果
high-churn 5 轮结果
Restore 结果
NODE_CHECK_ATTEMPT_FAIL 的收敛归类
Failed Pod 诊断结论
远端日志包路径、大小、sha256
本地 tar 与 sha256 校验结果
```

如果未替换 agent，只能报告“验证节点现有 agent”，并写明节点现有 agent SHA；不得声称已验证最新 `design/rdt-bulkhead-resource-plugins` 代码。

## 本地脚本验证

脚本实现完成后，本地至少运行：

```bash
bash -n scripts/*.sh
env -u PYTHONHOME -u PYTHONPATH python3 -m py_compile scripts/*.py
env -u PYTHONHOME -u PYTHONPATH python3 scripts/tests/test_bulkhead_guards.py
```

新增 RDT 解析逻辑需要 dry fixture 覆盖：

- 普通 CAT：多 domain `L3:` mask 计算。
- CDP：输出 `RDT_CAT_SKIP reason=cdp`。
- no-RDT：输出 `RDT_CAT_SKIP reason=no_rdt`。
- CPUList enabled/disabled。
- `MB:` 行不变。
- AQC restore 使用完整对象。
