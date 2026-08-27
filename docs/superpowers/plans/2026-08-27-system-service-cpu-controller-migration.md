# System-Service CPU Controller Migration Implementation Plan

## Goal

Extend the Bulkhead `system_service` plugin so that, on cgroup v1 hosts, it
migrates eligible system processes found in either the cpuset-controller root
or the cpu-controller root into the same configured `BulkheadSystemRelPath`.
The cpu-controller target must exist before a process is attached. Disabling
the plugin must return each managed controller membership to that controller's
root.

## Architecture

`system_service` owns candidate discovery, classification, controller-specific
membership intent, and retry semantics. `cgroup/client` owns safe controller
mount discovery, target creation, and controller-local `cgroup.procs` writes.
`cpuset_topology` remains the only owner of cpuset topology materialization and
the cpuset applied-view identity proof. No cpu-controller topology proof is
introduced.

## Tech Stack

Go, Linux cgroup v1/v2, `golang.org/x/sys/unix`, QRM Bulkhead plugins,
testify and standard Go tests.

## Baseline and Authority

- `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/systemservice/plugin.go`
  currently scans and attaches only through the cpuset hierarchy.
- `pkg/util/cgroup/client/client.go` owns cpuset-relative cgroup operations.
- `pkg/util/cgroup/client/identity_attach_linux.go` is the existing safe
  identity-bound directory traversal and attach precedent.
- `pkg/util/cgroup/common/path.go` currently derives v1 controller paths from
  controller names and therefore cannot alone support combined controller
  mounts such as `cpu,cpuacct`.
- `cmd/katalyst-agent/app/options/qrm/bulkhead/bulkhead.go` normalizes the
  configured system relative path but does not reject dot-path traversal.

## Compatibility Boundary

- cgroup v2 remains one unified membership hierarchy: it is scanned and
  attached exactly once.
- cgroup v1 scans cpuset and cpu controllers independently when both are
  available; no process is obtained through a global `/proc` scan.
- A cpu-controller-only candidate changes only cpu-controller membership; a
  cpuset-only candidate changes only cpuset-controller membership.
- A candidate present in both controller roots is classified once, then
  converged independently in both controllers.
- cpuset target authorization still requires the existing applied-view
  CPUSet/device/inode proof. cpu target creation does not manufacture or
  consume a cpuset proof.
- cpu migration remains active when cpuset topology is disabled or its target
  is not yet authorized; each controller converges independently.
- No API, dynamic-config, checkpoint, or metric schema outside the existing
  metric label set is changed.

## Verification

```bash
gofmt -w pkg/util/cgroup/common/*.go pkg/util/cgroup/client/*.go \
  pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/systemservice/*.go
go test ./pkg/util/cgroup/common -count=1
go test ./pkg/util/cgroup/client -count=1
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/systemservice -count=1
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/... -count=1
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/... -count=1
go test -race ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/... -count=1
```

## Plan Basis

### Facts

- Existing root discovery reads cpuset root `cgroup.procs` and optionally
  `tasks`; all enabled-path attaches target cpuset.
- Existing cgroup-file dispatch treats `cgroup.procs` as a cpuset file because
  only names beginning with `cpu.` select the cpu subsystem.
- Existing identity-bound attach opens the controller root and every relative
  component with `O_NOFOLLOW`, verifies identity, then writes through the
  pinned target directory.
- `BulkheadSystemRelPath` is configurable and only strips surrounding slashes;
  it can still carry a non-canonical relative path.
- A v1 cpu controller may be mounted in a combined hierarchy rather than at a
  fixed `/sys/fs/cgroup/cpu` directory.

### Invariants

- Every controller write uses a safe, canonical relative path and a pinned
  directory descriptor; no `MkdirAll` or `OpenFile` call may resolve an
  unchecked `targetRel`.
- A PID/TID source records controller-local `taskOnly` state. A leader source
  in one controller cannot weaken task-only handling for another controller.
- A successful attach in one controller never marks another controller
  complete. Retries operate only on memberships that remain in their source
  root or target.
- `ESRCH` satisfies only the attempted controller write for that listed PID;
  it does not suppress a pending write to another controller.
- A disabled transition completes only after every listed controller target
  membership has been returned to its corresponding root, or has exited.

## Architecture Integrity Lens

- **Canonical owner:** `cgroup/client` owns controller-specific filesystem
  mechanics; `system_service` owns migration policy and convergence.
- **No duplicated authority:** cpuset applied-view proof authorizes cpuset
  placement only. cpu target existence is a membership prerequisite, not a
  second topology authority.
- **Higher-level simplification:** use one controller-aware client capability
  instead of duplicating path selection and unsafe file writes in the plugin.
- **Retirement:** remove the plugin's implicit “cpuset root is the only root”
  assumption and related comments/tests. Retain the existing cpuset-only
  `AttachPID` API for existing callers.
- **Verdict:** proceed with a narrow controller-aware capability; do not widen
  topology models or add a compatibility fallback that silently skips cpu.

## Plan Pressure Test

- **Owner / contract / retirement:** controller mechanics move into
  `cgroup/client`; plugin remains policy owner; no old cpu fallback remains.
- **Architecture integrity:** shared controller resolver and pinned attach path
  prevent the plugin from becoming a second cgroup filesystem client.
- **Verification scope:** unit tests cover mount resolution, safe traversal,
  source semantics, partial success, and reset convergence.
- **Task executability:** each task has focused RED/GREEN tests and package
  commands.
- **Pressure result:** proceed.

## Plan-Time Complexity Check

- **Target files:** `systemservice/plugin.go` is already large and contains
  migration policy; cgroup path/FD mechanics belong in `cgroup/client`.
- **Add-in-place risk:** adding mount parsing and secure directory traversal to
  the plugin would mix policy with Linux filesystem authority.
- **Recommendation:** add a small controller-aware Linux client file and keep
  plugin changes limited to candidate/source and orchestration logic.

## Tasks

### Task 1: Add explicit cgroup controller resolution

**Files**

- Create `pkg/util/cgroup/common/controller_mount_linux.go`
- Create `pkg/util/cgroup/common/controller_mount_linux_test.go`

**Why**

Locate the actual mounted hierarchy for `cpu` and `cpuset` on v1, including
combined mounts, instead of assuming the directory name matches the controller.

**Impact / Compatibility**

The resolver is used only by the new controller-aware migration path. Existing
relative-path helpers remain unchanged. On v2 both controller queries resolve
to the unified cgroup root.

**Implementation**

1. Add a small exported resolver:

   ```go
   type ControllerMount struct {
       Root    string
       Unified bool
   }

   func ResolveControllerMount(subsys string) (ControllerMount, error)
   ```

2. On v2, return `{Root: CgroupFSMountPoint, Unified: true}` for both `cpu`
   and `cpuset`.
3. On v1, parse `/proc/self/mountinfo`; for each `cgroup` filesystem mount,
   parse the post-` - ` mount options and select the mount whose super options
   contain the requested controller token. A mount with `cpu,cpuacct` satisfies
   a request for `cpu`.
4. Return a typed “controller mount unavailable” error only when no cgroup
   mount advertises that controller. Treat malformed mountinfo and I/O errors
   as errors, not as absence.
5. Keep parsing behind package-level injectable reader functions so tests use
   literal mountinfo fixtures.

**Tests**

- v1 standalone `cpu` and `cpuset` mounts resolve independently.
- v1 combined `cpu,cpuacct` mount resolves `cpu`.
- v2 returns one identical unified root for both controllers.
- unavailable controller returns the typed absence error.
- malformed mountinfo returns an error, not absence.

**Verify RED/GREEN**

```bash
go test ./pkg/util/cgroup/common -run 'TestResolveControllerMount' -count=1
```

### Task 2: Add safe controller-local target operations

**Files**

- Modify `pkg/util/cgroup/client/client.go`
- Modify `pkg/util/cgroup/client/cache.go`
- Modify `pkg/util/cgroup/client/fake.go`
- Create `pkg/util/cgroup/client/controller_attach_linux.go`
- Create `pkg/util/cgroup/client/controller_attach_linux_test.go`
- Modify `pkg/util/cgroup/client/cache_test.go`

**Why**

The plugin needs one cgroup-client-owned way to ensure and write a controller
target without recreating unsafe path handling.

**Impact / Compatibility**

Add an optional capability rather than expanding `CgroupClient`; existing
callers and downstream fakes keep their current interface contract.

**Implementation**

1. Add an optional interface:

   ```go
   type ControllerPIDAttacher interface {
       ControllerMount(ctx context.Context, subsys string) (cgcommon.ControllerMount, error)
       EnsureControllerDir(ctx context.Context, subsys, rel string) error
       ReadControllerFile(ctx context.Context, subsys, rel, file string) ([]byte, error)
       AttachPIDToController(ctx context.Context, subsys, rel string, pid int) error
   }
   ```

2. Move or share `safeRelComponents` so both identity-bound and
   controller-local operations reject absolute, non-clean, dot, dot-dot, and
   empty path components. `rel == ""` remains valid only for root attaches.
3. Implement `EnsureControllerDir` by resolving the controller mount, opening
   the mount root with `O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC`, and creating each
   validated component with descriptor-relative `mkdirat/openat`. Return the
   pinned target descriptor only internally; close it after ensuring.
4. For cpuset creation, retain existing parent `cpuset.mems` initialization
   semantics. For cpu creation, create directories only.
5. Implement `AttachPIDToController` using the resolved mount and the same
   descriptor-relative traversal. Open target `cgroup.procs` via `openat` with
   `O_NOFOLLOW`, then handle `EINTR`, short writes, and context cancellation as
   the existing identity-bound path does.
6. Implement `ReadControllerFile` with the same validated traversal. Permit
   only fixed leaf names used by this feature: `cgroup.procs` and `tasks`.
7. Make `NewCachedCgroupClient` preserve this optional capability in its
   wrapper, and make `FakeCgroupClient` provide safe no-op stubs.

**Tests**

- unsafe target paths are rejected by ensure, read, and attach.
- nested safe cpu target is created and receives the PID through its own
  `cgroup.procs`.
- cpuset ensure still initializes mems while cpu ensure does not attempt it.
- root attach accepts `rel == ""`.
- cache wrapper exposes and delegates the new capability.
- symlink traversal cannot redirect an attach outside the controller root.

**Verify RED/GREEN**

```bash
go test ./pkg/util/cgroup/client -run 'Test.*Controller|Test.*SafeRel' -count=1
```

### Task 3: Model migration sources per controller

**Files**

- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/systemservice/plugin.go`
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/systemservice/plugin_test.go`

**Why**

Preserve leader/task-only semantics separately for cpu and cpuset while
deduplicating classification and PID pinning.

**Impact / Compatibility**

On v2 the plugin creates one `unified` source. On v1 an unavailable cpu
controller is explicitly skipped only after `ControllerMount` reports the typed
unavailable error; all other mount-discovery errors fail the sweep.

**Implementation**

1. Replace `rootCgroupProcsPath` with a source list:

   ```go
   type controllerSource struct {
       name  string
       mount cgcommon.ControllerMount
   }
   ```

2. Build sources at plugin construction or first periodical invocation through
   the controller-aware capability:
   - v2: one `unified` source.
   - v1: `cpuset` must be available; `cpu` is included when available.
   - unexpected cpu resolver errors return a migration error and metric.
3. Replace candidate state with per-controller source metadata:

   ```go
   type candidateMembership struct {
       controller string
       taskOnly   bool
   }

   type migrationCandidate struct {
       pid         int
       memberships map[string]candidateMembership
   }
   ```

4. Read each root's `cgroup.procs`; add leaders. Read `tasks` if available and
   add only missing TIDs as `taskOnly` for that controller. Do not let a leader
   state from one controller alter another controller’s state.
5. Merge by numeric PID, sort PIDs, pin and classify once. Apply the existing
   kernel-thread and userspace policy once after the pin succeeds.

**Tests**

- cpu-only root leader is discovered and classified.
- cpuset-only behavior remains unchanged.
- a PID in both roots is pinned/classified once and has two memberships.
- a cpuset leader plus cpu task-only TID preserves cpu `taskOnly=true`.
- v2 creates one source and does not duplicate candidates.
- unavailable cpu controller preserves cpuset migration; malformed or unreadable
  discovered cpu mount fails rather than silently skipping.

**Verify RED/GREEN**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/systemservice \
  -run 'Test.*(Root|Source|Candidate|Unified)' -count=1
```

### Task 4: Implement enabled-path controller convergence

**Files**

- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/systemservice/plugin.go`
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/systemservice/plugin_test.go`

**Why**

Move every eligible membership to the matching controller target while retaining
cpuset proof enforcement and safe retries after a partial write.

**Impact / Compatibility**

The existing cpuset attach remains identity-bound. cpu attach is controller-local
and runs only after its target is securely ensured.

**Implementation**

1. Discover and merge controller candidates before target preparation so only
   controllers with actual membership participate in the sweep.
2. Preflight all controller targets needed by
   the candidate set before any PID attach:
   - cpuset membership is enabled only when `authorizedMigrationTarget`
     confirms that the target exists and matches the applied proof.
   - cpu target is ensured through `EnsureControllerDir`.
   - unified uses the existing cpuset target/proof path once.
3. For each eligible pinned candidate:
   - cpuset membership uses `AttachPIDWithIdentity`.
   - cpu membership uses `AttachPIDToController`.
   - unified uses exactly one identity-bound cpuset attach.
4. Preserve per-controller outcomes while aggregating hard failures. `ESRCH`
   completes only that membership; a hard error in another membership remains
   retryable and returns an aggregate error.
5. Emit `bulkhead_system_service_result` with a `controller` tag:
   `cpuset`, `cpu`, `unified`, or `all`. Use controller-specific reasons for
   mount discovery, target ensure, candidate listing, and attach failures.

**Tests**

- cpu-only root PID is attached only to cpu `targetRel`; cpu target is ensured
  before the first attach.
- PID in both roots attaches to both targets after one classification.
- cpu target ensure failure does not block an independently authorized cpuset
  attach; the aggregate error keeps the cpu path retryable.
- missing or invalid cpuset proof does not block cpu target creation or cpu
  membership migration.
- cpuset attach succeeds and cpu attach fails: the next tick retries cpu only,
  without reattaching cpuset.
- `ESRCH` in cpu does not suppress a required cpuset attach, and vice versa.
- a client lacking the controller-aware capability fails closed when cpu work
  is required.
- metric records identify the failing controller.

**Verify RED/GREEN**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/systemservice \
  -run 'TestPeriodicalHandler_.*(CPU|Both|Partial|Ensure|Metric)' -count=1
```

### Task 5: Implement controller-symmetric disabled reset

**Files**

- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/systemservice/plugin.go`
- Modify `pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/systemservice/plugin_test.go`

**Why**

Ensure disabling the feature cannot leave stale cpu-controller membership under
`targetRel`.

**Impact / Compatibility**

Only target root membership is reset; no child cgroup traversal is added.
Missing cpu target means no cpu target membership remains and does not block a
cpuset reset.

**Implementation**

1. Enumerate `cgroup.procs` and optional `tasks` independently from cpuset,
   cpu, or unified target directories using `ReadControllerFile`.
2. Preserve controller-local `taskOnly` membership metadata while deduplicating
   PID pinning and classification-free reset processing.
3. Attach each membership to its matching controller root:
   - cpuset via existing `AttachPID(ctx, "", pid)`.
   - cpu via `AttachPIDToController(ctx, cpu, "", pid)`.
   - unified once via existing cpuset attach.
4. A missing cpu target skips only cpu reset. Listing errors or hard attach
   errors in an existing controller keep `lastPeriodicalEnabled=true`; the next
   disabled tick retries unfinished controller memberships.
5. Report reset failures with the same controller metric tag.

**Tests**

- cpu target PID returns only to cpu root.
- PID in both targets returns to both roots.
- cpuset reset success plus cpu reset failure keeps the transition pending;
  the next disabled tick retries cpu only.
- cpu target absence does not prevent a valid cpuset reset.
- task-only semantics remain controller-specific during reset.
- v2 target reset writes once.

**Verify RED/GREEN**

```bash
go test ./pkg/agent/qrm-plugins/cpu/dynamicpolicy/bulkhead/plugins/systemservice \
  -run 'TestPeriodicalHandler_.*Reset.*(CPU|Both|Partial|Unified)' -count=1
```

### Task 6: Run regression verification and review the diff

**Files**

- No source-file additions beyond prior tasks.

**Why**

Validate that the new controller capability did not alter unrelated cgroup or
Bulkhead behavior.

**Steps**

1. Run the full commands in `## Verification`.
2. Inspect `git diff --check`.
3. Inspect `git diff --stat` and the final diff; verify no dynamic config,
   API, checkpoint, or cpuset topology ownership change was introduced.
4. Confirm comments describe controller-specific membership rather than
   scheduler affinity.

## Repair and Retirement

**Repair track**

- Root cause: system_service treats cpuset root as the sole host-system process
  source and cgroup client operations implicitly select cpuset.
- Stable repair: controller-aware safe client capability plus independent
  source/target convergence.
- Compatibility: v2 remains unified; v1 works with standalone or combined cpu
  mounts.

**Retirement track**

- Delete comments and test names that state cpuset is the only root source.
- Do not retain a path-based cpu fallback after controller mount discovery is
  available.
- Keep the existing cpuset-only public methods for unrelated callers; removal
  is outside this change.

## Risks and Rollback

- Controller membership writes are not cross-controller atomic. The explicit
  membership model and retry tests provide eventual convergence; an operation
  error remains visible and leaves the transition pending.
- A host without cpu controller support remains cpuset-only by design. A host
  with malformed/unreadable mounted cpu hierarchy fails visibly rather than
  pretending cpu migration succeeded.
- Rollback is the dynamic disable transition: it returns target memberships to
  their matching roots. If a controller remains unavailable during rollback,
  the plugin retains pending state and reports the failed controller.

## Completion Criteria

- v1 cpu-root-only and cpuset-root-only candidates migrate to their respective
  targets.
- A v1 PID present in both roots converges in both targets and recovers from
  either-side partial failures without duplicate controller writes.
- v2 scans, migrates, and resets exactly once per PID.
- All relative target operations reject traversal and use pinned controller
  directories.
- The focused, Bulkhead-wide, dynamic-policy, and race test commands pass.
