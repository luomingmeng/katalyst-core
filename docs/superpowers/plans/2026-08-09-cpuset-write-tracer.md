# Cgroup Cpuset Write Tracer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a bounded BCC Python tracer that attributes writes to one cgroup v1 `cpuset.cpus` file and records the mask, return code, process identity, and kernel/user stacks.

**Architecture:** A BPF program correlates `sys_enter_write` by TID with `cpuset_write_resmask` entry/return probes. A Python consumer resolves `/proc/<tgid>/fd/<fd>`, filters the exact target path, symbolizes stack IDs immediately, and emits JSONL.

**Tech Stack:** Python 3.7, BCC Python bindings, eBPF kprobe/kretprobe, syscall tracepoints, BPF hash maps, perf buffer, unittest.

---

### Task 1: Pure Output Helpers

**Files:**
- Create: `hack/trace_cpuset_writes.py`
- Create: `hack/tests/test_trace_cpuset_writes.py`

- [ ] **Step 1: Write failing helper tests**

Cover:

```python
class HelperTests(unittest.TestCase):
    def test_decode_c_buffer_strips_nul_and_space(self):
        self.assertEqual("1-3,8", decode_c_buffer(b"1-3,8\n\x00junk"))

    def test_path_matches_exact_realpath(self):
        self.assertTrue(path_matches("/a/cpuset.cpus", "/a/cpuset.cpus"))
        self.assertFalse(path_matches("/a/cpuset.mems", "/a/cpuset.cpus"))

    def test_event_to_json_preserves_unresolved_frames(self):
        data = event_to_dict(
            timestamp="2026-08-09T16:00:00+08:00",
            event={"tgid": 12, "tid": 13, "comm": "agent"},
            path="/a/cpuset.cpus",
            user_buffer="1-3",
            kernel_buffer="1-3",
            return_value=4,
            kernel_stack=["cpuset_write_resmask+0x1", "0xffff"],
            user_stack=["main+0x2", "0x1234"],
        )
        self.assertEqual(12, data["tgid"])
        self.assertEqual(["main+0x2", "0x1234"], data["user_stack"])
```

- [ ] **Step 2: Verify RED**

```bash
python3 -m unittest hack.tests.test_trace_cpuset_writes -v
```

Expected: import failure because the script does not exist.

- [ ] **Step 3: Implement pure helpers**

Implement:

```python
def decode_c_buffer(value):
    raw = bytes(value).split(b"\0", 1)[0].strip()
    return raw.decode("utf-8", errors="replace")

def path_matches(actual, expected):
    return os.path.realpath(actual) == os.path.realpath(expected)

def event_to_dict(timestamp, event, path, user_buffer, kernel_buffer,
                  return_value, kernel_stack, user_stack):
    return {
        "timestamp": timestamp,
        "monotonic_ns": int(event.get("monotonic_ns", 0)),
        "tgid": int(event["tgid"]),
        "tid": int(event["tid"]),
        "comm": event["comm"],
        "fd": int(event.get("fd", -1)),
        "path": path,
        "requested_bytes": int(event.get("requested_bytes", 0)),
        "user_buffer": user_buffer,
        "kernel_buffer": kernel_buffer,
        "return_value": int(return_value),
        "kernel_stack": list(kernel_stack),
        "user_stack": list(user_stack),
    }
```

- [ ] **Step 4: Verify GREEN**

Run the unittest command again. Expected: all helper tests pass.

---

### Task 2: BPF Event Correlation

**Files:**
- Modify: `hack/trace_cpuset_writes.py`
- Modify: `hack/tests/test_trace_cpuset_writes.py`

- [ ] **Step 1: Add source-contract tests**

Assert generated BPF source contains:

```text
TRACEPOINT_PROBE(syscalls, sys_enter_write)
TRACEPOINT_PROBE(syscalls, sys_exit_write)
trace_cpuset_entry
trace_cpuset_return
BPF_STACK_TRACE
BPF_PERF_OUTPUT
```

Also assert buffer size is bounded to 256 and optional TGID filter is rendered.

- [ ] **Step 2: Verify RED**

Expected: `build_bpf_source` is undefined.

- [ ] **Step 3: Implement `build_bpf_source`**

The source defines:

```c
#define MAX_CAPTURE 256

struct write_ctx_t {
    u64 timestamp_ns;
    u64 user_ptr;
    u64 requested_bytes;
    u32 tgid;
    u32 tid;
    s32 fd;
    char user_buffer[MAX_CAPTURE];
};

struct event_t {
    u64 monotonic_ns;
    u64 requested_bytes;
    s64 return_value;
    u32 tgid;
    u32 tid;
    s32 fd;
    s32 kernel_stack_id;
    s32 user_stack_id;
    char comm[TASK_COMM_LEN];
    char user_buffer[MAX_CAPTURE];
    char kernel_buffer[MAX_CAPTURE];
};

BPF_HASH(active_writes, u32, struct write_ctx_t, 4096);
BPF_HASH(pending_cpuset, u32, struct event_t, 4096);
BPF_STACK_TRACE(stack_traces, 8192);
BPF_PERF_OUTPUT(events);
```

`sys_enter_write` captures the syscall context and bounded userspace buffer.
`trace_cpuset_entry` copies the kernel buffer and records both stack IDs.
`trace_cpuset_return` submits one event and deletes the pending map entry.
`sys_exit_write` deletes the syscall context.

- [ ] **Step 4: Verify source tests**

Expected: source-contract and helper tests pass.

---

### Task 3: CLI and JSONL Consumer

**Files:**
- Modify: `hack/trace_cpuset_writes.py`
- Modify: `hack/tests/test_trace_cpuset_writes.py`

- [ ] **Step 1: Add CLI parser tests**

Verify:

```text
default target path is the Duma sandbox cpuset.cpus
--duration accepts non-negative seconds
--pid accepts a positive TGID
--all-cpuset disables exact path filtering
--check performs no BPF attachment
```

- [ ] **Step 2: Implement CLI**

Arguments:

```text
--path
--duration
--output
--pid
--comm
--all-cpuset
--check
```

The default path is:

```text
/sys/fs/cgroup/cpuset/kubepods/pod855549b1-c112-44b9-885e-e87bd5899e8b/b2921a6741de19cf08055d0c212bf3d55a880fd574f859feb76dfe028dd5fd58/cpuset.cpus
```

- [ ] **Step 3: Implement environment checks**

`--check` validates:

- effective UID is zero;
- `bcc.BPF` imports successfully;
- `/sys/kernel/debug/tracing` exists;
- `/proc/kallsyms` contains `cpuset_write_resmask`;
- target path exists unless `--all-cpuset` is set.

Return nonzero and print one precise error per failed check.

- [ ] **Step 4: Implement event consumption**

For each perf event:

1. Resolve `/proc/<tgid>/fd/<fd>`.
2. Apply PID, comm, and exact path filters.
3. Decode both captured buffers.
4. Symbolize kernel stack with `ksym`.
5. Symbolize user stack with `sym(address, tgid)`.
6. Emit compact sorted JSON to stdout.
7. Duplicate to `--output` with line-buffered flushing.

Report lost perf events as JSON records with `event_type=lost`.

- [ ] **Step 5: Implement lifecycle**

- Attach kprobe and kretprobe explicitly.
- Open the perf buffer.
- Poll until `--duration` expires or SIGINT/SIGTERM is received.
- Close the output file in `finally`.
- Print startup configuration to stderr, never stdout.

- [ ] **Step 6: Run local verification**

```bash
python3 -m unittest hack.tests.test_trace_cpuset_writes -v
python3 -m py_compile hack/trace_cpuset_writes.py
git diff --check
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add hack/trace_cpuset_writes.py hack/tests/test_trace_cpuset_writes.py
git commit -m "feat(debug): trace cpuset cgroup writes"
```

---

### Task 4: Target Node Check and Attach Smoke Test

**Files:**
- No source changes expected

- [ ] **Step 1: Upload the script**

Use the architecture jump-host two-stage upload, falling back to `bgo scp` only
if the jump host is unavailable.

Target:

```text
/tmp/trace_cpuset_writes.py
```

- [ ] **Step 2: Run `--check`**

```bash
python3 /tmp/trace_cpuset_writes.py --check
```

Expected: every capability check reports OK and exit status is zero.

- [ ] **Step 3: Run bounded attach without writes**

```bash
python3 /tmp/trace_cpuset_writes.py --duration 5 \
  --output /tmp/trace_cpuset_writes_smoke.jsonl
```

Expected:

- probes load and detach;
- target cgroup is unchanged;
- zero events is acceptable;
- no verifier or stack-map error.

- [ ] **Step 4: Verify read-only behavior**

Capture target `cpuset.cpus` before and after the smoke test. Expected: identical.

- [ ] **Step 5: Copy final script to artifacts**

Copy the committed script to:

```text
qrm-bulkhead-test-artifacts/trace_cpuset_writes.py
```

Provide the user with the script link and exact command for a bounded trace
during the next target transition.
