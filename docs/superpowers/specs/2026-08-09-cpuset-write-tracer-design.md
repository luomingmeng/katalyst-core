# Cgroup Cpuset Write Tracer Design

## Goal

Provide a read-only BCC Python tracer that identifies the process responsible
for writes to one cgroup v1 `cpuset.cpus` file and records the attempted mask,
return value, kernel stack, and user stack.

## Target Environment

- Linux `5.15.152.bsk.10-amd64`
- cgroup v1 cpuset hierarchy
- Python 3.7
- BCC Python bindings installed
- `cpuset_write_resmask` available to kprobes
- BTF and kernel symbols available
- Root privileges required

The script must not require a standalone `clang` executable.

## Probe Design

### Syscall Entry

Attach to `syscalls:sys_enter_write`.

Store per-TID context:

- TGID and TID
- file descriptor
- userspace buffer preview
- requested byte count
- monotonic timestamp

The map entry is temporary and is removed at syscall exit.

### Cpuset Entry

Attach to `kprobe:cpuset_write_resmask`.

The probe confirms that the current write reached the cpuset resource-mask
handler. It records:

- kernel-side mask buffer
- kernel stack ID
- user stack ID
- process name
- syscall context for the same TID

The event remains pending until the function returns.

### Cpuset Return

Attach to `kretprobe:cpuset_write_resmask`.

Emit one perf-buffer event containing the return code and the captured entry
context. Remove the pending event map entry after emission.

### Syscall Exit

Attach to `syscalls:sys_exit_write`.

Delete stale per-TID syscall context. This ensures failed or unexpected write
paths do not leak BPF map entries.

## Path Resolution

The BPF program does not walk `kernfs_open_file` internals. The userspace
consumer resolves:

```text
/proc/<tgid>/fd/<fd>
```

immediately after receiving the event.

The event is printed only when the resolved path exactly matches the configured
target. The default target is the Duma sandbox leaf:

```text
/sys/fs/cgroup/cpuset/kubepods/pod855549b1-c112-44b9-885e-e87bd5899e8b/b2921a6741de19cf08055d0c212bf3d55a880fd574f859feb76dfe028dd5fd58/cpuset.cpus
```

An optional `--all-cpuset` mode prints every event that reaches
`cpuset_write_resmask`.

## Output

Write one JSON object per event. Fields:

```text
timestamp
monotonic_ns
tgid
tid
comm
fd
path
requested_bytes
user_buffer
kernel_buffer
return_value
kernel_stack
user_stack
```

The output is written to stdout and optionally duplicated to `--output`.
Stack frames are symbolized immediately while the process and its mappings
still exist. Unresolved frames are preserved as hexadecimal addresses.

## CLI

```text
--path <absolute-path>
--duration <seconds>
--output <jsonl-path>
--pid <tgid>
--comm <process-name>
--all-cpuset
--check
```

`--check` validates privileges, BCC imports, tracefs, target path, and
`cpuset_write_resmask` availability without attaching probes.

## Safety

- Never writes to cgroup files.
- Never changes affinity.
- Never restarts services.
- Uses bounded BPF maps.
- Limits captured buffers to 256 bytes.
- Stops automatically after `--duration`.
- Handles `SIGINT` and `SIGTERM` by exiting the poll loop and releasing BPF
  attachments.
- Reports perf-buffer lost events.

## Known Limits

- Covers the normal `write(2)` path. It does not correlate `writev` or
  `pwrite64`.
- `/proc/<tgid>/fd/<fd>` can disappear before userspace resolution if the
  writer closes the descriptor immediately.
- Stripped binaries or omitted frame pointers may produce unresolved user
  frames.
- `cpuset_write_resmask` is not a stable kernel ABI and must be rechecked after
  kernel upgrades.
- The script observes attempted writes. A successful return does not prove the
  value remained unchanged afterward.

## Verification

- Python syntax must pass under Python 3.7.
- Pure helper tests cover path matching, byte decoding, stack formatting, and
  JSON serialization without requiring BPF privileges.
- `--check` must pass on the target node.
- A bounded attach smoke test must load and detach without changing a cgroup.
- A real write test is performed only when explicitly authorized separately.
