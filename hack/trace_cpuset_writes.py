import argparse
import ctypes
import datetime
import json
import math
import os
import signal
import sys
import time
from collections.abc import Mapping


DEFAULT_TARGET_PATH = (
    "/sys/fs/cgroup/cpuset/kubepods/"
    "pod855549b1-c112-44b9-885e-e87bd5899e8b/"
    "b2921a6741de19cf08055d0c212bf3d55a880fd574f859feb76dfe028dd5fd58/"
    "cpuset.cpus"
)
TRACEFS_PATH = "/sys/kernel/debug/tracing"
KALLSYMS_PATH = "/proc/kallsyms"
KPROBE_TARGET = "cpuset_write_resmask"


def _non_negative_float(value):
    try:
        parsed = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be a number")
    if not math.isfinite(parsed) or parsed < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return parsed


def _positive_int(value):
    try:
        parsed = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be an integer")
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def create_argument_parser():
    parser = argparse.ArgumentParser(
        description="Trace writes handled by cpuset_write_resmask"
    )
    parser.add_argument("--path", default=DEFAULT_TARGET_PATH)
    parser.add_argument(
        "--duration",
        default=30.0,
        type=_non_negative_float,
        help="trace duration in seconds (default: 30)",
    )
    parser.add_argument("--output", help="duplicate JSONL to this file")
    parser.add_argument("--pid", type=_positive_int, help="filter by TGID")
    parser.add_argument("--comm", help="filter by exact process name")
    parser.add_argument(
        "--all-cpuset",
        action="store_true",
        help="do not filter events by target path",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="check prerequisites without attaching BPF probes",
    )
    return parser


def _load_bpf_class():
    from bcc import BPF

    return BPF


def _kallsyms_has_target(
    path=KALLSYMS_PATH,
    target=KPROBE_TARGET,
):
    try:
        with open(path, encoding="ascii", errors="replace") as symbols:
            for line in symbols:
                fields = line.split()
                if len(fields) >= 3 and fields[2] == target:
                    return True
    except OSError:
        return False
    return False


def run_checks(args, bpf_loader=_load_bpf_class):
    errors = []
    if os.geteuid() != 0:
        errors.append("root privileges are required (effective UID is not zero)")

    try:
        bpf_loader()
    except (ImportError, OSError) as error:
        errors.append("BCC Python bindings are unavailable: %s" % error)

    if not os.path.isdir(TRACEFS_PATH):
        errors.append("tracefs is unavailable at %s" % TRACEFS_PATH)
    if not _kallsyms_has_target():
        errors.append("%s is absent from %s" % (KPROBE_TARGET, KALLSYMS_PATH))
    if not args.all_cpuset and not os.path.exists(args.path):
        errors.append("target path does not exist: %s" % args.path)
    return errors


def build_bpf_source(tgid=None):
    if tgid is not None and (type(tgid) is not int or tgid <= 0):
        raise ValueError("tgid must be a positive integer")

    tgid_filter = ""
    if tgid is not None:
        tgid_filter = """
    if (tgid != %d) {
        return 0;
    }
""" % tgid

    source = r"""
#include <uapi/linux/ptrace.h>
#include <linux/fs.h>
#include <linux/kdev_t.h>
#include <linux/kernfs.h>
#include <linux/sched.h>

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
    u64 inode;
    s64 return_value;
    u32 dev_major;
    u32 dev_minor;
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
BPF_PERCPU_ARRAY(event_scratch, struct event_t, 1);
BPF_PERF_OUTPUT(events);

TRACEPOINT_PROBE(syscalls, sys_enter_write)
{
    u64 pid_tgid = bpf_get_current_pid_tgid();
    u32 tgid = pid_tgid >> 32;
    u32 tid = (u32)bpf_get_current_pid_tgid();
__TGID_FILTER__
    struct write_ctx_t write_ctx = {};
    write_ctx.timestamp_ns = bpf_ktime_get_ns();
    write_ctx.user_ptr = (u64)args->buf;
    write_ctx.requested_bytes = args->count;
    write_ctx.tgid = tgid;
    write_ctx.tid = tid;
    write_ctx.fd = args->fd;

    u64 capture_size = args->count;
    if (capture_size > MAX_CAPTURE) {
        capture_size = MAX_CAPTURE;
    }
    if (capture_size > 0) {
        bpf_probe_read_user(
            write_ctx.user_buffer,
            capture_size,
            (const void *)args->buf
        );
    }

    active_writes.update(&tid, &write_ctx);
    return 0;
}

TRACEPOINT_PROBE(syscalls, sys_exit_write)
{
    u32 tid = (u32)bpf_get_current_pid_tgid();
    active_writes.delete(&tid);
    return 0;
}

int trace_cpuset_entry(struct pt_regs *ctx)
{
    u32 tid = (u32)bpf_get_current_pid_tgid();
    struct write_ctx_t *write_ctx = active_writes.lookup(&tid);
    if (!write_ctx) {
        return 0;
    }

    u32 zero = 0;
    struct event_t *event = event_scratch.lookup(&zero);
    if (!event) {
        return 0;
    }

    event->monotonic_ns = write_ctx->timestamp_ns;
    event->requested_bytes = write_ctx->requested_bytes;
    event->dev_major = 0;
    event->dev_minor = 0;
    event->inode = 0;
    event->return_value = 0;
    event->tgid = write_ctx->tgid;
    event->tid = write_ctx->tid;
    event->fd = write_ctx->fd;
    event->kernel_stack_id =
        stack_traces.get_stackid(ctx, 0);
    event->user_stack_id =
        stack_traces.get_stackid(
            ctx,
            BPF_F_USER_STACK
        );
    bpf_get_current_comm(&event->comm, sizeof(event->comm));
    __builtin_memcpy(
        event->user_buffer,
        write_ctx->user_buffer,
        sizeof(event->user_buffer)
    );
    __builtin_memset(event->kernel_buffer, 0, sizeof(event->kernel_buffer));

    struct kernfs_open_file *of =
        (struct kernfs_open_file *)PT_REGS_PARM1(ctx);
    struct file *file = NULL;
    struct inode *inode = NULL;
    struct super_block *sb = NULL;
    bpf_probe_read_kernel(&file, sizeof(file), &of->file);
    if (file) {
        bpf_probe_read_kernel(&inode, sizeof(inode), &file->f_inode);
    }
    if (inode) {
        bpf_probe_read_kernel(
            &event->inode,
            sizeof(event->inode),
            &inode->i_ino
        );
        bpf_probe_read_kernel(&sb, sizeof(sb), &inode->i_sb);
    }
    if (sb) {
        dev_t dev = 0;
        bpf_probe_read_kernel(&dev, sizeof(dev), &sb->s_dev);
        event->dev_major = MAJOR(dev);
        event->dev_minor = MINOR(dev);
    }

    const char *kernel_buffer = (const char *)PT_REGS_PARM2(ctx);
    u64 kernel_size = (u64)PT_REGS_PARM3(ctx);
    if (kernel_size > MAX_CAPTURE) {
        kernel_size = MAX_CAPTURE;
    }
    if (kernel_size > 0) {
        bpf_probe_read_kernel(
            event->kernel_buffer,
            kernel_size,
            kernel_buffer
        );
    }

    pending_cpuset.update(&tid, event);
    return 0;
}

int trace_cpuset_return(struct pt_regs *ctx)
{
    u32 tid = (u32)bpf_get_current_pid_tgid();
    struct event_t *event = pending_cpuset.lookup(&tid);
    if (!event) {
        return 0;
    }

    event->return_value = PT_REGS_RC(ctx);
    events.perf_submit(ctx, event, sizeof(*event));
    pending_cpuset.delete(&tid);
    return 0;
}
"""
    return source.replace("__TGID_FILTER__", tgid_filter.rstrip())


def decode_c_buffer(value):
    raw = bytes(value).split(b"\0", 1)[0].strip()
    return raw.decode("utf-8", errors="replace")


def path_matches(actual, expected):
    try:
        actual = os.fspath(actual)
        expected = os.fspath(expected)
        if _contains_nul(actual) or _contains_nul(expected):
            return False
        return os.path.realpath(actual) == os.path.realpath(expected)
    except (TypeError, ValueError, OSError):
        return False


def _contains_nul(path):
    nul = b"\0" if isinstance(path, bytes) else "\0"
    return nul in path


def _unwrap_ctypes_value(value):
    if isinstance(value, ctypes._SimpleCData):
        return value.value
    return value


def _event_field(event, name, default=None):
    if isinstance(event, Mapping):
        value = event.get(name, default)
    else:
        value = getattr(event, name, default)
    return _unwrap_ctypes_value(value)


def _decode_text(value):
    value = _unwrap_ctypes_value(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _decode_stack(stack):
    return [_decode_text(frame) for frame in stack]


def event_to_dict(
    timestamp,
    event,
    path,
    observed_fd_path,
    observed_fd_path_matches,
    user_buffer,
    kernel_buffer,
    return_value,
    kernel_stack,
    user_stack,
):
    return {
        "timestamp": timestamp,
        "monotonic_ns": int(_event_field(event, "monotonic_ns", 0)),
        "tgid": int(_event_field(event, "tgid")),
        "tid": int(_event_field(event, "tid")),
        "comm": _decode_text(_event_field(event, "comm")),
        "fd": int(_event_field(event, "fd", -1)),
        "dev_major": int(_event_field(event, "dev_major", 0)),
        "dev_minor": int(_event_field(event, "dev_minor", 0)),
        "inode": int(_event_field(event, "inode", 0)),
        "path": path,
        "observed_fd_path": observed_fd_path,
        "observed_fd_path_matches": observed_fd_path_matches,
        "requested_bytes": int(_event_field(event, "requested_bytes", 0)),
        "user_buffer": user_buffer,
        "kernel_buffer": kernel_buffer,
        "return_value": int(_unwrap_ctypes_value(return_value)),
        "kernel_stack": _decode_stack(kernel_stack),
        "user_stack": _decode_stack(user_stack),
    }


def symbolize_stack(stack_table, stack_id, resolver):
    stack_id = int(_unwrap_ctypes_value(stack_id))
    if stack_id < 0:
        return ["<stack id %d>" % stack_id]

    frames = []
    try:
        addresses = stack_table.walk(stack_id)
        for address in addresses:
            symbol = resolver(address)
            if symbol and _decode_text(symbol) != "[unknown]":
                frames.append(_decode_text(symbol))
            else:
                frames.append("0x%x" % address)
    except Exception as error:
        frames.append("<stack id %d: %s>" % (stack_id, error))
    return frames


def _wall_clock_timestamp():
    return datetime.datetime.now(datetime.timezone.utc).astimezone().isoformat()


class JsonlWriter:
    def __init__(self, stdout=None, output_path=None):
        self.stdout = stdout if stdout is not None else sys.stdout
        self.output = None
        if output_path:
            self.output = open(
                output_path,
                mode="a",
                encoding="utf-8",
                buffering=1,
            )

    def write(self, record):
        line = json.dumps(
            record,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        self.stdout.write(line + "\n")
        self.stdout.flush()
        if self.output is not None:
            self.output.write(line + "\n")
            self.output.flush()

    def close(self):
        if self.output is not None:
            self.output.close()
            self.output = None


class EventConsumer:
    def __init__(
        self,
        bpf,
        args,
        writer,
        target_dev_major=None,
        target_dev_minor=None,
        target_inode=None,
        target_path=None,
        readlink=os.readlink,
        clock=_wall_clock_timestamp,
    ):
        self.bpf = bpf
        self.args = args
        self.writer = writer
        self.target_dev_major = target_dev_major
        self.target_dev_minor = target_dev_minor
        self.target_inode = target_inode
        self.target_path = target_path
        self.readlink = readlink
        self.clock = clock

    def _resolve_path(self, event):
        proc_fd = "/proc/%d/fd/%d" % (int(event.tgid), int(event.fd))
        try:
            return self.readlink(proc_fd)
        except OSError:
            return None

    def handle_event(self, cpu, data, size):
        del cpu, size
        event = self.bpf["events"].event(data)
        tgid = int(event.tgid)
        comm = decode_c_buffer(event.comm)

        if self.args.pid is not None and tgid != self.args.pid:
            return
        if self.args.comm is not None and comm != self.args.comm:
            return

        observed_fd_path = self._resolve_path(event)
        if not self.args.all_cpuset:
            event_key = (
                int(event.dev_major),
                int(event.dev_minor),
                int(event.inode),
            )
            target_key = (
                self.target_dev_major,
                self.target_dev_minor,
                self.target_inode,
            )
            if event_key != target_key:
                return
            path = self.target_path
            observed_fd_path_matches = (
                observed_fd_path is not None
                and path_matches(observed_fd_path, self.target_path)
            )
        else:
            path = observed_fd_path
            observed_fd_path_matches = None

        stack_table = self.bpf["stack_traces"]
        kernel_stack = symbolize_stack(
            stack_table,
            event.kernel_stack_id,
            self.bpf.ksym,
        )
        user_stack = symbolize_stack(
            stack_table,
            event.user_stack_id,
            lambda address: self.bpf.sym(address, tgid),
        )
        record = event_to_dict(
            timestamp=self.clock(),
            event=event,
            path=path,
            observed_fd_path=observed_fd_path,
            observed_fd_path_matches=observed_fd_path_matches,
            user_buffer=decode_c_buffer(event.user_buffer),
            kernel_buffer=decode_c_buffer(event.kernel_buffer),
            return_value=event.return_value,
            kernel_stack=kernel_stack,
            user_stack=user_stack,
        )
        self.writer.write(record)

    def handle_lost(self, *args):
        record = {
            "event_type": "lost",
            "lost": int(args[-1]),
        }
        if len(args) > 1:
            record["cpu"] = int(args[-2])
        self.writer.write(record)


def run_tracer(
    args,
    bpf_class=None,
    writer=None,
    monotonic=time.monotonic,
    stat_func=os.stat,
):
    if bpf_class is None:
        bpf_class = _load_bpf_class()
    if writer is None:
        writer = JsonlWriter(output_path=args.output)

    bpf = None
    entry_attached = False
    return_attached = False
    old_handlers = {}
    stopped = [False]
    target_dev_major = None
    target_dev_minor = None
    target_inode = None
    target_path = None

    def stop(signum, frame):
        del signum, frame
        stopped[0] = True

    try:
        if not args.all_cpuset:
            target_path = os.path.realpath(args.path)
            target_stat = stat_func(target_path)
            target_dev_major = os.major(target_stat.st_dev)
            target_dev_minor = os.minor(target_stat.st_dev)
            target_inode = int(target_stat.st_ino)

        for signum in (signal.SIGINT, signal.SIGTERM):
            old_handlers[signum] = signal.signal(signum, stop)

        bpf = bpf_class(text=build_bpf_source(tgid=args.pid))
        bpf.attach_kprobe(
            event=KPROBE_TARGET,
            fn_name="trace_cpuset_entry",
        )
        entry_attached = True
        bpf.attach_kretprobe(
            event=KPROBE_TARGET,
            fn_name="trace_cpuset_return",
        )
        return_attached = True

        consumer = EventConsumer(
            bpf,
            args,
            writer,
            target_dev_major=target_dev_major,
            target_dev_minor=target_dev_minor,
            target_inode=target_inode,
            target_path=target_path,
        )
        bpf["events"].open_perf_buffer(
            consumer.handle_event,
            lost_cb=consumer.handle_lost,
        )

        deadline = monotonic() + args.duration
        while not stopped[0]:
            now = monotonic()
            if now >= deadline:
                break
            remaining = min(0.1, deadline - now)
            remaining_ms = max(1, int(remaining * 1000))
            bpf.perf_buffer_poll(timeout=remaining_ms)
    finally:
        try:
            try:
                if bpf is not None and return_attached:
                    bpf.detach_kretprobe(event=KPROBE_TARGET)
            finally:
                if bpf is not None and entry_attached:
                    bpf.detach_kprobe(event=KPROBE_TARGET)
        finally:
            try:
                for signum, handler in old_handlers.items():
                    signal.signal(signum, handler)
            finally:
                writer.close()


def main(argv=None):
    args = create_argument_parser().parse_args(argv)
    if args.check:
        errors = run_checks(args)
        if errors:
            for error in errors:
                print("ERROR: %s" % error, file=sys.stderr)
            return 1
        print("OK: tracer prerequisites satisfied", file=sys.stderr)
        return 0

    print(
        "Tracing %s for %.3f seconds (pid=%s, comm=%s, output=%s)"
        % (
            "all cpuset writes" if args.all_cpuset else args.path,
            args.duration,
            args.pid if args.pid is not None else "all",
            args.comm if args.comm is not None else "all",
            args.output if args.output is not None else "stdout only",
        ),
        file=sys.stderr,
    )
    run_tracer(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
