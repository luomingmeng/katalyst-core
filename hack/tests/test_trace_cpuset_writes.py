import ctypes
import io
import json
import os
import signal
import tempfile
import unittest
from unittest import mock

from hack.trace_cpuset_writes import DEFAULT_TARGET_PATH
from hack.trace_cpuset_writes import EventConsumer
from hack.trace_cpuset_writes import JsonlWriter
from hack.trace_cpuset_writes import build_bpf_source
from hack.trace_cpuset_writes import create_argument_parser
from hack.trace_cpuset_writes import decode_c_buffer
from hack.trace_cpuset_writes import event_to_dict
from hack.trace_cpuset_writes import path_matches
from hack.trace_cpuset_writes import run_checks
from hack.trace_cpuset_writes import run_tracer
from hack.trace_cpuset_writes import symbolize_stack


class HelperTests(unittest.TestCase):
    def test_decode_c_buffer_strips_nul_and_space(self):
        self.assertEqual("1-3,8", decode_c_buffer(b"1-3,8\n\x00junk"))

    def test_path_matches_exact_realpath(self):
        self.assertTrue(path_matches("/a/cpuset.cpus", "/a/cpuset.cpus"))
        self.assertFalse(path_matches("/a/cpuset.mems", "/a/cpuset.cpus"))

    def test_path_matches_rejects_none_and_nul(self):
        self.assertFalse(path_matches(None, "/a/cpuset.cpus"))
        self.assertFalse(path_matches("/a/cpuset.cpus", None))
        self.assertFalse(path_matches("/a/\0cpuset.cpus", "/a/cpuset.cpus"))
        self.assertFalse(path_matches(b"/a/\0cpuset.cpus", b"/a/cpuset.cpus"))

    def test_path_matches_returns_false_for_path_errors(self):
        for error in (TypeError, ValueError, OSError):
            with self.subTest(error=error.__name__):
                with mock.patch(
                    "hack.trace_cpuset_writes.os.path.realpath",
                    side_effect=error("invalid path"),
                ):
                    self.assertFalse(
                        path_matches("/a/cpuset.cpus", "/a/cpuset.cpus")
                    )

    def test_event_to_dict_preserves_unresolved_frames(self):
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

        self.assertEqual(
            {
                "timestamp": "2026-08-09T16:00:00+08:00",
                "monotonic_ns": 0,
                "tgid": 12,
                "tid": 13,
                "comm": "agent",
                "fd": -1,
                "path": "/a/cpuset.cpus",
                "requested_bytes": 0,
                "user_buffer": "1-3",
                "kernel_buffer": "1-3",
                "return_value": 4,
                "kernel_stack": ["cpuset_write_resmask+0x1", "0xffff"],
                "user_stack": ["main+0x2", "0x1234"],
            },
            data,
        )

    def test_event_to_dict_supports_ctypes_structure_and_json_output(self):
        class Event(ctypes.Structure):
            _fields_ = [
                ("monotonic_ns", ctypes.c_uint64),
                ("tgid", ctypes.c_uint32),
                ("tid", ctypes.c_uint32),
                ("comm", ctypes.c_char * 16),
                ("fd", ctypes.c_int),
                ("requested_bytes", ctypes.c_size_t),
            ]

        event = Event(
            monotonic_ns=123,
            tgid=12,
            tid=13,
            comm=b"agent-\xff",
            fd=9,
            requested_bytes=4,
        )
        data = event_to_dict(
            timestamp="2026-08-09T16:00:00+08:00",
            event=event,
            path="/a/cpuset.cpus",
            user_buffer="1-3",
            kernel_buffer="1-3",
            return_value=ctypes.c_long(4),
            kernel_stack=[b"write-\xff", b"0xffff"],
            user_stack=[b"main+0x2", "0x1234"],
        )

        self.assertEqual(123, data["monotonic_ns"])
        self.assertEqual("agent-\ufffd", data["comm"])
        self.assertEqual(4, data["return_value"])
        self.assertEqual(["write-\ufffd", "0xffff"], data["kernel_stack"])
        self.assertEqual(["main+0x2", "0x1234"], data["user_stack"])
        json.dumps(data)

    def test_event_to_dict_unwraps_ctypes_values_from_mapping(self):
        data = event_to_dict(
            timestamp="now",
            event={
                "monotonic_ns": ctypes.c_uint64(123),
                "tgid": ctypes.c_uint32(12),
                "tid": ctypes.c_uint32(13),
                "comm": ctypes.c_char_p(b"worker"),
                "fd": ctypes.c_int(7),
                "requested_bytes": ctypes.c_size_t(8),
            },
            path="/a/cpuset.cpus",
            user_buffer="1-3",
            kernel_buffer="1-3",
            return_value=ctypes.c_long(-1),
            kernel_stack=[],
            user_stack=[],
        )

        self.assertEqual(
            (123, 12, 13, "worker", 7, 8, -1),
            (
                data["monotonic_ns"],
                data["tgid"],
                data["tid"],
                data["comm"],
                data["fd"],
                data["requested_bytes"],
                data["return_value"],
            ),
        )
        json.dumps(data)


class BPFSourceContractTests(unittest.TestCase):
    def test_source_correlates_write_and_cpuset_probes_by_tid(self):
        source = build_bpf_source()

        self.assertIn("TRACEPOINT_PROBE(syscalls, sys_enter_write)", source)
        self.assertIn("TRACEPOINT_PROBE(syscalls, sys_exit_write)", source)
        self.assertIn("int trace_cpuset_entry(struct pt_regs *ctx)", source)
        self.assertIn("int trace_cpuset_return(struct pt_regs *ctx)", source)
        self.assertIn("u32 tid = (u32)bpf_get_current_pid_tgid();", source)
        self.assertIn("active_writes.update(&tid, &write_ctx);", source)
        self.assertIn("active_writes.lookup(&tid)", source)
        self.assertIn("pending_cpuset.update(&tid, event);", source)
        self.assertIn("pending_cpuset.lookup(&tid)", source)
        self.assertIn("active_writes.delete(&tid);", source)
        self.assertIn("pending_cpuset.delete(&tid);", source)

    def test_source_captures_bounded_buffers_identity_and_stacks(self):
        source = build_bpf_source()

        self.assertIn("#define MAX_CAPTURE 256", source)
        self.assertIn("char user_buffer[MAX_CAPTURE];", source)
        self.assertIn("char kernel_buffer[MAX_CAPTURE];", source)
        self.assertIn("BPF_HASH(active_writes, u32, struct write_ctx_t, 4096);", source)
        self.assertIn("BPF_HASH(pending_cpuset, u32, struct event_t, 4096);", source)
        self.assertIn("BPF_STACK_TRACE(stack_traces, 8192);", source)
        self.assertIn("BPF_PERF_OUTPUT(events);", source)
        self.assertIn("bpf_probe_read_user", source)
        self.assertIn("bpf_probe_read_kernel", source)
        self.assertIn("bpf_get_current_comm", source)
        self.assertNotIn("BPF_F_REUSE_STACKID", source)
        self.assertIn("stack_traces.get_stackid(ctx, 0)", source)
        self.assertRegex(
            source,
            r"stack_traces\.get_stackid\(\s*ctx,\s*"
            r"BPF_F_USER_STACK\s*\)",
        )

    def test_source_submits_cpuset_return_value(self):
        source = build_bpf_source()

        self.assertIn("event->return_value = PT_REGS_RC(ctx);", source)
        self.assertIn("events.perf_submit(ctx, event, sizeof(*event));", source)

    def test_source_renders_optional_tgid_filter(self):
        unfiltered = build_bpf_source()
        filtered = build_bpf_source(tgid=4321)

        self.assertNotIn("if (tgid != 4321)", unfiltered)
        self.assertIn("if (tgid != 4321)", filtered)
        self.assertIn("return 0;", filtered)

    def test_source_rejects_invalid_tgid_filter(self):
        for value in (0, -1, True, "4321"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    build_bpf_source(tgid=value)


class CLIParserTests(unittest.TestCase):
    def setUp(self):
        self.parser = create_argument_parser()

    def test_defaults_to_duma_cpuset_path(self):
        args = self.parser.parse_args([])

        self.assertEqual(DEFAULT_TARGET_PATH, args.path)
        self.assertGreaterEqual(args.duration, 0)
        self.assertIsNone(args.pid)
        self.assertIsNone(args.comm)
        self.assertIsNone(args.output)
        self.assertFalse(args.all_cpuset)
        self.assertFalse(args.check)

    def test_accepts_all_supported_options(self):
        args = self.parser.parse_args(
            [
                "--path",
                "/cg/cpuset.cpus",
                "--duration",
                "1.5",
                "--pid",
                "42",
                "--comm",
                "katalyst_agent",
                "--output",
                "/tmp/events.jsonl",
                "--all-cpuset",
                "--check",
            ]
        )

        self.assertEqual("/cg/cpuset.cpus", args.path)
        self.assertEqual(1.5, args.duration)
        self.assertEqual(42, args.pid)
        self.assertEqual("katalyst_agent", args.comm)
        self.assertEqual("/tmp/events.jsonl", args.output)
        self.assertTrue(args.all_cpuset)
        self.assertTrue(args.check)

    def test_rejects_negative_duration(self):
        with self.assertRaises(SystemExit):
            self.parser.parse_args(["--duration", "-0.1"])

    def test_rejects_non_positive_pid(self):
        for value in ("0", "-1"):
            with self.subTest(value=value):
                with self.assertRaises(SystemExit):
                    self.parser.parse_args(["--pid", value])


class EnvironmentCheckTests(unittest.TestCase):
    def setUp(self):
        self.args = create_argument_parser().parse_args(
            ["--path", "/cg/cpuset.cpus", "--check"]
        )

    def test_reports_each_failed_requirement_without_attaching(self):
        bpf_class = mock.Mock()
        with mock.patch(
            "hack.trace_cpuset_writes.os.geteuid", return_value=1000
        ), mock.patch(
            "hack.trace_cpuset_writes.os.path.isdir", return_value=False
        ), mock.patch(
            "hack.trace_cpuset_writes.os.path.exists", return_value=False
        ), mock.patch(
            "hack.trace_cpuset_writes._kallsyms_has_target", return_value=False
        ):
            errors = run_checks(self.args, bpf_loader=lambda: bpf_class)

        self.assertEqual(4, len(errors))
        self.assertTrue(any("root" in error for error in errors))
        self.assertTrue(any("tracefs" in error for error in errors))
        self.assertTrue(any("cpuset_write_resmask" in error for error in errors))
        self.assertTrue(any("/cg/cpuset.cpus" in error for error in errors))
        bpf_class.assert_not_called()

    def test_reports_delayed_bcc_import_failure(self):
        with mock.patch(
            "hack.trace_cpuset_writes.os.geteuid", return_value=0
        ), mock.patch(
            "hack.trace_cpuset_writes.os.path.isdir", return_value=True
        ), mock.patch(
            "hack.trace_cpuset_writes.os.path.exists", return_value=True
        ), mock.patch(
            "hack.trace_cpuset_writes._kallsyms_has_target", return_value=True
        ):
            errors = run_checks(
                self.args,
                bpf_loader=mock.Mock(side_effect=ImportError("no bcc")),
            )

        self.assertEqual(1, len(errors))
        self.assertIn("BCC", errors[0])

    def test_all_cpuset_does_not_require_target_path(self):
        args = create_argument_parser().parse_args(["--all-cpuset", "--check"])
        with mock.patch(
            "hack.trace_cpuset_writes.os.geteuid", return_value=0
        ), mock.patch(
            "hack.trace_cpuset_writes.os.path.isdir", return_value=True
        ), mock.patch(
            "hack.trace_cpuset_writes.os.path.exists", return_value=False
        ), mock.patch(
            "hack.trace_cpuset_writes._kallsyms_has_target", return_value=True
        ):
            errors = run_checks(args, bpf_loader=lambda: object)

        self.assertEqual([], errors)


class StackSymbolizationTests(unittest.TestCase):
    def test_negative_stack_id_is_explicit(self):
        stack_table = mock.Mock()

        self.assertEqual(
            ["<stack id -14>"],
            symbolize_stack(stack_table, -14, lambda address: address),
        )
        stack_table.walk.assert_not_called()

    def test_symbolizes_each_frame_and_preserves_unresolved_address(self):
        stack_table = mock.Mock()
        stack_table.walk.return_value = [0x10, 0x20]

        def resolve(address):
            return b"known+0x1" if address == 0x10 else None

        self.assertEqual(
            ["known+0x1", "0x20"],
            symbolize_stack(stack_table, 7, resolve),
        )


class JsonlWriterTests(unittest.TestCase):
    def test_writes_compact_sorted_json_to_stdout_and_file(self):
        stdout = io.StringIO()
        with tempfile.TemporaryDirectory() as directory:
            output_path = os.path.join(directory, "events.jsonl")
            writer = JsonlWriter(stdout=stdout, output_path=output_path)
            writer.write({"z": 1, "a": "值"})
            writer.close()

            expected = '{"a":"值","z":1}\n'
            self.assertEqual(expected, stdout.getvalue())
            with open(output_path, encoding="utf-8") as output:
                self.assertEqual(expected, output.read())


class EventConsumerTests(unittest.TestCase):
    class Event:
        monotonic_ns = 123
        requested_bytes = 4
        return_value = 4
        tgid = 12
        tid = 13
        fd = 9
        kernel_stack_id = 1
        user_stack_id = 2
        comm = b"agent"
        user_buffer = b"1-3\n"
        kernel_buffer = b"1-3\n"

    def make_consumer(self, **overrides):
        args = create_argument_parser().parse_args([])
        for name, value in overrides.items():
            setattr(args, name, value)
        bpf = mock.MagicMock()
        bpf["stack_traces"].walk.side_effect = lambda stack_id: [stack_id]
        bpf.ksym.side_effect = lambda address: ("k%d" % address).encode()
        bpf.sym.side_effect = lambda address, tgid: (
            "u%d:%d" % (address, tgid)
        ).encode()
        writer = mock.Mock()
        consumer = EventConsumer(
            bpf,
            args,
            writer,
            readlink=mock.Mock(return_value=DEFAULT_TARGET_PATH),
            clock=mock.Mock(return_value="now"),
        )
        return consumer, bpf, writer

    def test_resolves_proc_fd_filters_and_emits_symbolized_event(self):
        consumer, bpf, writer = self.make_consumer(pid=12, comm="agent")
        event_table = bpf["events"]
        event_table.event.return_value = self.Event()

        consumer.handle_event(0, object(), 0)

        consumer.readlink.assert_called_once_with("/proc/12/fd/9")
        record = writer.write.call_args[0][0]
        self.assertEqual(DEFAULT_TARGET_PATH, record["path"])
        self.assertEqual("1-3", record["user_buffer"])
        self.assertEqual(["k1"], record["kernel_stack"])
        self.assertEqual(["u2:12"], record["user_stack"])

    def test_applies_pid_comm_and_exact_path_filters(self):
        cases = [
            {"pid": 99},
            {"comm": "other"},
            {"readlink": "/other/cpuset.cpus"},
        ]
        for case in cases:
            with self.subTest(case=case):
                overrides = {
                    key: value for key, value in case.items() if key != "readlink"
                }
                consumer, bpf, writer = self.make_consumer(**overrides)
                if "readlink" in case:
                    consumer.readlink.return_value = case["readlink"]
                bpf["events"].event.return_value = self.Event()

                consumer.handle_event(0, object(), 0)

                writer.write.assert_not_called()

    def test_all_cpuset_emits_when_proc_fd_disappeared(self):
        consumer, bpf, writer = self.make_consumer(all_cpuset=True)
        consumer.readlink.side_effect = OSError("gone")
        bpf["events"].event.return_value = self.Event()

        consumer.handle_event(0, object(), 0)

        self.assertIsNone(writer.write.call_args[0][0]["path"])

    def test_lost_events_are_json_records(self):
        consumer, _, writer = self.make_consumer()

        consumer.handle_lost(3, 17)

        self.assertEqual(
            {"event_type": "lost", "cpu": 3, "lost": 17},
            writer.write.call_args[0][0],
        )


class TracerLifecycleTests(unittest.TestCase):
    def test_attaches_entry_return_opens_perf_and_closes_in_finally(self):
        args = create_argument_parser().parse_args(["--duration", "0"])
        bpf = mock.MagicMock()
        bpf_class = mock.Mock(return_value=bpf)
        writer = mock.Mock()

        run_tracer(args, bpf_class=bpf_class, writer=writer)

        bpf_class.assert_called_once()
        bpf.attach_kprobe.assert_called_once_with(
            event="cpuset_write_resmask", fn_name="trace_cpuset_entry"
        )
        bpf.attach_kretprobe.assert_called_once_with(
            event="cpuset_write_resmask", fn_name="trace_cpuset_return"
        )
        bpf["events"].open_perf_buffer.assert_called_once()
        bpf.detach_kretprobe.assert_called_once_with(
            event="cpuset_write_resmask"
        )
        bpf.detach_kprobe.assert_called_once_with(event="cpuset_write_resmask")
        writer.close.assert_called_once_with()

    def test_poll_failure_still_closes_writer_and_detaches(self):
        args = create_argument_parser().parse_args(["--duration", "1"])
        bpf = mock.MagicMock()
        bpf.perf_buffer_poll.side_effect = RuntimeError("poll failed")
        writer = mock.Mock()

        with self.assertRaises(RuntimeError):
            run_tracer(
                args,
                bpf_class=mock.Mock(return_value=bpf),
                writer=writer,
                monotonic=mock.Mock(side_effect=[0, 0]),
            )

        bpf.detach_kretprobe.assert_called_once()
        bpf.detach_kprobe.assert_called_once()
        writer.close.assert_called_once_with()

    def test_detach_failure_still_closes_writer(self):
        args = create_argument_parser().parse_args(["--duration", "0"])
        bpf = mock.MagicMock()
        bpf.detach_kretprobe.side_effect = RuntimeError("detach failed")
        writer = mock.Mock()

        with self.assertRaises(RuntimeError):
            run_tracer(
                args,
                bpf_class=mock.Mock(return_value=bpf),
                writer=writer,
            )

        writer.close.assert_called_once_with()

    def test_installs_sigint_and_sigterm_handlers(self):
        args = create_argument_parser().parse_args(["--duration", "0"])
        with mock.patch(
            "hack.trace_cpuset_writes.signal.signal"
        ) as install_signal:
            run_tracer(
                args,
                bpf_class=mock.Mock(return_value=mock.MagicMock()),
                writer=mock.Mock(),
            )

        installed = [call[0][0] for call in install_signal.call_args_list]
        self.assertIn(signal.SIGINT, installed)
        self.assertIn(signal.SIGTERM, installed)


if __name__ == "__main__":
    unittest.main()
