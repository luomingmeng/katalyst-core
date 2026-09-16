# Mixed-QoS Repeated E2E Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpower-subagent-driven-development (recommended) or superpower-executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Validate the deployed frozen admission implementation through repeated canonical stress and the complete mixed-QoS configuration matrix.

**Architecture:** Execute all stateful suites serially on `fdbd-dc02-27-49--14`. Each suite is fenced by process identity, checkpoint, health, and cleanup checks; failures stop subsequent stress and preserve evidence.

**Tech Stack:** Bash, Python 3, Katalyst QRM E2E scripts, Kubernetes API, `bgo`, cgroup v1, SHA256 archives.

---

### Task 1: Node and Control-Plane Preflight

**Files:**
- Read: `.trae/skills/qrm-bulkhead-e2e/scripts/final_e2e_preflight_wrapper.sh`
- Read: `.trae/skills/qrm-bulkhead-e2e/scripts/qos_matrix_controller.py`
- Output: `/root/qrm-mixed-qos-e2e/<run>/preflight.log`

- [ ] Verify that no E2E process is active and the node is in reset state.
- [ ] Verify QRM and SysAdvisor runtime and rootfs SHA256 equal `0594ded74169df6c4a17866461d3b01ecab2b7d7e7c92e39f99f9b387224037d`.
- [ ] Verify health, checkpoint readability, whole-core alignment, Kubernetes node identity, `crictl`, AQC/CNC readback, and controller prerequisites.
- [ ] Stop without mutation if any preflight fence fails.

### Task 2: Three Canonical Suites

**Files:**
- Execute: `/root/qrm-bulkhead-e2e/scripts/final_e2e_preflight_wrapper.sh`
- Output: `/tmp/qrm-bulkhead-e2e-0-<run-tag>/`

- [ ] Run suite 1 with a unique short run tag.
- [ ] Require preflight, reset dry-run/actual, target dry-run/actual, standard 3 rounds, high-churn 5 rounds, overlap-churn 3 rounds, and final reset to return zero.
- [ ] Repeat the complete sequence for suites 2 and 3.
- [ ] Stop later suites on the first non-zero phase and preserve its output directory.

### Task 3: Generate the QoS Matrix

**Files:**
- Execute: `/root/qrm-bulkhead-e2e/scripts/qos_matrix_profiles.py`
- Output: `/tmp/qrm-bulkhead-e2e-0-<run-tag>/qrm_matrix_<run-tag>/manifests/`

- [ ] Discover physical-core width and node UID from live state.
- [ ] Generate P0 and P1 manifests for A1–A5 plus P1/A2-follow.
- [ ] Verify all eleven manifest SHA256 files and DNS-safe identities.
- [ ] Record the original AQC object and hash before the first mutation.

### Task 4: Execute P0 Mixed-QoS Cohort

**Files:**
- Execute: `/root/qrm-bulkhead-e2e/scripts/qos_matrix_cohort.sh`
- Output: `/tmp/qrm-bulkhead-e2e-0-<run-tag>/qrm_matrix_<run-tag>/cohorts/p0/`

- [ ] Apply each AQC operation only after controller dispatch persistence.
- [ ] Run A1–A5 serially with one exclusive-DNB, two DNB, two SNB, two non-SNB, and two system Pods per phase.
- [ ] Require stable, recreate, early-delete, immutable generation, checkpoint, cgroup, health, sched-domain, and schedstat checks to pass.
- [ ] Restore the original AQC and drain all run-owned state.

### Task 5: Execute P1 Mixed-QoS Cohort

**Files:**
- Execute: `/root/qrm-bulkhead-e2e/scripts/qos_matrix_cohort.sh`
- Output: `/tmp/qrm-bulkhead-e2e-0-<run-tag>/qrm_matrix_<run-tag>/cohorts/p1/`

- [ ] Run A1–A5 serially with preserve enabled.
- [ ] Execute A2 initial and A2-follow as separate controller identities.
- [ ] Verify transition envelopes bind immutable before/after generations and preserve-only topology changes.
- [ ] Restore the original AQC and drain all run-owned state.

### Task 6: Final Reset and Evidence

**Files:**
- Execute: `/root/qrm-bulkhead-e2e/scripts/reset_bulkhead_config.sh`
- Execute: `/root/qrm-bulkhead-e2e/scripts/package_e2e_logs.sh`
- Create: `qrm-bulkhead-test-artifacts/<run artifacts>`

- [ ] Perform fail-closed final reset and require `EXPECTED_STATE=OK mode=reset`.
- [ ] Package every canonical and matrix output, controller journal, transition envelope, runtime evidence cleanup record, and summary.
- [ ] Copy archives to the selected workspace folder.
- [ ] Verify remote/local size and SHA256 equality and run `tar -tzf` locally.
- [ ] Produce a cross-run summary separating transient retries from terminal failures.
