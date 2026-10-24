#!/usr/bin/env python3
# SPDX-License-Identifier: MulanPSL-2.0
"""Manual Webots regression, outside CI; reserves model-fixture port 51090."""

import argparse
import asyncio
import contextlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from http.server import ThreadingHTTPServer

import grpc
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from testing.fake_vlm.server import Handler, _json_values_from_text

GENERATED = ROOT / "examples/webots/rbnx-boot/cache/service-map-rbnx/rbnx-build/codegen/proto_gen"
sys.path.insert(0, str(GENERATED))
import atlas_pb2
import atlas_pb2_grpc
import lifecycle_pb2
import pilot_pb2
import executor_pb2

GOAL = "ci-test: trace hardware health through recorded dependencies"
SOFTWARE_PLAN = "dependency-demo-software-only"
DIRECT_PLAN = "dependency-demo-pilot-stopped"
STOP_TIMEOUT = 8
MODEL_PORT = 51090
PROTO_FILE = max(ROOT.glob("target/debug/build/robonix-vitals-*/out/vitals.proto"),
                 key=lambda path: path.stat().st_mtime, default=None)
PROTO = PROTO_FILE.parent if PROTO_FILE else None


def rpc(method, data=None, port=50061, atlas=False, timeout=10):
    """Call existing generated interfaces without adding test contracts."""
    result = subprocess.run(
        ["grpcurl", "-plaintext", "-max-time", str(timeout), "-import-path",
         str(ROOT / "system/atlas/proto") if atlas else str(PROTO),
         "-proto", "atlas.proto" if atlas else "robonix_contracts.proto",
         "-d", json.dumps(data or {}), f"127.0.0.1:{port}", method],
        check=True, capture_output=True, text=True, timeout=timeout + 2,
    )
    return json.loads(result.stdout)


def plans():
    """Read Executor's active table without creating another plan."""
    response = rpc("robonix.contracts.RobonixSystemExecutorListActivePlans/ListActivePlans")
    return json.loads(response["plansJson"])["plans"]


def hardware_plans():
    """Find the fixture's real Explore operations while they are running."""
    return [plan for plan in plans() if any(operation.get("provider_id") == "explore"
            and operation.get("state") == "running" for operation in plan.get("ops", []))]


async def wait_for(predicate, seconds, description):
    """Bound startup and fault assertions instead of assuming fixed sleeps are sufficient."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            result = await asyncio.wait_for(asyncio.to_thread(predicate),
                                            max(0.001, deadline - time.monotonic()))
        except asyncio.TimeoutError as error:
            raise AssertionError(f"Timed out waiting for {description}") from error
        if result:
            return result
        await asyncio.sleep(min(0.25, max(0, deadline - time.monotonic())))
    raise AssertionError(f"Timed out waiting for {description}")


def inject(profile, fault_file):
    """Change only this fixture's fault file inside the already-running simulator."""
    payload = {"offline": ["body/hokuyo_lidar"] if profile == "lidar" else []}
    subprocess.run(
        ["docker", "exec", "-i", os.environ.get("ROBONIX_SIM_CONTAINER", "robonix_tiago_sim"),
         "sh", "-c", 'cat > "$1.tmp" && mv "$1.tmp" "$1"', "sh", fault_file],
        input=json.dumps(payload), text=True, check=True, capture_output=True, timeout=5,
    )


def impact_from_request(request):
    """Read the newest computed context rather than an older cached alert."""
    for message in reversed(request.get("messages", [])):
        content = message.get("content", "")
        if isinstance(content, str) and "impact_json: " in content:
            values = list(_json_values_from_text(content.rsplit("impact_json: ", 1)[1]))
            if values:
                return values[0]
    return None


def dependency_envelope(request, content, node, status):
    """Use the task identity and running trees from Pilot's current status board."""
    marker = "## Tasks (current status board)\n"
    board = next((message["content"].split(marker, 1)[1]
                  for message in reversed(request.get("messages", []))
                  if isinstance(message.get("content"), str) and marker in message["content"]), None)
    if board is None:
        raise ValueError("The dependency fixture requires Pilot's current task status board")
    task_ids = re.findall(r"^- (T[0-9]+): in_progress\s*$", board, re.MULTILINE)
    if len(task_ids) != 1:
        raise ValueError(f"The dependency fixture requires one open task, received {task_ids}")
    task_id = task_ids[0]
    dispatches = node.get("op") == "do" or bool(node.get("children"))
    running = re.search(r"^  plan .+ \(running\): ", board, re.MULTILINE) is not None
    updates = []
    if status != "done" or not running:
        update = {
            "task_id": task_id, "goal": GOAL, "status": status,
            "success_criterion": "Observe Vitals cancel affected work without waiting for model replies",
        }
        if status == "done":
            update["status_reason"] = "Dependency health analysis completed."
        updates.append(update)
    return {
        "content": content,
        "message_updates": [],
        "rtdl_parent_task_id": task_id if dispatches else None,
        "rtdl_description": content,
        "rtdl": node,
        "task_updates": updates,
    }


class DependencyModel(Handler):
    started = False
    reports = []
    errors = []
    release = threading.Event()
    waiting = threading.Event()
    reply_times = []
    stage = "startup"
    requests = []
    admission = threading.Event()
    admission_lock = threading.Lock()
    admission_remaining = 0
    admission_replies = []
    partial_started = threading.Event()
    partial_release = threading.Event()
    partial_tail_sent = threading.Event()

    def do_POST(self):
        """Keep analysis blocked until the harness observes Vitals' independent stop."""
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        report = impact_from_request(request)
        if type(self).stage == "streaming_prefix" and not type(self).partial_started.is_set():
            self.stream_partial_reply(request)
            return
        content = "Starting the dependency regression exploration."
        status = "in_progress"
        if not type(self).started:
            type(self).started = True
            node = {"op": "sequence", "op_id": 0, "description": "Dependency health exploration",
                    "children": [{"op": "do", "op_id": 0, "description": "Explore using lidar",
                                  "cap": "explore.explore_explore",
                                  "args": {"area_hint": "this room", "max_speed_m_s": 0.1, "timeout_s": 300}}]}
        else:
            if report:
                type(self).reports.append(report)
                if any(candidate["plan_id"] == SOFTWARE_PLAN
                       for candidate in report.get("candidate_plans", [])):
                    type(self).errors.append("Unrelated software was marked affected")
            type(self).requests.append({"stage": type(self).stage,
                                        "received_at": time.monotonic(), "impact": report})
            type(self).waiting.set()
            if not type(self).release.wait(timeout=120):
                type(self).errors.append("Harness did not release the blocked model within 120 seconds")
                return
            time.sleep(0.25)
            node = {"op": "sequence", "op_id": 0, "description": "Health analysis only", "children": []}
            status = "done"
            content = "Vitals owns targeted stopping; health analysis dispatches no actions."
            with type(self).admission_lock:
                if type(self).admission.is_set() and type(self).admission_remaining > 0:
                    type(self).admission_remaining -= 1
                    node = {"op": "do", "op_id": 0, "description": "Admission check: explore using lidar",
                            "cap": "explore.explore_explore",
                            "args": {"area_hint": "this room", "max_speed_m_s": 0.1, "timeout_s": 300}}
                    content = "Admission check proposes the same lidar-dependent exploration."
                    status = "in_progress"
                    type(self).admission_replies.append({
                        "reply_at": time.monotonic(), "rtdl": node,
                        "blocked_feedback": "Vitals blocked this plan" in json.dumps(request.get("messages", [])),
                    })
            type(self).reply_times.append(time.monotonic())
        try:
            envelope = dependency_envelope(request, content, node, status)
        except ValueError as error:
            type(self).errors.append(str(error))
            self.send_error(400, str(error))
            return
        with contextlib.suppress(BrokenPipeError, ConnectionResetError):
            self._stream_envelope(request, envelope)

    def stream_partial_reply(self, request):
        """Pause after an accepted operator prefix, before a dependent call is sent."""
        node = {"op": "sequence", "description": "Streaming health admission", "children": [
            {"op": "do", "description": "Harmless streamed software prefix",
             "cap": "executor.builtin_run_command",
             "args": {"command": "printf STREAM_PREFIX_OK"}},
            {"op": "do", "description": "Late lidar-dependent exploration",
             "cap": "explore.explore_explore",
             "args": {"area_hint": "this room", "max_speed_m_s": 0.1, "timeout_s": 300}}
        ]}
        envelope = dependency_envelope(request, "Checking streamed admission.", node, "in_progress")
        raw = json.dumps(envelope, separators=(",", ":"))
        first_child = raw.index('"children":[') + len('"children":[')
        _, boundary = json.JSONDecoder().raw_decode(raw, first_child)
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()

        def emit(text):
            payload = {"choices": [{"index": 0, "delta": {"content": text}, "finish_reason": None}]}
            self.wfile.write(("data: " + json.dumps(payload) + "\n\n").encode())
            self.wfile.flush()

        with contextlib.suppress(BrokenPipeError, ConnectionResetError):
            emit(raw[:boundary])
            type(self).partial_started.set()
            if not type(self).partial_release.wait(timeout=30):
                type(self).errors.append("Streaming prefix was not released")
                return
            emit(raw[boundary:])
            type(self).partial_tail_sent.set()
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()


def software_stdio():
    """Hold one real MCP call until Executor cancels this subprocess during cleanup."""
    for line in sys.stdin:
        request = json.loads(line)
        if "id" not in request:
            continue
        method = request["method"]
        if method == "initialize":
            result = {"protocolVersion": request["params"]["protocolVersion"],
                      "capabilities": {"tools": {}},
                      "serverInfo": {"name": "software-only-fixture", "version": "1"}}
        elif method == "tools/list":
            result = {"tools": [{"name": "remember", "description": "Wait without hardware inputs",
                                "inputSchema": {"type": "object"}}]}
        elif method == "tools/call":
            threading.Event().wait()
            result = {"content": []}
        else:
            result = {}
        print(json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": result}), flush=True)


async def software_worker(atlas):
    """Provide an unrelated long-running software call with a real lifecycle endpoint."""
    async def driver(request, _context):
        """Publish the fixture's transitions through the real Atlas lifecycle interface."""
        state = atlas_pb2.STATE_ACTIVE if request.command == 1 else atlas_pb2.STATE_INACTIVE
        await atlas.SetLifecycleState(atlas_pb2.SetLifecycleStateRequest(id="software_wait", state=state))
        return lifecycle_pb2.Driver_Response(ok=True, state="active" if request.command == 1 else "inactive")

    server = grpc.aio.server()
    server.add_generic_rpc_handlers([
        grpc.method_handlers_generic_handler("robonix.contracts.RobonixLifecycleDriver", {
            "Driver": grpc.unary_unary_rpc_method_handler(driver,
                request_deserializer=lifecycle_pb2.Driver_Request.FromString,
                response_serializer=lifecycle_pb2.Driver_Response.SerializeToString)}),
    ])
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    try:
        await atlas.RegisterService(atlas_pb2.RegisterRequest(id="software_wait", namespace="robonix/service/memory"))
        await atlas.DeclareCapability(atlas_pb2.DeclareCapabilityRequest(
            provider_id="software_wait", contract_id="robonix/service/memory/remember",
            transport=atlas_pb2.TRANSPORT_MCP,
            endpoint=f"stdio://{sys.executable} {Path(__file__).resolve()} --software-worker",
            params=atlas_pb2.TransportParams(mcp=atlas_pb2.McpParams(input_schema_json="{}"))))
        for contract, method, service in [
            ("robonix/lifecycle/driver", "Driver", "RobonixLifecycleDriver"),
        ]:
            await atlas.DeclareCapability(atlas_pb2.DeclareCapabilityRequest(
                provider_id="software_wait", contract_id=contract, transport=atlas_pb2.TRANSPORT_GRPC,
                endpoint=f"127.0.0.1:{port}", params=atlas_pb2.TransportParams(
                    grpc=atlas_pb2.GrpcParams(proto_file="robonix_contracts.proto",
                                            service_name=f"robonix.contracts.{service}",
                                            method=f"/robonix.contracts.{service}/{method}"))))
        await atlas.SetLifecycleState(atlas_pb2.SetLifecycleStateRequest(id="software_wait", state=atlas_pb2.STATE_INACTIVE))
        await atlas.SetLifecycleState(atlas_pb2.SetLifecycleStateRequest(id="software_wait", state=atlas_pb2.STATE_ACTIVE))
    except BaseException:
        await server.stop(0)
        raise
    return server


def manifest(directory, fault_file, explore_path=None):
    """Build only the real packages needed for this fault scenario, without private VLM settings."""
    explore_root = (Path(explore_path).expanduser().resolve() if explore_path is not None
                    else ROOT / "examples/webots/rbnx-boot/cache/skill-explore-rbnx")
    fixture = {
        "manifestVersion": 1, "name": "health-dependency-regression",
        "system": {"atlas": {"listen": "127.0.0.1:50051"}, "executor": {"listen": "127.0.0.1:50061"},
                   "soma": {"robot_yaml": str(ROOT / "examples/webots/soma.yaml")},
                   "vitals": {"listen": "127.0.0.1:50093"},
                   "pilot": {"listen": "127.0.0.1:50071",
                             "vlm": {"upstream": f"http://127.0.0.1:{MODEL_PORT}/v1",
                                     "api_key": "local-test", "model": "dependency-test"}}},
        "primitive": [{"name": f"tiago_{name}", "path": str(ROOT / f"examples/webots/primitives/tiago_{name}"),
                       "config": {"variant": "lite", "sensor_timeout_s": 2.0,
                                  "fault_file": fault_file} if name == "health"
                       else {"sentinel_timeout_s": 150} if name in ("camera", "lidar") else {}}
                      for name in ("chassis", "camera", "lidar", "health")],
        "service": [
            {"name": "mapping", "path": str(ROOT / "examples/webots/rbnx-boot/cache/service-map-rbnx"),
             "config": {"algo": "slam_toolbox", "use_sim_time": True,
                        "sensor_providers": {"lidar2d": "tiago_lidar", "odom": "tiago_chassis"}}},
            {"name": "nav2", "path": str(ROOT / "examples/webots/rbnx-boot/cache/service-navigation-rbnx"),
             "config": {"params_file": "config/nav2_params.yaml",
                        "use_sim_time": True, "action_wait_s": 240,
                        "dynamic_speed": {"max_linear_speed_mps": 0.26, "default_percentage": 100,
                                          "step_percentage": 20, "min_percentage": 20},
                        "guard_terminal_xy_m": 0.20,
                        "provider_ids": {"map": "mapping", "odom": "tiago_chassis",
                                         "scan": "tiago_lidar", "depth": "tiago_camera"}}}],
        "skill": [{"name": "explore", "path": str(explore_root), "config": {}}],
    }
    camera = next(entry for entry in fixture["primitive"] if entry["name"] == "tiago_camera")
    camera["config"].update(width=640, height=480, fx=554.0, fy=554.0, cx=320.0, cy=240.0)
    (directory / "config").mkdir()
    shutil.copy2(ROOT / "examples/webots/config/nav2_params.yaml", directory / "config/nav2_params.yaml")
    path = directory / "robonix_manifest.yaml"
    path.write_text(yaml.safe_dump(fixture, sort_keys=False))
    return path


def record(events, stage, event, **details):
    """Leave ordered evidence of stopping before the model's release."""
    entry = {"stage": stage, "event": event, "monotonic_s": time.monotonic(), **details}
    events.append(entry)
    print(f"[{stage}] {event}: {json.dumps(details)}", flush=True)
    return entry["monotonic_s"]


def software_running(active):
    """Require the original software call to remain running and uncancelled."""
    return any(plan["plan_id"] == SOFTWARE_PLAN and not plan.get("cancelled")
               and any(operation.get("provider_id") == "software_wait"
                       and operation.get("state") == "running" for operation in plan.get("ops", []))
               for plan in active)


def dependency_evidence(requests, stage, cached=False):
    """Accept the existing impact report even after its candidate plans disappear."""
    for request in requests:
        report = request["impact"]
        if request["stage"] != stage or not report:
            continue
        providers = report.get("dependent_providers", [])
        if not {"tiago_lidar", "mapping", "nav2", "explore"} <= {
                provider["provider_id"] for provider in providers}:
            continue
        if any(provider["provider_id"] == "mapping"
               and (not cached or provider.get("evidence") == "last_known")
               and any(edge["provider_id"] == "tiago_lidar" and edge["consumer_id"] == "mapping"
                       for edge in provider.get("path", [])) for provider in providers):
            return report
    return None


def lidar_health(expected):
    """Reject an absent lidar reading instead of treating an empty match as recovery."""
    snapshot = rpc("robonix.contracts.RobonixSystemVitalsGet/GetVitals", port=50093).get("snapshot", {})
    components = [component for component in snapshot.get("components", [])
                  if component["name"] == "body/hokuyo_lidar"]
    return bool(components) and all(component.get("health", 0) == expected for component in components)


async def wait_stopped(plan_ids, started, stage, events, vitals_log):
    """Observe local completion and Vitals' terminal-run confirmation without issuing control."""
    def stopped():
        """Require both local completion and the health worker's successful control result."""
        active = plans()
        assert software_running(active), "Unrelated software stopped or was cancelled"
        if any(plan["plan_id"] in plan_ids for plan in active):
            return False
        evidence = log_since(vitals_log, 0)
        return all(
            f"Executor cancellation completed plan={plan_id}:" in evidence
            for plan_id in plan_ids
        )

    try:
        await wait_for(stopped, max(0.001, STOP_TIMEOUT - (time.monotonic() - started)),
                       f"{stage}: targeted plans to stop within {STOP_TIMEOUT}s of injection")
    except Exception as error:
        record(events, stage, "stop_unconfirmed", elapsed_s=round(time.monotonic() - started, 3),
               error=str(error))
        raise
    elapsed = time.monotonic() - started
    assert elapsed <= STOP_TIMEOUT, f"{stage}: cancellation took {elapsed:.3f}s"
    record(events, stage, "executor_stopped", elapsed_s=round(elapsed, 3),
           plan_ids=sorted(plan_ids), provider_status_confirmed=True)
    return elapsed


async def recover(fault_file, stopped_ids, stage, events):
    """Observe healthy recovery without permitting automatic hardware restarts."""
    await asyncio.to_thread(inject, "normal", fault_file)
    await wait_for(lambda: lidar_health(0), 15, f"{stage}: lidar recovery")
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        active = await asyncio.to_thread(plans)
        assert software_running(active), "Unrelated software did not survive recovery"
        assert not any(plan["plan_id"] in stopped_ids
                       or any(operation.get("provider_id") == "explore"
                              for operation in plan.get("ops", [])) for plan in active), \
            "Hardware work restarted automatically after recovery"
        await asyncio.sleep(0.25)
    record(events, stage, "recovered_without_restart", observation_s=5)


def pilot_pidfd(directory, path, boot):
    """Pin the fixture's verified Pilot identity so PID reuse cannot redirect signals."""
    state = json.loads((directory / "rbnx-boot/state.json").read_text())
    assert boot.poll() is None and state["boot_pid"] == boot.pid, "Fixture boot identity changed"
    assert Path(state["manifest_path"]).resolve() == path.resolve(), "Foreign fixture manifest"
    pilot = next(component for component in state["components"] if component["name"] == "pilot")
    process = Path(f"/proc/{pilot['pid']}")
    before = (process / "stat").read_text().rsplit(")", 1)[1].split()
    assert int(before[1]) == boot.pid, "Pilot is not a child of this fixture's boot"
    assert os.getpgid(pilot["pid"]) == pilot["pgid"] == pilot["pid"], "Pilot process group changed"
    assert (process / "exe").resolve() == (ROOT / "target/debug/robonix-pilot").resolve(), \
        "Pilot executable does not match the fixture"
    assert state["boot_id"] and f"RBNX_BOOT_ID={state['boot_id']}".encode() in \
        (process / "environ").read_bytes().split(b"\0"), "Pilot boot ownership changed"
    descriptor = os.pidfd_open(pilot["pid"])
    try:
        after = (process / "stat").read_text().rsplit(")", 1)[1].split()
        assert before[19] == after[19], "Pilot PID was reused during verification"
    except BaseException:
        os.close(descriptor)
        raise
    return pilot["pid"], descriptor


def stop_process(process):
    """Reap only a subprocess launched by this fixture, including a stuck shutdown."""
    if process is not None and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def log_since(path, offset):
    """Read only one stage's appended evidence without mixing earlier executions."""
    with path.open("rb") as log:
        log.seek(offset)
        return log.read().decode("utf-8", errors="replace")


async def run(explore_path=None):
    """Run real-process assertions and always cancel test work and shut down the fixture."""
    if PROTO is None:
        raise RuntimeError("Run cargo build --workspace and build the Webots packages first")
    for port in (50051, 50061, 50071, 50091, 50093, MODEL_PORT):
        with socket.socket() as listener:
            if listener.connect_ex(("127.0.0.1", port)) == 0:
                raise RuntimeError(f"Port {port} is occupied; stop the existing deployment before this demo")
    directory = Path(tempfile.mkdtemp(prefix="robonix-health-dependency-demo-"))
    fault_file = f"/tmp/{directory.name}-faults.json"
    path = manifest(directory, fault_file, explore_path)
    env = dict(os.environ, ROBONIX_SOURCE_PATH=str(ROOT), RMW_IMPLEMENTATION="rmw_zenoh_cpp")
    fixture_home = directory / "rbnx-home"
    fixture_home.mkdir()
    (fixture_home / "config.yaml").write_text(yaml.safe_dump({
        "robonix_source_path": str(ROOT),
        "package_storage_path": str(fixture_home / "packages"),
    }))
    env["ROBONIX_HOME"] = str(fixture_home)
    env["PATH"] = str(ROOT / "target/debug") + os.pathsep + env.get("PATH", "")
    env.pop("RUST_LOG", None)
    model = ThreadingHTTPServer(("127.0.0.1", MODEL_PORT), DependencyModel)
    threading.Thread(target=model.serve_forever, daemon=True).start()
    log = (directory / "boot.log").open("w")
    boot = subprocess.Popen([str(ROOT / "target/debug/rbnx"), "boot", "-f", str(path), "--no-update-check"],
                            cwd=directory, env=env, stdout=log, stderr=subprocess.STDOUT)
    channel = grpc.aio.insecure_channel("127.0.0.1:50051")
    atlas = atlas_pb2_grpc.AtlasStub(channel)
    executor_channel = grpc.aio.insecure_channel("127.0.0.1:50061")
    worker = None
    execute_calls = []
    executions = []
    asks = []
    output = None
    owned_plans = {SOFTWARE_PLAN, DIRECT_PLAN}
    events = []
    timings = {}
    verification = {"passed": False, "unrelated_plan_preserved": SOFTWARE_PLAN,
                    "cancellation_seconds": timings, "events": events,
                    "model_reply_times": DependencyModel.reply_times,
                    "model_requests": DependencyModel.requests, "impact_reports": DependencyModel.reports,
                    "unsafe_model_replies": DependencyModel.admission_replies}

    async def consume(call):
        """Drain real execution streams so Executor continues running both plans."""
        async for _event in call:
            pass

    def submit(request):
        """Submit direct plans through the same Executor interface used by Pilot."""
        assert len(request.nodes) == 1 and request.nodes[0].node_kind == 2

        async def frames():
            yield executor_pb2.ExecuteStreamInput(
                message_kind=0, plan_id=request.plan_id,
                session_id=request.session_id, round=request.round)
            yield executor_pb2.ExecuteStreamInput(message_kind=1, node_index=0, node=request.nodes[0])
            yield executor_pb2.ExecuteStreamInput(message_kind=3)

        call = executor_channel.stream_stream(
            "/robonix.contracts.RobonixSystemExecutorExecuteStream/ExecuteStream",
            request_serializer=executor_pb2.ExecuteStreamInput.SerializeToString,
            response_deserializer=lambda data: data)(frames())
        execute_calls.append(call)
        executions.append(asyncio.create_task(consume(call)))

    def start_ask():
        """Track every fixture-owned turn, including explicit analysis after stopping."""
        process = subprocess.Popen([str(ROOT / "target/debug/rbnx"), "ask", "--json", GOAL],
                                   env=env, cwd=directory, stdout=output, stderr=subprocess.STDOUT)
        asks.append(process)
        return process

    async def collect_evidence(stage, cached=False):
        """Request analysis explicitly because an empty RTDL ends the previous turn."""
        for _attempt in range(4):
            process = start_ask()
            record(events, stage, "analysis_only_ask", pid=process.pid)
            await wait_for(lambda: process.poll() is not None, 15, f"{stage}: analysis-only ask")
            assert process.returncode == 0, f"Analysis ask failed; see {directory / 'ask.log'}"
            report = dependency_evidence(DependencyModel.requests, stage, cached=cached)
            if report:
                record(events, stage, "dependency_path_verified", cached=cached)
                return
            await asyncio.sleep(2.1)
        raise AssertionError(f"{stage}: dependency path missing from the existing Pilot impact report")

    async def verify_admission():
        """Reject two unsafe replies before dispatch, with durable log evidence."""
        await wait_for(lambda: all(process.poll() is not None for process in asks),
                       15, "earlier fixture asks to finish")
        assert await asyncio.to_thread(lidar_health, 2), "Admission requires the lidar fault to persist"
        paths = {"ask": directory / "ask.log",
                 "executor": directory / "rbnx-boot/logs/executor.log",
                 "vitals": directory / "rbnx-boot/logs/vitals.log"}
        output.flush()
        offsets = {name: path.stat().st_size for name, path in paths.items()}
        evidence = {"passed": False, "log_offsets": offsets, "active_samples": []}
        verification["admission"] = evidence
        DependencyModel.stage = "admission"
        DependencyModel.admission_remaining = 2
        DependencyModel.admission.set()
        try:
            process = start_ask()
            record(events, "admission", "unsafe_ask_started", pid=process.pid, response_budget=2)

            def checked():
                """Catch admitted hardware work while preserving the original software call."""
                active = plans()
                evidence["active_samples"].append({
                    "monotonic_s": time.monotonic(), "plan_ids": [plan["plan_id"] for plan in active]})
                assert software_running(active), "Admission check interrupted unrelated software"
                assert not any(operation.get("provider_id") == "explore"
                               for plan in active for operation in plan.get("ops", [])), \
                    "Unsafe exploration entered Executor's active table"
                return process.poll() is not None

            await wait_for(checked, 15, "two unsafe replies to be rejected and the ask to end")
            assert process.returncode == 0, f"Admission ask failed; see {paths['ask']}"
            for _sample in range(4):
                await asyncio.to_thread(checked)
                await asyncio.sleep(0.25)
            tails = {name: log_since(path, offsets[name]) for name, path in paths.items()}
            evidence["log_tails"] = tails
            ask_events = [value for value in _json_values_from_text(tails["ask"])
                          if isinstance(value, dict) and "event_kind" in value]
            assert any("Vitals blocked this plan" in event.get("final_text", "") for event in ask_events), \
                "Pilot did not report the admission rejection"
            plan_events = [event["plan"] for event in ask_events if event.get("plan") is not None]
            assert not any(node.get("call", {}).get("provider_id") == "explore"
                           for plan in plan_events for node in plan.get("nodes", [])), \
                "Pilot submitted an unsafe call in its streamed prefix"
            assert not any("dispatching call_id=" in line and "provider='explore'" in line
                           for line in tails["executor"].splitlines()), \
                "Executor dispatched the unsafe exploration before cancellation"
            for line in tails["vitals"].splitlines():
                marker = "requesting targeted cancel plan="
                if marker in line:
                    plan_id = line.split(marker, 1)[1].split()[0].rstrip('"}')
                    assert plan_id in owned_plans, f"Vitals had to cancel a newly admitted plan: {plan_id}"
            replies = DependencyModel.admission_replies
            assert len(replies) == 2 and DependencyModel.admission_remaining == 0, \
                "The admission test did not consume exactly two unsafe replies"
            assert replies[0]["rtdl"] == replies[1]["rtdl"] and replies[1]["blocked_feedback"], \
                "The second response did not retry the same blocked operation"
            evidence.update(passed=True, unsafe_response_count=2,
                            plan_events=len(plan_events), unsafe_call_nodes=0, explore_dispatches=0)
            record(events, "admission", "rejected_before_dispatch", unsafe_responses=2)
        finally:
            DependencyModel.admission.clear()

    async def verify_streaming_prefix():
        """Inject a fault after START/operator acceptance but before the model's do node."""
        await wait_for(lambda: all(process.poll() is not None for process in asks),
                       15, "prior asks to finish before streaming admission")
        assert await asyncio.to_thread(lidar_health, 0)
        executor_log = directory / "rbnx-boot/logs/executor.log"
        offset = executor_log.stat().st_size
        pilot_log = directory / "rbnx-boot/logs/pilot.log"
        pilot_offset = pilot_log.stat().st_size
        DependencyModel.stage = "streaming_prefix"
        process = start_ask()
        try:
            await wait_for(DependencyModel.partial_started.is_set, 15, "paused model prefix")

            def prefix():
                return [plan for plan in plans()
                        if plan.get("description") == "Streaming health admission"
                        and any(operation.get("provider_id") == "executor"
                                and operation.get("state") == "succeeded"
                                for operation in plan.get("ops", []))]

            pending = await wait_for(prefix, 10, "Executor acceptance before the model tail")
            prefix_ids = {plan["plan_id"] for plan in pending}
            owned_plans.update(prefix_ids)
            assert all(operation.get("provider_id") in ("", "executor") for plan in pending
                       for operation in plan.get("ops", []))
            record(events, "streaming_prefix", "operator_accepted_before_fault",
                   plan_ids=sorted(prefix_ids))
            await asyncio.to_thread(inject, "lidar", fault_file)
            await wait_for(lambda: lidar_health(2), 10, "streaming-prefix lidar fault")
            await wait_for(lambda: "[pilot/vitals] health alert received" in
                           log_since(pilot_log, pilot_offset), 10, "Pilot observation before model tail")
            DependencyModel.partial_release.set()
            await wait_for(lambda: process.poll() is not None, 20, "faulted prefix cancellation")
            assert process.returncode == 0
            await wait_for(lambda: not (prefix_ids & {plan["plan_id"] for plan in plans()}),
                           10, "streaming prefix terminal confirmation")
            assert software_running(await asyncio.to_thread(plans))
            tail = log_since(executor_log, offset)
            assert not any("dispatching call_id=" in line and "provider='explore'" in line
                           for line in tail.splitlines()), "Late unsafe do node reached dispatch"
            safe_dispatches = [line for line in tail.splitlines()
                               if "dispatching call_id=" in line and "STREAM_PREFIX_OK" in line]
            assert len(safe_dispatches) == 1, "Completed software prefix was replayed"
            verification["streaming_prefix"] = {
                "passed": True, "plan_ids": sorted(prefix_ids),
                "model_tail_sent": DependencyModel.partial_tail_sent.is_set(),
                "explore_dispatches": 0, "safe_prefix_dispatches": len(safe_dispatches),
            }
            record(events, "streaming_prefix", "late_call_rejected", explore_dispatches=0)
            await recover(fault_file, prefix_ids, "streaming_prefix", events)
        finally:
            DependencyModel.partial_release.set()

    try:
        def ready():
            """Surface boot failures while waiting for the fixture to own its ports."""
            text = (directory / "boot.log").read_text()
            assert "[FAIL]" not in text, text[-2000:]
            assert boot.poll() is None or "component(s) up" in text, text[-2000:]
            return "component(s) up" in text
        await wait_for(ready, 180, "fixture startup")
        print("Artifacts:", directory, flush=True)
        await asyncio.to_thread(inject, "normal", fault_file)
        await wait_for(lambda: lidar_health(0), 20, "healthy lidar baseline")
        worker = await software_worker(atlas)
        request = pilot_pb2.Plan(plan_id=SOFTWARE_PLAN, session_id="dependency-test", nodes=[
            pilot_pb2.RtdlNode(node_kind=2, op_id="software-op", description="Independent software-only work",
                              call=pilot_pb2.CapabilityCall(provider_id="software_wait",
                                  contract_id="robonix/service/memory/remember",
                                  call_id=SOFTWARE_PLAN + ":0",
                                  args_json='{"request_json":{"data":"unrelated software work"}}'))])
        submit(request)
        await wait_for(lambda: software_running(plans()), 15, "running software-only plan")
        output = (directory / "ask.log").open("w")
        DependencyModel.stage = "model_blocked"
        start_ask()
        running = await wait_for(hardware_plans, 45, "real exploration execution")
        owned_plans.update(plan["plan_id"] for plan in running)
        await asyncio.sleep(5)
        target_ids = {plan["plan_id"] for plan in running}
        assert target_ids <= {plan["plan_id"] for plan in await asyncio.to_thread(plans)}, \
            "Exploration ended before the first fault"
        fault_at = record(events, "model_blocked", "inject_error")
        await asyncio.to_thread(inject, "lidar", fault_file)
        timings["model_blocked"] = await wait_stopped(
            target_ids, fault_at, "model_blocked", events,
            directory / "rbnx-boot/logs/vitals.log",
        )
        await wait_for(lambda: lidar_health(2), 10, "lidar ERROR")
        await wait_for(DependencyModel.waiting.is_set, 15, "blocked model analysis")
        assert not DependencyModel.release.is_set() and not DependencyModel.reply_times, \
            "The model answered before independent stopping was verified"
        released_at = record(events, "model_blocked", "release_model_after_stop")
        assert fault_at + timings["model_blocked"] < released_at
        DependencyModel.release.set()
        await collect_evidence("model_blocked")
        await recover(fault_file, target_ids, "model_blocked", events)

        DependencyModel.release.clear()
        pilot_pid, descriptor = pilot_pidfd(directory, path, boot)
        try:
            signal.pidfd_send_signal(descriptor, signal.SIGSTOP)
            await wait_for(lambda: Path(f"/proc/{pilot_pid}/stat").read_text()
                           .rsplit(")", 1)[1].split()[0] == "T", 3, "fixture Pilot SIGSTOP")
            record(events, "pilot_stopped", "pilot_sigstop_verified", pid=pilot_pid)
            submit(pilot_pb2.Plan(plan_id=DIRECT_PLAN, session_id="dependency-test-direct", nodes=[
                pilot_pb2.RtdlNode(node_kind=2, op_id="direct-explore-op", description="Explore with Pilot stopped",
                                  call=pilot_pb2.CapabilityCall(provider_id="explore",
                                      contract_id="robonix/skill/explore/explore", call_id=DIRECT_PLAN + ":0",
                                      args_json=json.dumps({"area_hint": "this room", "max_speed_m_s": 0.1,
                                                            "timeout_s": 300})))]))
            await wait_for(lambda: any(plan["plan_id"] == DIRECT_PLAN for plan in hardware_plans()),
                           20, "direct Explore execution while Pilot is stopped")
            inspected = await atlas.InspectAtlas(atlas_pb2.InspectAtlasRequest(), timeout=5)
            channels = json.loads(inspected.json)["channels"]
            assert any(edge["provider_id"] == "tiago_lidar" and edge["consumer_id"] == "mapping"
                       for edge in channels.values()), "No live Mapping-to-lidar dependency was recorded"
            await atlas.SetLifecycleState(atlas_pb2.SetLifecycleStateRequest(
                id="tiago_lidar", state=atlas_pb2.STATE_TERMINATED,
                detail="owned dependency-demo fixture fault"), timeout=5)
            for channel_id, edge in channels.items():
                if edge["provider_id"] == "tiago_lidar":
                    await atlas.DisconnectCapability(
                        atlas_pb2.DisconnectCapabilityRequest(channel_id=channel_id), timeout=5)
            inspected = await atlas.InspectAtlas(atlas_pb2.InspectAtlasRequest(), timeout=5)
            assert not any(edge["provider_id"] == "tiago_lidar"
                           for edge in json.loads(inspected.json)["channels"].values()), \
                "Lidar channels survived cleanup"
            assert any(plan["plan_id"] == DIRECT_PLAN for plan in await asyncio.to_thread(plans)), \
                "Direct exploration ended before the health fault"
            fault_at = record(events, "pilot_stopped", "inject_error_after_channel_removal")
            await asyncio.to_thread(inject, "lidar", fault_file)
            timings["pilot_stopped"] = await wait_stopped(
                {DIRECT_PLAN}, fault_at, "pilot_stopped", events,
                directory / "rbnx-boot/logs/vitals.log",
            )
            assert Path(f"/proc/{pilot_pid}/stat").read_text().rsplit(")", 1)[1].split()[0] == "T", \
                "Pilot resumed before independent stopping was verified"
        finally:
            DependencyModel.stage = "pilot_resumed"
            with contextlib.suppress(ProcessLookupError):
                signal.pidfd_send_signal(descriptor, signal.SIGCONT)
            os.close(descriptor)
            record(events, "pilot_resumed", "pilot_sigcont", pid=pilot_pid)
            DependencyModel.release.set()
        await collect_evidence("pilot_resumed", cached=True)
        await verify_admission()
        await recover(fault_file, target_ids | {DIRECT_PLAN}, "pilot_resumed", events)
        await verify_streaming_prefix()
        assert not DependencyModel.errors, DependencyModel.errors
        assert all(reply_at >= released_at for reply_at in DependencyModel.reply_times), \
            "A follow-up model response preceded the first release"
        verification["passed"] = True
        print("PASS: Vitals stopped exploration before the blocked model was released", flush=True)
        print("PASS: Vitals stopped direct exploration with Pilot SIGSTOP; software stayed running", flush=True)
        print("PASS: Pilot rejected both unsafe replies before any Explore dispatch", flush=True)
        print("PASS: Mapping last_known path retained; recovery did not restart hardware work", flush=True)
        print("PASS: A fault after streamed prefix acceptance blocked the late hardware call", flush=True)
    except BaseException as error:
        verification["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        (directory / "verification.json").write_text(json.dumps(verification, indent=2))
        DependencyModel.release.set()
        DependencyModel.partial_release.set()
        for process in asks:
            await asyncio.to_thread(stop_process, process)
        with contextlib.suppress(Exception):
            await asyncio.to_thread(inject, "normal", fault_file)
            for plan in await asyncio.to_thread(plans):
                if plan["plan_id"] in owned_plans:
                    await asyncio.to_thread(rpc, "robonix.contracts.RobonixSystemExecutorControlPlan/ControlPlan",
                                            {"action": "cancel", "planId": plan["plan_id"], "waitMs": 3000})
        for call in execute_calls:
            call.cancel()
        for execution in executions:
            execution.cancel()
        await asyncio.gather(*executions, return_exceptions=True)
        if worker is not None:
            await worker.stop(0)
        await executor_channel.close()
        await channel.close()
        try:
            await asyncio.to_thread(subprocess.run, [str(ROOT / "target/debug/rbnx"), "shutdown"],
                                    cwd=directory, env=env, check=True, timeout=45)
        finally:
            await asyncio.to_thread(stop_process, boot)
            await asyncio.to_thread(model.shutdown)
            model.server_close()
            log.close()
            if output is not None:
                output.close()
            await asyncio.to_thread(subprocess.run, [
                "docker", "exec", os.environ.get("ROBONIX_SIM_CONTAINER", "robonix_tiago_sim"),
                "rm", "-f", "--", fault_file, fault_file + ".tmp"],
                check=True, capture_output=True, timeout=5)


if __name__ == "__main__":
    if "--software-worker" in sys.argv:
        software_stdio()
    else:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("--explore-path", type=Path,
                            help="Explore package checkout to use instead of the Webots cache")
        args = parser.parse_args()
        asyncio.run(run(args.explore_path))
