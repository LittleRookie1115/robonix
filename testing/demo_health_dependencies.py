#!/usr/bin/env python3
# SPDX-License-Identifier: MulanPSL-2.0
"""Verify health dependency tracing against real Webots packages and a deterministic VLM."""

import asyncio
import contextlib
import json
import os
from pathlib import Path
import shutil
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

GOAL = "ci-test: trace hardware health through recorded dependencies"
SOFTWARE_PLAN = "dependency-demo-software-only"
MODEL_PORT = 51090
PROTO_FILE = max(ROOT.glob("target/debug/build/*/out/vitals.proto"),
                 key=lambda path: path.stat().st_mtime, default=None)
PROTO = PROTO_FILE.parent if PROTO_FILE else None


def rpc(method, data=None, port=50061, atlas=False):
    """Call existing generated interfaces without adding test contracts."""
    result = subprocess.run(
        ["grpcurl", "-plaintext", "-max-time", "10", "-import-path",
         str(ROOT / "system/atlas/proto") if atlas else str(PROTO),
         "-proto", "atlas.proto" if atlas else "robonix_contracts.proto",
         "-d", json.dumps(data or {}), f"127.0.0.1:{port}", method],
        check=True, capture_output=True, text=True, timeout=15,
    )
    return json.loads(result.stdout)


def plans():
    response = rpc("robonix.contracts.RobonixSystemExecutorListActivePlans/ListActivePlans")
    return json.loads(response["plansJson"])["plans"]


def hardware_plans():
    return [plan for plan in plans() if any(operation.get("provider_id") == "explore"
            and operation.get("state") == "running" for operation in plan.get("ops", []))]


async def wait_for(predicate, seconds, description):
    """Bound startup and fault assertions instead of assuming fixed sleeps are sufficient."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        await asyncio.sleep(0.5)
    raise AssertionError(f"Timed out waiting for {description}")


def inject(profile):
    subprocess.run(["bash", str(ROOT / "examples/webots/primitives/tiago_health/scripts/inject_fault.sh"), profile],
                   check=True, timeout=12)


def impact_from_request(request):
    """Read the newest computed context rather than an older cached alert."""
    for message in reversed(request.get("messages", [])):
        content = message.get("content", "")
        if isinstance(content, str) and "impact_json: " in content:
            values = list(_json_values_from_text(content.rsplit("impact_json: ", 1)[1]))
            if values:
                return values[0]
    return None


class DependencyModel(Handler):
    started = False
    reports = []
    errors = []

    def do_POST(self):
        """A deliberately simple model chooses cancellation only from supplied evidence."""
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        report = impact_from_request(request)
        content = "Starting the dependency regression exploration."
        status = "in_progress"
        if not type(self).started:
            type(self).started = True
            node = {"op": "sequence", "op_id": 0, "description": "Dependency health exploration",
                    "children": [{"op": "do", "op_id": 0, "description": "Explore using lidar",
                                  "cap": "explore.explore_explore",
                                  "args": {"area_hint": "this room", "max_speed_m_s": 0.1, "timeout_s": 300}}]}
        elif report and report.get("candidate_plans"):
            candidates = report["candidate_plans"]
            ids = [candidate["plan_id"] for candidate in candidates]
            names = {provider["provider_id"] for provider in report["dependent_providers"]}
            valid = {"tiago_lidar", "mapping", "nav2", "explore"} <= names and SOFTWARE_PLAN not in ids
            if not valid:
                type(self).errors.append("Missing dependency chain or unrelated software was marked affected")
            type(self).reports.append(report)
            node = {"op": "cancel_plan", "plan_id": ids[0], "wait_ms": 10000}
            content = "Recorded dependencies identify the affected exploration plan; unrelated software stays running."
        elif type(self).reports:
            node = {"op": "sequence", "op_id": 0, "description": "Cancellation complete", "children": []}
            status = "done"
            content = "The affected hardware plan was cancelled; software-only work was preserved."
        else:
            node = {"op": "sequence", "op_id": 0, "description": "Await health update", "children": []}
            content = "Exploration is running; waiting for a health transition."
        self._stream_envelope(request, {"content": content, "rtdl_description": content,
                                      "rtdl": node, "task_update": {"goal": GOAL, "status": status,
                                      "success_criterion": "Cancel only the hardware-dependent plan"}})


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


def manifest(directory):
    """Build only the real packages needed for this fault scenario, without private VLM settings."""
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
                                  "fault_file": "/tmp/robonix-tiago-health-faults.json"} if name == "health"
                       else {"sentinel_timeout_s": 150} if name in ("camera", "lidar") else {}}
                      for name in ("chassis", "camera", "lidar", "health")],
        "service": [
            {"name": "mapping", "path": str(ROOT / "examples/webots/rbnx-boot/cache/service-map-rbnx"),
             "config": {"algo": "rtabmap", "use_sim_time": True,
                        "sensor_providers": {"lidar2d": "tiago_lidar", "odom": "tiago_chassis"}}},
            {"name": "nav2", "path": str(ROOT / "examples/webots/rbnx-boot/cache/service-navigation-rbnx"),
             "config": {"params_file": "config/nav2_params.yaml", "use_sim_time": True, "action_wait_s": 240,
                        "dynamic_speed": {"max_linear_speed_mps": 0.26, "default_percentage": 100,
                                          "step_percentage": 20, "min_percentage": 20},
                        "guard_terminal_xy_m": 0.20,
                        "provider_ids": {"map": "mapping", "odom": "tiago_chassis", "scan": "tiago_lidar"}}}],
        "skill": [{"name": "explore", "path": str(ROOT / "examples/webots/rbnx-boot/cache/skill-explore-rbnx"), "config": {}}],
    }
    (directory / "config").mkdir()
    shutil.copy2(ROOT / "examples/webots/config/nav2_params.yaml", directory / "config/nav2_params.yaml")
    path = directory / "robonix_manifest.yaml"
    path.write_text(yaml.safe_dump(fixture, sort_keys=False))
    return path


async def run():
    """Run real-process assertions and always cancel test work and shut down the fixture."""
    if PROTO is None:
        raise RuntimeError("Run cargo build --workspace and build the Webots packages first")
    for port in (50051, 50061, 50071, 50093, MODEL_PORT):
        with socket.socket() as listener:
            if listener.connect_ex(("127.0.0.1", port)) == 0:
                raise RuntimeError(f"Port {port} is occupied; stop the existing deployment before this demo")
    directory = Path(tempfile.mkdtemp(prefix="robonix-health-dependency-demo-"))
    path = manifest(directory)
    env = dict(os.environ, ROBONIX_SOURCE_PATH=str(ROOT), RMW_IMPLEMENTATION="rmw_zenoh_cpp")
    env["PATH"] = str(ROOT / "target/debug") + os.pathsep + env.get("PATH", "")
    env.pop("RUST_LOG", None)
    model = ThreadingHTTPServer(("127.0.0.1", MODEL_PORT), DependencyModel)
    threading.Thread(target=model.serve_forever, daemon=True).start()
    log = (directory / "boot.log").open("w")
    boot = subprocess.Popen([str(ROOT / "target/debug/rbnx"), "boot", "-f", str(path), "--no-update-check"],
                            cwd=directory, env=env, stdout=log, stderr=subprocess.STDOUT)
    channel = grpc.aio.insecure_channel("127.0.0.1:50051")
    atlas = atlas_pb2_grpc.AtlasStub(channel)
    worker = None
    execute_call = None
    ask = None
    owned_plans = {SOFTWARE_PLAN}
    try:
        def ready():
            text = (directory / "boot.log").read_text()
            assert "[FAIL]" not in text, text[-2000:]
            assert boot.poll() is None or "component(s) up" in text, text[-2000:]
            return "component(s) up" in text
        await wait_for(ready, 180, "fixture startup")
        inject("normal")
        worker = await software_worker(atlas)
        executor_channel = grpc.aio.insecure_channel("127.0.0.1:50061")
        request = pilot_pb2.Plan(plan_id=SOFTWARE_PLAN, session_id="dependency-test", nodes=[
            pilot_pb2.RtdlNode(node_kind=2, op_id="software-op", description="Independent software-only work",
                              call=pilot_pb2.CapabilityCall(provider_id="software_wait",
                                  contract_id="robonix/service/memory/remember",
                                  call_id=SOFTWARE_PLAN + ":0",
                                  args_json='{"request_json":{"data":"unrelated software work"}}'))])
        execute_call = executor_channel.unary_stream(
            "/robonix.contracts.RobonixSystemExecutorExecute/Execute",
            request_serializer=pilot_pb2.Plan.SerializeToString,
            response_deserializer=lambda data: data)(request)
        async def consume():
            async for _event in execute_call:
                pass
        execution = asyncio.create_task(consume())
        await wait_for(lambda: any(plan["plan_id"] == SOFTWARE_PLAN for plan in plans()), 15, "software-only plan")
        output = (directory / "ask.log").open("w")
        ask = subprocess.Popen([str(ROOT / "target/debug/rbnx"), "ask", "--json", GOAL],
                               env=env, cwd=directory, stdout=output, stderr=subprocess.STDOUT)
        running = await wait_for(hardware_plans, 45, "real exploration execution")
        owned_plans.update(plan["plan_id"] for plan in running)
        await asyncio.sleep(5)
        await atlas.SetLifecycleState(atlas_pb2.SetLifecycleStateRequest(
            id="tiago_lidar", state=atlas_pb2.STATE_TERMINATED, detail="dependency fault regression"))
        inspected = await atlas.InspectAtlas(atlas_pb2.InspectAtlasRequest())
        for channel_id, edge in json.loads(inspected.json)["channels"].items():
            if edge["provider_id"] == "tiago_lidar":
                await atlas.DisconnectCapability(atlas_pb2.DisconnectCapabilityRequest(channel_id=channel_id))
        inspected = await atlas.InspectAtlas(atlas_pb2.InspectAtlasRequest())
        assert not any(edge["provider_id"] == "tiago_lidar"
                       for edge in json.loads(inspected.json)["channels"].values())
        await asyncio.sleep(3)
        inject("lidar")
        await wait_for(lambda: bool(DependencyModel.reports), 30, "computed impact reaching the VLM")
        await wait_for(lambda: not hardware_plans(), 30, "targeted cancellation")
        assert any(plan["plan_id"] == SOFTWARE_PLAN for plan in plans()), "Unrelated software was cancelled"
        assert not DependencyModel.errors, DependencyModel.errors
        assert any(provider["provider_id"] == "mapping" and provider["evidence"] == "last_known"
                   and any(edge["provider_id"] == "tiago_lidar" for edge in provider["path"])
                   for report in DependencyModel.reports for provider in report["dependent_providers"]), \
            "Removed sensor channel evidence was not retained"
        inject("normal")
        await wait_for(lambda: rpc("robonix.contracts.RobonixSystemVitalsGet/GetVitals", port=50093)
            .get("snapshot", {}).get("components")
            and all(component.get("health", 0) == 0 for component in
                    rpc("robonix.contracts.RobonixSystemVitalsGet/GetVitals", port=50093)["snapshot"]["components"]
                    if component["name"] == "body/hokuyo_lidar"), 15, "hardware recovery")
        report = {"passed": True, "unrelated_plan_preserved": SOFTWARE_PLAN,
                  "impact_reports": DependencyModel.reports}
        (directory / "verification.json").write_text(json.dumps(report, indent=2))
        print("PASS: dependency evidence reaches the VLM after Atlas removes the lidar channel")
        print("PASS: exploration cancelled while software-only work remains active")
        print("Artifacts:", directory)
        execution.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await execution
        await executor_channel.close()
    finally:
        if ask is not None and ask.poll() is None:
            ask.terminate()
            ask.wait(timeout=10)
        with contextlib.suppress(Exception):
            inject("normal")
            for plan in plans():
                if plan["plan_id"] in owned_plans:
                    rpc("robonix.contracts.RobonixSystemExecutorControlPlan/ControlPlan",
                        {"action": "cancel", "planId": plan["plan_id"], "waitMs": 3000})
        if execute_call is not None:
            execute_call.cancel()
        if worker is not None:
            await worker.stop(0)
        await channel.close()
        subprocess.run([str(ROOT / "target/debug/rbnx"), "shutdown"], cwd=directory, env=env, timeout=45)
        if boot.poll() is None:
            boot.terminate()
            boot.wait(timeout=15)
        model.shutdown()
        log.close()


if __name__ == "__main__":
    if "--software-worker" in sys.argv:
        software_stdio()
    else:
        asyncio.run(run())
