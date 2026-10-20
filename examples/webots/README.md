# Webots Tiago example

End-to-end demo: Webots Tiago sim + per-device robonix driver packages
+ Nav2 service + agent loop. Goal: bring it up and control the robot
from `rbnx chat`.

## Layout

```
examples/webots/
├── sim/                       NOT a robonix package. Plain docker
│   ├── start.sh               compose stack (Webots + eaios_webots).
│   └── ...                    Bring up FIRST, before anything else.
├── boot.sh                    Variant-aware wrapper around rbnx boot.
├── soma.yaml                  TIAGo Lite body description (default).
├── soma.full.yaml             Full single-arm TIAGo body description.
├── primitives/                One device = one package.
│   ├── tiago_chassis/         /amcl_pose + /cmd_vel  → chassis/{state, move}
│   ├── tiago_camera/          /head_front_camera/*   → camera/{snapshot, depth_snapshot}
│   ├── tiago_lidar/           /scanner               → lidar/snapshot
│   ├── tiago_health/          simulated health and demo fault injection
│   └── audio_driver/          (separate, mic/spkr — old schema, not deployed yet)
├── services/
│   └── tiago_nav2/            Nav2 launch + ActionClient wrapper
└── robonix_manifest.yaml      Top-level deploy manifest.
```

Drivers run **inside** the sim container via `docker exec` so they share
the simulator's DDS graph. They are NOT host-side processes; the host
only needs `rbnx`, Docker, and an X11 display.

## Bring-up

Two terminals only:

```bash
# T1 — sim (Ctrl-C stops):
bash examples/webots/sim/start.sh

# T2 — robonix stack (whatever robonix_manifest.yaml declares):
bash examples/webots/boot.sh
```

`rbnx boot` from `examples/webots/` remains equivalent to the default Lite
command. The wrapper is required when selecting the full variant because it
keeps Soma and simulated health on the same robot description.

## TIAGo variants

The default `lite` variant is the existing TIAGo base and body without an arm:

```bash
# Terminal 1
bash examples/webots/sim/start.sh --tiago-variant lite

# Terminal 2
bash examples/webots/boot.sh --tiago-variant lite
```

The `full` variant uses Cyberbotics Webots R2025a `Tiago`: the same mobile
base and sensors plus its seven-axis front arm and default parallel gripper:

```bash
# Terminal 1 (keep this running)
bash examples/webots/sim/start.sh --tiago-variant full

# Terminal 2
bash examples/webots/boot.sh --tiago-variant full
```

The launcher automatically downloads the official Webots R2025a asset bundle
(about 661 MB) on the first full start; the Docker volume caches it for later
starts. Set `ROBONIX_WEBOTS_DOWNLOAD_ALL_ASSETS=0` only when the cache is
already populated by another method. Both terminals must use the same variant.
The full profile registers arm and gripper joint interfaces and reports their
nominal health, but it does not yet deploy a Robonix arm/gripper primitive, so
agent plans cannot command manipulation tasks yet.

W3D streaming clients resolve mesh and HDR URLs in the browser as well as in
Webots. The computer running the browser therefore needs direct or proxied
access to `raw.githubusercontent.com`; the server-side Docker cache does not
replace that browser requirement.

The sim launcher supports multiple built-in worlds:

```bash
bash examples/webots/sim/start.sh --world office.wbt
bash examples/webots/sim/start.sh --world apartment.wbt
bash examples/webots/sim/start.sh --world complete_apartment.wbt
bash examples/webots/sim/start.sh --world break_room.wbt
bash examples/webots/sim/start.sh --world kitchen.wbt
```

You can also pre-export `ROBONIX_WEBOTS_WORLD=<world>.wbt`.

`office.wbt` is the default. Its small, checksum-pinned cache downloads once
from the
[`syswonder/robonix-assets`](https://github.com/syswonder/robonix-assets/releases/tag/webots-office-seed-v3)
Release through `https://ghfast.top/` and is then reused from the persistent
Docker volume. For `apartment.wbt`, `complete_apartment.wbt`, `break_room.wbt`,
and `kitchen.wbt`, enable the official Webots offline asset bundle once:

```bash
ROBONIX_WEBOTS_DOWNLOAD_ALL_ASSETS=1 \
  bash examples/webots/sim/start.sh --world apartment.wbt
```

This downloads Cyberbotics' `assets-R2025a.zip` release asset through the same
mirror and stores it in the persistent Webots cache volume. Later runs reuse
the cache.

|  |  |
|---|---|
| `office.wbt`<br>![office](sim/thumbnails/office.jpg) | `apartment.wbt`<br>![apartment](sim/thumbnails/apartment.jpg) |
| `complete_apartment.wbt`<br>![complete apartment](sim/thumbnails/complete_apartment.jpg) | `break_room.wbt`<br>![break room](sim/thumbnails/break_room.jpg) |
| `kitchen.wbt`<br>![kitchen](sim/thumbnails/kitchen.jpg) |  |

Then a third terminal for `rbnx chat`. `rbnx caps` lists the
capabilities atlas knows about; `rbnx tools` lists the MCP tools
the LLM agent can call.

## Navigation obstacles

Mapping loads `config/rtabmap_params.yaml` to ray-trace observed free space
and clear the TIAGo base's 0.272 metre radius under its recorded poses. Without
that configuration, sparse free cells and an unknown cell under the robot can
prevent a frontier explorer from finding connected targets.
Laser scans use best-effort sensor QoS so mapping prioritizes current scans.

Navigation uses both the planar lidar and the head camera's registered depth.
The existing navigation service converts depth and camera intrinsics discovered
through Atlas into a point cloud. Both costmaps use a spatio-temporal voxel
layer to retain obstacles with heights between -0.05 and 1.3 metres in the
costmap frame, including window sills and table tops that the low lidar cannot
see. The controller checks the Soma footprint with a 0.03 metre margin.
Nav2's collision monitor also observes the lidar and depth point cloud.
Nav2 checks that it can plan to a goal before dispatching motion. The simulator
planner does not silently substitute a nearby goal for an obstructed frontier.

The deployment's navigation behavior tree replans and retains spin, wait and
backup recovery, but never automatically clears either costmap. Clearing near
an overhang can erase its last observation after it enters the depth blind
zone. If the depth point cloud stops advancing for one second, the navigation
service's final velocity guard latches zero velocity output and requests
cancellation of the current navigation UUID. A fresh goal and fresh depth
data are required to resume. The timeout is
configurable through `depth_obstacles.source_timeout_s`.
This guard covers Nav2's velocity output, not direct chassis motion commands.

The camera has a 0.6 metre minimum depth range. Avoidance must see an overhang
before entering that blind zone; enabling depth is not a contact sensor or a
guarantee that a robot already touching an obstacle can recover autonomously.
Keep the camera TF and intrinsics consistent with the simulator. A blocked
robot with healthy sensor streams is a navigation failure, not a disconnected
camera or lidar.

## Simulated hardware health

`rbnx boot` starts `tiago_health` during Soma stage 1. The primitive publishes
simulated battery, wheel, and audio readings plus camera and lidar sample
availability every 500 ms. Soma maps them onto the component tree in `soma.yaml`.
With the `full`
variant it also publishes all seven arm joints and the gripper against
`soma.full.yaml`.

The deployment manifest declares Vitals as a built-in system component, so
`rbnx boot` starts it automatically after Soma and Pilot. Soma uses `50091`
and voiceprint uses `50092` in this deployment, so Vitals listens on `50093`.
If a cached package reports that its generated lifecycle Driver is missing or
outdated, refresh the manifest's generated artifacts before boot:

```bash
rbnx build -f examples/webots/robonix_manifest.yaml --no-update-check
```

Confirm that Atlas sees it as active:

```bash
rbnx caps -v | rg vitals
```

Query the normalized result:

```bash
cd /path/to/robonix
PROTO_DIR=$(ls -td target/debug/build/robonix-vitals-*/out | head -n1)

grpcurl -plaintext \
  -import-path "$PROTO_DIR" \
  -proto robonix_contracts.proto \
  -d '{}' \
  127.0.0.1:50093 \
  robonix.contracts.RobonixSystemVitalsGet/GetVitals | jq
```

Expected power values are approximately `82%` and `24.8 V`; both wheels are
online with enabled torque and temperatures below the default thresholds.

### Demonstrate a hardware fault and recovery

Keep the Webots and `rbnx boot` terminals running. Start a long-running
exploration task that uses lidar, then inject a lidar fault from a third
terminal:

```bash
bash examples/webots/primitives/tiago_health/scripts/inject_fault.sh lidar
```

The helper updates the local fault file inside the simulator container without
restarting Webots or any Robonix component. The next health frame reports
`body/hokuyo_lidar` offline; Soma maps it to `ERROR`. Vitals independently
requests cancellation of plans that depend on lidar through Executor without
waiting for model analysis. Pilot receives the alert for subsequent planning.
Unrelated plans continue. Inspect the Vitals snapshot with the `grpcurl`
command above, the Executor active-plan table, and Vitals logs for
`[vitals/stop]`. A cancellation request alone does not prove completion:
check local plan completion in Executor's active table and a completed control
result with terminal async run status. Physical stopping needs separate observation.

Restore the sensor and verify the health transition:

```bash
bash examples/webots/primitives/tiago_health/scripts/inject_fault.sh normal
```

Use `camera` instead of `lidar` to inject a camera fault. The helper accepts
only these two monitored components. `ERROR` and `STALE` trigger targeted
stopping from recorded dependencies; `WARN` alone does not cancel a plan.
Recovery does not automatically restart cancelled work.

Pilot also receives a computed dependency impact block. Soma's `exports` must
use the instance names from this deployment (`tiago_lidar`, `mapping`, `nav2`,
and `explore`), and consumers must retain their Atlas channels while using an
input. The impact block lists recorded input contracts, dependent services and
skills, and matching active plan ids instead of requiring the VLM to infer
dependencies from tool names.

To verify that stopping works independently of both model replies and Pilot,
stop the Robonix stack but keep the Lite simulator running. Build the current
Rust programs and generated Webots package artifacts, then run the demo from
the repository root:

```bash
cargo build --workspace
rbnx build -f examples/webots/robonix_manifest.yaml --no-update-check
python3 testing/demo_health_dependencies.py
```

The script needs `grpcio`, `protobuf`, and `PyYAML` in its Python environment
and uses the generated Mapping bindings. It also needs Linux with Python
`pidfd` support, `grpcurl`, and access to the running Lite simulator through
Docker. `ROBONIX_SIM_CONTAINER` selects the container when its name differs
from `robonix_tiago_sim`.

The fixture uses Atlas `50051`, Executor `50061`, Pilot `50071`, Soma `50091`,
Vitals `50093`, and a local fake VLM on `51090`. It refuses occupied ports.
It supplies its own model configuration and a unique health fault file inside
the simulator container.

1. **Blocked model.** Start a real Explore task through Pilot and a separate
   software plan using an MCP stdio worker. After warming the dependency
   graph, inject lidar `ERROR`. The fake model holds every follow-up reply
   behind a `threading.Event` and never emits `cancel_plan`. The target plan
   must leave Executor's active table and have a Vitals log confirming completed
   cancellation within **8 seconds of starting fault injection**, while the
   original software operation remains running and
   uncancelled. Only after observing the stop does the script release model
   replies. The existing `impact_json` in Pilot's requests must eventually
   include `tiago_lidar`, `mapping`, `nav2`, and `explore` with the recorded
   Mapping-to-lidar path. Its `candidate_plans` may already be empty. After
   stopping, the script explicitly submits an analysis-only `rbnx ask` to
   collect refreshed evidence; the fake model returns only an empty sequence
   outside the admission check below.
2. **Paused Pilot.** Verify the fixture's Pilot PID against its boot state,
   parent, executable, boot identity, and process start time, then send
   `SIGSTOP` through a pinned pidfd. Submit another task to the same Explore
   capability directly through Executor. Mark only the fixture's lidar
   instance terminated before removing its channels, then inject another health
   fault after graph refresh. The target must stop within the same 8-second
   limit while Pilot remains paused and software work continues. A `finally`
   block sends `SIGCONT`. A new fixture-owned analysis-only `rbnx ask` then
   obtains Pilot's existing impact report, which must retain Mapping's
   `evidence: "last_known"` and lidar dependency path. This does not depend
   on the first ask or its session remaining active.
3. **Reject unsafe model actions.** Keep the fault present after Pilot resumes.
   Enable the fake model's admission flag and start a fresh fixture-owned ask.
   The model returns the same affected Explore operation twice, including the
   corrective retry, and emits no cancellation. The ask must finish within
   15 seconds with `Vitals blocked this plan` in its final output and no Plan
   event. The script continuously checks that no Explore operation enters
   Executor's active table, then checks the appended Executor log for any
   Explore dispatch and the Vitals log for cancellation of a newly admitted
   plan. These checks reject execution followed by fast cancellation. The
   model allows at most two unsafe responses and returns empty sequences
   afterward, so the fixture cannot create an unbounded unsafe retry loop.

Each recovery must report lidar healthy and leave stopped hardware work
inactive for a further five seconds while software work continues. The sensor
fault helper has no WARN profile, so this demo does not inject WARN; unit tests
cover the WARN/UNKNOWN cases.

The terminal prints stage markers and cancellation latency. The temporary
artifact directory contains `boot.log`, `ask.log`, and `verification.json`,
including ordered fault/stop/model-release timestamps, model requests, and
the original impact reports. Admission evidence includes both unsafe model
replies, active-table samples, and the ask/Executor/Vitals log increments;
the full ask output remains in `ask.log`. A failed run also writes its error and any
evidence collected before cleanup. The assertions require local plan completion
and Vitals' confirmed control result, which includes terminal async run status.
They do not observe physical stopping; prompt evidence is checked separately.
An 8-second timeout is
a failed demonstration, not a successful cancellation request.

Cleanup cancels only fixture-owned plans, reaps every ask process, resumes its
Pilot, shuts down the temporary deployment, and removes its unique fault file. Webots stays running.
This is simulated health injection, not a physical sensor failure.

To tear everything down: Ctrl-C the `rbnx boot` terminal, OR from
any other shell:

```bash
cd examples/webots && rbnx shutdown    # SIGTERM each component's PGID
bash sim/stop.sh                       # then stop the Webots container
```

`rbnx shutdown` reads `rbnx-boot/state.json` (boot writes it
incrementally as components come up) and tears them down in
reverse order. Each chassis / camera / lidar / nav2 package's
`scripts/start.sh` installs a `trap` that pkills the in-container
python on EXIT/INT/TERM — so the docker-exec'd drivers don't
strand the next bring-up by holding ports 50111-50113 / 50211-50213.

## Env vars

Pre-export before `rbnx boot` (or set in shell rc):

```bash
export VLM_BASE_URL=https://api.openai.com/v1   # or your OpenAI-compatible endpoint
export VLM_API_KEY=sk-...
export VLM_MODEL=gpt-5.5
```

The deploy manifest references these via `${VLM_*}`.

## What `rbnx boot` does

1. Reads `robonix_manifest.yaml`, brings up the `system:` block (including
   Vitals) and the implicitly required Soma process using their installed
   binaries. Listen addresses, log levels, and VLM settings come from the
   manifest.
2. For each `primitive:` / `service:` entry, in declaration order:
   - Spawns the package via `rbnx start -p <path>` (which runs that
     package's `scripts/start.sh` — for tiago drivers that's a
     `docker exec` into the sim container that runs the Python driver).
   - Polls atlas until the package registers and declares its lifecycle
     Driver. Every provider must expose exactly one Driver; a missing or
     ambiguous one fails startup.
   - Calls `Driver(CMD_INIT, config_json)`, then `CMD_ACTIVATE` for
     primitives and services. Skills stop at `INACTIVE`; Executor activates
     them on their first call.
3. Sits on Ctrl-C / SIGTERM, then tears down all children.

## How the LLM picks tools

After `rbnx boot` is up, Pilot's prompt lists each provider that
registered a `CAPABILITY.md`, by provider id and one-line description; the
LLM reads a manual on demand with the `read_capability_doc` builtin, passing
the provider id. Atlas serves the content, so no file path is involved.

Capabilities are exposed to the LLM as `<provider_id>.<area>_<leaf>`
(camera and lidar both have a `snapshot` leaf — the LLM sees
`tiago_camera.camera_snapshot` and `tiago_lidar.lidar_snapshot`). The MCP
server inside each driver still registers tools by leaf name.

Pilot replans after each tree's results arrive. The model reports progress
in `task_update`: `in_progress` while steps remain, `done` once its
`success_criterion` holds.
