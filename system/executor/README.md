# robonix-executor

Executes Pilot's RTDL plans: validates input, schedules `sequence` / `parallel`
nodes, and runs each `do` capability call. External calls resolve through Atlas
and use MCP; builtin capabilities run in-process.

## Build and run

From the repository root, with Atlas running:

```sh
cargo build -p robonix-executor
./target/debug/robonix-executor --atlas 127.0.0.1:50051 --listen 127.0.0.1:50061
```

| Flag | Environment variable | Default / purpose |
|---|---|---|
| `--atlas` | `ROBONIX_ATLAS_ENDPOINT` | Atlas at `127.0.0.1:50051` |
| `--listen` | `ROBONIX_EXECUTOR_LISTEN` | gRPC on `127.0.0.1:50061` |
| `--id` | `ROBONIX_EXECUTOR_PROVIDER_ID` | Provider id `executor` |
| `--config` | `ROBONIX_CONFIG_PATH` | Optional YAML: `atlas_endpoint`, `listen`, `id`, `verification` |
| `--config-json` | — | `system.executor` manifest block passed by `rbnx` |

Connection settings use CLI > flag environment variable > YAML > default.
`ROBONIX_ATLAS` is an additional fallback before YAML. Manifest verification
settings override YAML verification settings.

Logging uses Scribe: `system.executor.log` (via `--config-json`) overrides
`SCRIBE_FILE_LEVEL` (default `info`); `SCRIBE_CONSOLE_LEVEL` defaults to `warn`.
The standalone `--log` flag is accepted but currently not applied.

## Execution and control

All gRPC contracts are under `robonix/system/executor/`:

| Contract | Purpose |
|---|---|
| `execute` | Submit a complete `Plan`; receive an event stream |
| `execute_stream` | Submit a plan incrementally; receive the same events |
| `control_plan` | `cancel` one `plan_id` or `cancel_all`; `wait_ms=0` uses 5000 ms |
| `list_active_plans` | Query active plans and node states |
| `cancel_all_plans` | Compatibility interface for canceling all plans |
| `get_health` | Report Executor health |

Both execution RPCs share one scheduler:

- `sequence` runs children in order and skips later children after a failure;
  `parallel` runs branches concurrently without canceling siblings on failure.
- Input requires a nonempty plan ID, unique op/call IDs, nonempty node
  descriptions, object-valued call arguments, at most 256 nodes and 32 levels.
  Complete plans must form one tree; active plan IDs cannot be reused.
- Events are `plan_started`, `node_state`, and `plan_complete`. Node events
  retain node identity; leaf results carry the capability output. Parallel
  results arrive in completion order. Failures and cancellation set `any_failed`.
- Cancellation seals streaming input, skips later work, and interrupts hung
  synchronous dispatch and async status polling. Dropping a synchronous request
  only confirms local cancellation; it cannot confirm a physical stop.
- Async cancellation sends the exact owned `run_id`, then polls its status.
  `accepted=true` alone is insufficient. Discovery, cancel, and confirmation
  share a two-second deadline; failed handles remain available for a retry.
  `success=true, completed=false` means cancellation is pending. A remote
  refusal, including an MCP error, still permits status confirmation within
  that same deadline: an already terminal run needs no further cancellation.
  Without valid terminal evidence, the handle and cancellation/status errors
  remain available and return `success=false, completed=false` once known.
- Local completion or input disconnection does not remove unconfirmed async
  work. The canceled plan stays in `list_active_plans`, with its owned calls
  visible as `running`, so health dependency matching and scoped retries keep
  working. Its plan ID cannot be reused until it drains. At most 256 plans are
  retained; admission fails at the limit instead of evicting unresolved handles.

Incremental input follows `START → NODE / CLOSE → END`. Execution can begin
before `END`; operators close innermost first. Invalid input returns
`invalid_argument`; capability failures use node events. Input loss before
`END` or response disconnection triggers cancellation and cleanup.
See the [stream protocol](../../capabilities/lib/executor/msg/ExecuteStreamInput.msg),
[event IDL](../../capabilities/lib/executor/msg/RtdlEvent.msg), and
[contracts](../../capabilities/system/executor/) for field definitions.

## Async calls and verification

Async MCP capabilities register both `<contract_id>/status` and
`<contract_id>/cancel`; registering only one is an error. Executor polls status
every 2 seconds until `SUCCEEDED`, `FAILED`, `CANCELED`, or `TIMEOUT`.
Status responses provide `state` and optional `detail`. The initial response
must identify the execution with a nonempty `run_id`; status/cancel calls never
fall back to a provider's current or latest run. `known=false`, a different
returned `run_id`, and malformed status cannot confirm completion. A missing
initial ID retains unresolved ownership and requires operator intervention.
An initial request already in flight remains owned until its response arrives,
even after cancellation, so its late run ID can be canceled accurately.
Calls without either sub-contract complete when their initial response returns.

Executor serves the shared `robonix/lifecycle/driver` contract and initializes
and activates it through the same gRPC endpoint before reporting readiness.
`CMD_SHUTDOWN` first closes admission to both execution RPCs, then drains tracked
plans. A failed drain returns `ok=false`, keeps the server available for
cancellation retries, and does not publish `TERMINATED`. New execution remains
rejected. Only a confirmed drain allows lifecycle shutdown.

The deployment supervisor currently terminates built-in system processes with
signals rather than calling their Driver. That path does not invoke this
cancellation-confirmation callback; use the Driver RPC for a confirmed shutdown.

Verification is optional. Configure it in the deployment manifest:

```yaml
system:
  executor:
    verification:
      overlap: false
      rules:
        - target_contract_id: robonix/service/navigation/navigate
          target_provider_id: simple_nav  # optional
          verifier_provider_id: scene_verifier
          verifier_args:
            scene_provider_id: scene
```

Provider-specific rules override contract-only rules; duplicate rules are
rejected. `verifier_args` must be an object and is forwarded unchanged.
Only successful calls with a matching rule invoke `robonix/service/verifier/verify`.
A failed verdict, unavailable verifier, malformed response, or 60-second timeout
makes the node fail.

- `overlap: false` (default): wait for verification before completing the node.
- `overlap: true`: emit `VERIFYING` and continue execution; wait for verification
  before final node/operator states and `plan_complete`. Verification failures
  do not stop later siblings. Canceling a plan does not cancel a leaf already
  in `VERIFYING` or overwrite its verification result.

## Builtin capabilities

Under `robonix/system/executor/builtin/`:

- `read_file`, `write_file`, `patch_file`, `list_dir`, `run_command`: file and
  shell operations rooted at `ROBONIX_WORKSPACE` (default: current directory).
- `read_capability_doc`: read a registered provider's `CAPABILITY.md` followed
  by the full descriptions of its capabilities. Args: `provider_id` (required).
  Providers without a `CAPABILITY.md` still return their capability descriptions.
- `cancel_plan`, `get_all_plans`, `get_plan_status`: compatibility plan controls;
  Pilot uses `control_plan` and `list_active_plans` directly.

## Source map

| File | Responsibility |
|---|---|
| [main.rs](src/main.rs), [config.rs](src/config.rs) | Startup, registration, configuration |
| [grpc_server.rs](src/grpc_server.rs) | Execution, control, query, and health RPCs |
| [plan_input.rs](src/plan_input.rs) | Plan validation and incremental construction |
| [plan_scheduler.rs](src/plan_scheduler.rs) | Scheduling and cleanup |
| [capability_execution.rs](src/capability_execution.rs) | One `do` call's execution and verification flow |
| [active_plans.rs](src/active_plans.rs) | Active state, queries, and cancellation |
| [rtdl_events.rs](src/rtdl_events.rs), [pb.rs](src/pb.rs) | Event helpers and generated protobuf modules |
| [verification.rs](src/verification.rs), [dispatch/](src/dispatch/) | Result verification and capability dispatch |

## Validation

```sh
cargo test -p robonix-executor --all-targets
cargo test -p robonix-executor cancellation_tests
cargo build -p robonix-atlas -p robonix-executor -p robonix-pilot
python3 scripts/test_execute_stream.py
```

The cross-process test uses a local fake VLM, requires `grpcio` and
`grpcio-tools`, and retains logs under `/tmp`.
The cancellation tests run real Executor gRPC, Atlas, and HTTP MCP services.
They cover cancellation before `END`, late input, late initial responses,
unrelated plans, hung calls, retained failures, retries, and lifecycle shutdown.
