# robonix-pilot

`robonix-pilot` is the VLM-driven reasoning and planning service for Robonix. It receives `SystemPilot` tasks, builds the model prompt from the current Atlas capability catalog, asks the VLM for an RTDL plan, expands that RTDL into a `Plan`, and sends the plan to `robonix-executor`.

## Build

From the repo root:

```sh
cargo build -p robonix-pilot
```

The crate is part of the top-level Cargo workspace and is built / installed by `make build` and `make install` respectively.

Run Pilot's tests with `cargo test -p robonix-pilot`. RTDL parser tests exercise the incremental parser used at runtime.

## Run

Manual launch:

```sh
robonix-pilot \
  --vlm-upstream https://api.openai.com/v1 \
  --vlm-api-key sk-... \
  --vlm-model gpt-5.5
```

Common configuration:

- `--atlas` / `ROBONIX_ATLAS_ENDPOINT`: Atlas endpoint. Defaults to `127.0.0.1:50051`.
- `--listen` / `ROBONIX_PILOT_LISTEN`: Pilot gRPC listen address. Defaults to `127.0.0.1:50071`.
- `--id` / `ROBONIX_PILOT_PROVIDER_ID`: Pilot provider id registered with Atlas. Defaults to `pilot`.
- `--vlm-upstream` / `ROBONIX_VLM_UPSTREAM`: OpenAI-compatible API base URL.
- `--vlm-api-key` / `ROBONIX_VLM_API_KEY`: VLM API key.
- `--vlm-model` / `ROBONIX_VLM_MODEL`: VLM model name.
- `--vlm-context-window-tokens` / `ROBONIX_VLM_CONTEXT_WINDOW_TOKENS`: optional operator override for the deployed model's total context capacity.
- `--vlm-format` / `ROBONIX_VLM_FORMAT`: API dialect. Only `openai` is currently supported.
- `--config` / `ROBONIX_CONFIG_PATH`: optional YAML config file.
- `log`: Scribe log-file level (`debug`/`info`/`warn`/`error`), read from the `log` key of `--config-json`, which `rbnx` fills from the manifest. Falls back to `SCRIBE_FILE_LEVEL`, then `info`. A standalone `--log` flag is accepted but not applied.
- `ROBONIX_PILOT_SOUL`: optional path to a SOUL markdown file. If unset, Pilot tries `~/.robonix/SOUL.md`.
- `ROBONIX_PILOT_MAX_TOOL_ROUNDS`: maximum RTDL execution rounds per turn. Defaults to `64`.
- `ROBONIX_PILOT_VLM_IDLE_TIMEOUT_SECS`: maximum time without a VLM stream event before ending the turn and canceling its submitted plan. Defaults to `30` seconds and is clamped to `5..=300`. Non-empty `reasoning_content` deltas refresh this timer but are not appended to the assistant JSON.
- `ROBONIX_SESSION_DIR`: where session transcripts are kept (see Session transcripts). `rbnx boot` sets it; defaults to `./sessions`.

## Context capacity: automatic, visible, and overrideable

Pilot compacts history before the next request would exceed the model's total context capacity. This is not the same thing as `max_tokens`: it is the full input + output budget.

History's room is the context window minus this round's non-history context (the system prefix and the current runtime sections) and a fixed 6,144-token reserve for the next reply and estimation error. When history outgrows its room, one compaction leaves behind at most:

| Kept | Share of the room | Form |
| --- | --- | --- |
| The most recent messages | 50% | verbatim; no single message over 20% of the room |
| The current task's own words, if they fell out of that tail | 10% | verbatim |
| Everything older, including the previous summary | 15%, and at most 6,000 tokens | one rolling summary written by the model |

Together that is at most 75% of the room, so every compaction frees at least a quarter of it and the next one cannot follow immediately. If the summarizer fails, the older messages are still dropped behind a note saying so, and the previous summary is kept; compaction never leaves history as large as it found it. After a compaction the next round re-sends the full capability catalog, Soma body, and doc index, because the summary may have absorbed the earlier copies. Each compaction writes a `[pilot/compaction]` record with the room, the caps, and the history size before and after.

Pilot resolves the capacity in this order:

1. A manual deployment value (`vlm.context_window_tokens`, the environment variable, or the CLI flag). This is authoritative.
2. Provider metadata from `GET /models/{id}`.
3. Provider metadata from `GET /models`: first the exact id, then a unique canonical terminal name. This handles a gateway advertising `anthropic/claude-opus-5.5` when the deployment configured `claude-opus-5.5`. Two different providers with the same terminal name are intentionally treated as ambiguous, never guessed.
4. Otherwise Pilot starts without pre-emptive history compaction. A manual declaration enables predictable compaction for an unknown/private model.

The startup `[pilot/context_budget]` JSON record includes the configured model, the selected provider id when a canonical match was used, capacity, and source. Canonical matches emit a warning asking the operator to verify the gateway route. At `rbnx boot`, `context=... (manual)` means an operator override; otherwise `context=auto` means Pilot will resolve and log the real source after startup.

Set a manual value only when the route is private, metadata is missing or ambiguous, or the gateway has a smaller server cap:

```yaml
system:
  pilot:
    vlm:
      upstream: https://provider.example/v1
      api_key: ${VLM_API_KEY}
      model: local-vision-planner
      context_window_tokens: 32768 # manual: inspect the model card/server
```

Use the smaller effective limit when a model card and an inference-server setting (such as `max_model_len` or `max_seq_len`) disagree.

## Prompt assets

On every planning round the model sees, in this order:

- the standing system prompt, built by `build_system_prompt` in `src/planner.rs`;
- `RTDL_PROTOCOL_REMINDER` in `src/planner.rs`, the same text on every round so the prompt prefix stays cacheable;
- the session history, which only grows (see Tasks below);
- the live sections of the round (catalog changes, in-flight trees, Executor snapshot, embodiment and environment);
- the task status board, the request's tail, rebuilt every round and never written into history;
- the response JSON schema, built by `rtdl_envelope_schema` in `src/vlm.rs`, sent as the request's `response_format`.

`assemble_planning_messages` in `src/prompt.rs` puts them in order; `src/prompt.rs` also does usage accounting, and each round's section list supplies both context-budget estimates and request rendering. There is no other prompt text: no protocol file is sent.

## Tasks

The user's messages arrive one at a time. A task is one piece of work the user asked for: the user's messages about it (the request, then every correction, addition or answer), the goal and success criterion the model has drawn from them, its status (`in_progress`, `done`, `failed`, `superseded`), and the RTDL trees dispatched for it. Several tasks can be open at once; every tree belongs to exactly one task. `src/tasks.rs` keeps them per session, and the transcript keeps them across a restart.

Pilot owns identity and facts: message ids (`m1`, `m2`, …), task ids (`T1`, `T2`, …), the user's words, which task each message and tree is filed under, and how each tree ended. A message enters history once, as `User message m7 (filed under T2): …`, and a `Pilot task record` line is appended when a task changes. Compaction keeps the open tasks' messages before anything else, newest first when they do not all fit. The model owns the judgement, through three reply fields:

| Field | Meaning |
| --- | --- |
| `message_updates` | Refiles new messages: `[{"message_id":"m7","parent_task_id":"new"}]` starts a task, `[{"message_id":"m7","parent_task_id":"T1"}]` moves the message to another open task. Without it a new message stays under the newest open task. Only messages that arrived since the model's last reply can be refiled. |
| `rtdl_parent_task_id` | The task the reply's tree belongs to: an open task id, `"new"` for the task the reply creates, or `null` when it dispatches no tree. A tree with no usable `rtdl_parent_task_id` is filed under the newest open task and the model is told so. A tree holds one task's steps only: when several tasks need steps at once, the model dispatches one task's tree, and Pilot asks again at once while another open task has no tree running. |
| `task_updates` | `[{"task_id":"T1","goal":"…","success_criterion":"…","status":"…","status_reason":"…"}]`, each field but `task_id` optional. A task cannot close while one of its trees runs, or in the reply that dispatches its tree; an item naming an unknown or closed task is rejected. Rejected items are fed back to the model and the others still apply. |

Every `"new"` in one reply names the same task: the one its `message_updates` start, which Pilot creates and gives the next id. A reply therefore creates at most one task, and `rtdl_parent_task_id` or `task_updates` naming `"new"` without a message filed under it is rejected.

A reply gives its fields in the order `content`, `message_updates`, `rtdl_parent_task_id`, `rtdl_description`, `rtdl`, `task_updates`: what dispatch needs comes before `rtdl`, so a streaming planner knows a tree's task when the tree starts, and what needs to know what was dispatched comes after it. Pilot applies them in that order. Pilot's `serde_json` has `preserve_order` so that the schema's properties reach the provider in that order.

Streaming waits for complete `message_updates` and `rtdl_parent_task_id` before releasing nodes. Pilot files those messages and attaches the submitted tree once, then applies the trailing `task_updates`; `"new"` still refers to the same task. Canceled or invalid response prefixes stay in the session's forest so their results and task ownership survive the turn.

A turn waits while a tree of an open task runs and replans when a tree ends; it ends when every task is closed, or when an open task waits for the user. Trees belong to the session, not to the turn: a tree still running when a turn ends keeps reporting, the next turn takes in what it reported, and a stop with no active turn cancels it.

Pilot preserves each `do.bind` name when expanding RTDL into a Plan. Executor stores the successful result in that tree's execution context; later arguments and conditions can reference it with `{"var":"name","path":["field"]}`.

Pilot rejects identical capability calls while their earlier nodes are unfinished, including during verification. A terminal node releases its call for recovery before its parent tree completes; identical unfinished siblings remain protected.

Pilot's standing prompt is deployment-independent. It does not hard-code Scene, memory, chassis, or another provider family's semantics; those come from the current Atlas capability catalog and the provider documentation loaded through `read_capability_doc` when needed. A provider appearing, disappearing, or reconnecting therefore does not rewrite the standing system prefix.

Atlas discovery still runs before every planning round. Pilot omits contracts whose `[contract]` metadata sets `llm_callable = false` from the model-facing catalog and target map, while leaving them available to Executor and other Atlas consumers. An omitted field, including metadata from an older Atlas, defaults to visible. When the remaining provider, contract, description, and schema fields are unchanged, Pilot reuses the rendered capability catalog. The startup-cached Soma YAML is serialized as compact JSON while the authoritative URDF remains unchanged.

A plan names a capability by the exact catalog entry `<provider_id>.<llm_name>`. Contract ids remain runtime identifiers for Atlas, contracts, and internal configuration; they are not a second model-facing spelling. Pilot omits a capability from both the catalog and target map when its contract metadata sets `llm_callable = false`.

A planning request is split so that its prefix is cacheable. The system message carries only what never changes, Pilot rules and the RTDL contract, before the ordered conversation history. Everything that can change lives in history. Memory and an explicit response mode are appended once for each submitted task; every task records either voice or text mode, so a later text request supersedes voice-only constraints without rewriting history. The callable capability catalog, the Soma body data, and the provider-doc index are re-read before every planning round. The first round of each task, and the first round after history compaction, appends each of them in full; later rounds append only what changed, and for the catalog that is a list of the capabilities added, changed, or removed. A snapshot counts as shown only once its round is written to history, so a round dropped for a steer or a stale plan does not leave the model without it. A provider registering or leaving therefore never invalidates the cached prefix. Live embodiment health, in-flight trees, Executor state, and environment state are appended on every round (the environment state is Scene's robot context; the ids and names of the map's regions and points of interest, which is `list_regions` without its geometry, each with the navigate-ready goal that `goal_region` answered for it where one is reachable (asked once per map, in the background, so the first rounds after a new map may show fewer); and the ids and labels of the objects Scene knows, by region, which is `find` for each region. A plan can then name a destination, or tell whether an object is known, without first spending a round on those queries. The regions and their goals are shown like the catalog, in full on a task's first round and again only when they change, with numbers to the millimetre; the robot context and the objects are shown every round; a task whose `context_json` sets `"state_view": false` gets Scene's robot context alone, as before these were added), followed by the assistant reply and any task-state/result records. The next request therefore extends rather than reorders the prior message sequence. Prompt logs include both the cacheable-prefix byte length and fingerprint, making accidental prefix churn observable.

Both Soma blocks are projected onto what a planner acts on. From the body description that drops the `urdf` file reference — a path in the provider's own filesystem namespace, pointing at the kinematics this crate already declines to inject — the `footprint` collision polygon that navigation rather than the planner consumes, and the description of each exported capability, which the capability catalog in the same prompt already carries verbatim; the component-to-capability mapping stays, since the catalog does not say which part of the body offers a capability. From the live health snapshot it drops fields at an absent value, which the prompt's own "missing means unknown" rule already covers. `dimensions`, sensor `placement`, `can_do` / `cannot_do`, and the deployment notes are body facts and remain.

The catalog prints each capability's name, the opening paragraph of its description, and its argument schema. A description longer than that paragraph — a provider that wrote its request/response manual into the description field rather than a `CAPABILITY.md` — is summarized, and the entry carries a `more:` line naming the `provider_id` to pass to the Executor's `read_capability_doc` builtin for the full text. The catalog stays in history, with later rounds appending only changes. A manual is fetched when the model requests it.

Every request writes a `[pilot/prompt]` JSON log with text bytes and four-byte token estimates for standing context, RTDL protocol, current runtime-context sections (including the capability catalog record, when there is one), and full ordered history. Pilot supplies one opaque `prompt_cache_key` for the **lifetime of the running Pilot deployment**, rather than one key per task: the unchanged system prefix and completed histories can route to the same cache across independent tasks without using a user/session identifier. The message-ordering test asserts that a later normal planning request is an exact extension of the prior wire-message sequence; this is the code-level reusable-prefix invariant. Compatible providers also report exact input, output, total, cached-input, and uncached-input tokens through streaming usage. Pilot logs those values per request together with `cache_hit` and `cache_hit_ratio`, plus cumulative token totals, cache-hit request counts, and the cumulative cache-hit ratio for the interaction. Provider-reported cache ratio remains a runtime measurement: routing, TTL expiry, provider cache granularity, and a newly appended suffix make it distinct from code-level prefix reuse. Pricing is not hard-coded because an OpenAI-compatible proxy may route the same model name to differently priced backends; the logged totals can be priced against the operator's actual provider contract. The fake VLM's reported usage is explicitly a deterministic four-byte estimate, not a production tokenizer result. A provider that rejects a named optional cache/usage field or a `response_format` type with HTTP 400 or 422 is retried once without it, and later requests from the same Pilot process leave it out up front instead of paying the rejected round trip every planning round; unrelated client errors are returned unchanged.

The `info` records above carry sizes and token counts, never prompt text. To see exactly what the model receives and returns, raise Pilot's log level to `debug`: set `log: debug` on the Pilot component in the deployment manifest, or `SCRIBE_FILE_LEVEL=debug` for the log file and `SCRIBE_CONSOLE_LEVEL=debug` for the terminal. Each planning request then also writes `[pilot/prompt/messages]`, the full ordered message sequence sent to the provider with inline images reduced to their byte size, and each reply writes `[pilot/rtdl/raw]`, the model output as received before parsing. These records contain user tasks and scene contents verbatim, so enable them only while debugging.

## Session transcripts

Pilot keeps every session's conversation in an append-only JSON-lines file, `<session dir>/<session_id>/transcript.jsonl`. The session dir is `$ROBONIX_SESSION_DIR`, which `rbnx boot` sets to `<deploy>/rbnx-boot/sessions`, or `./sessions` when unset; each session has its own directory so records about it can live beside the transcript. Session state is not a log: boot clears old logs but never this directory, and `rbnx clean` keeps it unless given `--sessions`. History compaction removes old messages from what the model sees; the transcript keeps all of them, so no interaction is lost. Each line has `ts` and `kind`: `session` (the first line, with the session id and Pilot version), `message` (one history message; an image is recorded by size only, as `image_bytes`), `task_state` (the task state after it changed), and `compaction` (the full history that replaced the old one, with how many messages were evicted and pinned).

When Pilot receives a task for a session it has no history for, such as after a restart, it rebuilds that session from the transcript: the last `compaction` record's history, or an empty one, followed by every later `message`, plus the last `task_state`. Images are not restored. A line that does not parse, such as one cut short by a crash, is skipped with a warning. A failed write is logged, its records stay queued for the next write, and it never stops the turn. There is no retention policy yet; delete old sessions by hand or with `rbnx clean --sessions`.

## RTDL Planning Flow

Pilot retries opening a VLM stream once if it fails or times out. `ROBONIX_PILOT_VLM_IDLE_TIMEOUT_SECS` bounds each opening attempt and the gap between received stream events (default 30 seconds, clamped to 5–300). Once the stream opens, a timeout cancels the unfinished plan and retains its results without replaying the request.

Pilot no longer sends OpenAI `tools` / function schemas as the primary planning path. Instead, it writes the RTDL grammar and available capability list into the prompt. The model must return a single JSON object: `content` (text shown to the user), `message_updates`, `rtdl_parent_task_id`, `rtdl_description` (a short label for the tree), `rtdl` (the tree) and `task_updates`; the task fields are described under Tasks above:

```json
{
  "content": "I will inspect the current scene.",
  "message_updates": [],
  "rtdl_parent_task_id": "T1",
  "rtdl_description": "inspect the scene",
  "rtdl": {
    "op": "sequence",
    "children": [
      {
        "op": "do",
        "cap": "tiago_camera.camera_snapshot",
        "args": {}
      }
    ]
  },
  "task_updates": []
}
```

RTDL supports:

- `sequence`: ordered children.
- `parallel`: concurrent children. Executor waits for every child and does not cancel sibling branches when one fails.
- `do`: one capability call, where `cap` is the `capability_name` shown in the prompt, `<provider_id>.<area>_<name>` (for example `tiago_camera.camera_snapshot` for `robonix/primitive/camera/snapshot`), and `args` is a JSON object.
- `if`, `repeat`, `while`: branches and loops on values, with `do.bind` and `{"var": ...}` references (see [RTDL conditions and loops](#rtdl-conditions-and-loops)). A branch on what a step returned is an `if`.
- `fallback`: children tried in order until one succeeds; the rest are skipped. It fails only when every child fails, so its later children say what to do when the earlier ones fail.

The per-round protocol also tells the model that a tree which only reads costs a round, so each read goes into the tree that acts on it, with each course under an `if`. A task whose `context_json` sets `"fold_reads": false` leaves this out, so its effect can be measured.

Pilot parses RTDL incrementally and submits complete nodes to Executor through `execute_stream`, so execution can begin before the model finishes its response. User-visible text waits for the complete response to validate. Invalid model output, including malformed or truncated JSON, unknown capabilities, and invalid RTDL, triggers up to two correction requests per planning round. Pilot first cancels this attempt's submitted plan and records observed execution results, then sends the error and a short cancellation notice back to the model through the existing planning loop. Each correction refreshes runtime context and counts toward `ROBONIX_PILOT_MAX_TOOL_ROUNDS`.

Pilot forwards `VERIFYING` node states for live visibility but does not put them in LLM history or `BatchResult`. Once verification finishes, the final leaf result is written to history exactly once; a final failure triggers replanning immediately, unless it is under a `fallback`, which handles it. A `fallback` whose every child failed triggers replanning like any other failure. `BatchResult` contains only the latest final state for each node.

The dispatch record in planning history lists RTDL node definitions, including
`do` nodes under a `repeat` whose count is zero. It is not evidence that those
capabilities ran. Pilot records each successful `repeat` operator's completed
iteration count separately; use leaf feedback to identify calls that ran.

A failed leaf requests recovery once: its parent failures and the tree's completion do not request the same recovery again. If a leaf fails while the model is still generating or submitting nodes, Pilot cancels that unfinished attempt, retains its execution results, and replans from the failure feedback. A later successful sibling still requests a planning round at completion, and requested cancellation completion still triggers its follow-up.

Parallel example:

```json
{
  "content": "I will inspect both signals.",
  "message_updates": [],
  "rtdl_parent_task_id": "T1",
  "rtdl_description": "inspect camera and health",
  "rtdl": {
    "op": "parallel",
    "children": [
      { "op": "do", "cap": "tiago_camera.camera_snapshot", "args": {} },
      { "op": "do", "cap": "tiago_health.health_state", "args": {} }
    ]
  },
  "task_updates": []
}
```

An empty sequence means the model is dispatching no new tree this round:

```json
{ "content": "Done.", "message_updates": [], "rtdl_parent_task_id": null, "rtdl_description": "", "rtdl": { "op": "sequence", "children": [] }, "task_updates": [] }
```

Pilot accepts it only when it does not contradict the task state. When a task
update keeps a task `in_progress` and nothing runs — neither this session's trees
nor any plan in Executor's active-plan snapshot — the model claims the task is
advancing while nothing will run, so the reply is rejected
and the model gets a corrective round; if it repeats,
the turn ends with what the model said. The per-round reminder states the
pairing rule: return an empty sequence with `"task_updates": []` if no task
changes, such as while waiting for a running tree. To close a task, include
its ID and status, for example `"task_updates": [{"task_id":"T1","status":"done"}]`.

Pilot uses the specific node error as model feedback when RTDL argument or
condition evaluation fails or a while loop reaches its limit with the
condition still true. It can start recovery while another branch is running.

### Speculative planning

A task whose `context_json` sets `"speculate": true` (`rbnx ask --context '{"speculate":true}'`) lets Pilot prepare the next reply while a tree runs. When a round dispatches a tree that is then the only one in flight, Pilot sends up to four background requests, one per outcome the tree can report: it succeeded, or step k of a root `do` or an all-`do` sequence failed. A step's failure is prepared for only if its capability fails often enough to be worth a request: Pilot counts each capability's calls and failures as it runs, estimates the failure rate with one failure and one success assumed beforehand (so a capability not yet seen counts as failing half the time), and prepares the failures of steps at 10% or more, most likely first, up to three. The `start` record lists them as `failure_rates`. A task whose `context_json` sets `"branch_budget": false` prepares for the first three steps' failures in order, whatever their rates. Each request is the next round's history with a user message stating that outcome as an assumption; the outputs are unknown, so the model refers to them as `{"var": "<op_id>", "path": [...]}` and decides on them with `if`, or replies `NEEDS_RESULTS`.

When the tree's report starts the next round, Pilot uses the prepared reply instead of a new request only if the tree ended with the outcome the reply assumed, no user message arrived, history was not compacted, the capability catalog, Soma data and provider docs did not change, no other tree runs, and the reply parses. Nor is it used if it searches Scene (`find`) for a label that two of the turn's searches already found nothing for (`searches again for what came back empty twice`): written before the results, such a reply can only look elsewhere, while whether to look further or stop rests on what the robot has seen, images included, which only the real request is shown. A failed step starts the round before its tree's completion arrives, so Pilot waits up to 2 s for it. If the prepared reply is not settled once the round has gathered its context (still being written, or waiting on the decision model below), Pilot sends the real request alongside it and uses whichever comes first: the prepared reply once it is settled, or the real request once its reply text begins, at which point the prepared reply is discarded (`request answered first`). A prepared reply therefore never delays the round. The prepared reply then goes through the normal parse and submit path, and a `{"var": "<op_id>", "path": [...]}` naming a finished step, which the prepared-reply request asks for where the next step depends on that step's output, is filled in with that part of its output: Executor's variables live in one plan, so a reference to an earlier tree is Pilot's to resolve. Nothing speculative is dispatched before the real outcome is known. Each speculation logs `[pilot/segue]` records: `start`, `ready` per request (latency and tokens), and `commit` or `discard` with the reason.

A prepared reply is written from the state of the round that dispatched its tree, and that state can change while the tree runs. A reply that names an object by its Scene id relied on that object being where the state view placed it. Before using a reply, Pilot compares those objects with the round's fresh state view. If one has moved to another region or is no longer among Scene's objects, Pilot cuts the reply's tree, when its root is a sequence, before the first top-level node that names one. If what is left calls something other than a `read_only` capability, Pilot dispatches that part and adds a note to history saying what changed and that the rest did not run. Otherwise it discards the reply (`read set changed: …`). Objects the reply does not name do not count, and neither do objects that appeared since, except for a reply that searches Scene (`find`): it was planned without them, and with them listed it may look elsewhere for what is now known, so Pilot does not use it when the state view lists an object (by label and region, since perception can give an object it sees again a new id) that it did not when the reply was written and that fits what the task looks for. An object whose label answers a search the reply makes, as Scene's find matches them, fits at once; otherwise the decision model is asked whether any newly listed object plainly fits the task (a `decide` record), and the reply is set aside at p(yes) 0.3 or more, since setting one aside costs a planning request while searching elsewhere for what is already listed costs a trip. Without a decision model, any newly listed object sets it aside. A long tree's careful situation is also asked to act first on an object the environment state already lists that fits the task. Each such check logs a `read_set` record with the objects that moved, the objects newly listed, and how many nodes were kept. A task whose `context_json` sets `"read_set": false` only logs the record and uses the reply as written. Without the state view, the check is not made.

When to prepare follows from how long things take, which Pilot learns as it runs: about how long a prepared reply takes to begin and to finish, and how long each capability's calls take. The latter is Pilot's experience, kept in `experience.json` under the session directory (`$ROBONIX_SESSION_DIR`, else `./sessions`) so it outlives a restart. It holds each capability's recent successful durations and the recent navigations with the straight-line distance each covered, measured from where the robot was (the state view's robot context at the start of a round, or where the last navigation stopped, which nav2 reports as `last_pose`). A tree is expected to take the sum of its steps: a navigation to literal coordinates from a known start is timed by a line fitted to the navigations' distances, and any other step by its capability's median duration. A task whose `context_json` sets `"experience": false` times a tree by each capability's longest successful call in this process instead, as Pilot did before it kept experience. A tree expected to end before a prepared reply could answer sooner than the next round's own request is not prepared for while it runs (a `skip` record); a capability not yet seen to finish counts as long. A step that has succeeded has the reply prepared for its failure dropped.

Pilot prepares along the success path up to three trees ahead, or N with `"lookahead": N` in `context_json` (1 prepares only for the running tree). When the success reply is written and a tree it would dispatch is expected to end before a reply could be written while it runs, Pilot at once asks for the reply after that tree too, in the same conversation: the assumption, the reply just written, and a second assumption that this tree also succeeded, with its steps under op_ids made up for the request. Each such tree logs an `ahead` record with what it is expected to take and whether its reply was asked for. At most four background requests run at once, so a request prepared ahead waits for a branch to finish or be dropped. If the first reply is used and dispatches exactly the tree the second was written for, the second becomes that tree's speculation (a `start` record with `ahead_ms`), its made-up op_ids are replaced by the real ones, and it is used under the same conditions as any prepared reply. A `ready` record's `depth` is 1 for the running tree and 2 or more for trees ahead, and its `outcome` names the path, e.g. `succeeded/0` for the tree the first situation dispatches.

`"read_only": [contract ids]` in `context_json` names capabilities that change nothing, e.g. `{"speculate":true,"lookahead":2,"read_only":["robonix/system/scene/find"]}`. A tree that calls only those may run before its turn, because running it twice costs a lookup and nothing else:

- When a success reply would dispatch such a tree and none of its arguments is an output still unknown, Pilot runs the tree at once through Executor (a `read-ahead-…` plan, logged as a `read` record) and asks for the reply after it with what each step returned, instead of asking blind. That reply is used only if the steps return the same when the tree runs in its turn; numbers may differ by the drift of a repeated measurement (0.1, or 20%), anything else must be equal, and a step that returns an image is never read ahead. Otherwise the speculation is discarded (`results differ from those read ahead`).
- A situation whose reply is such a tree is used whatever probability the decision model gives it, as long as the model picks it: a wrong pick costs one lookup.

With a TypeSafe decision model configured, a fast model answers two questions that the conditions above cannot decide:

- **Which situation happened.** Each branch asks the planning model for one to three situations (for the success of a tree not known to end before a reply could be written, a bold one, which carries the task as far as one tree can from what the outputs most likely show, and a careful one, which moves it forward without relying on them; neither may give the task up or report that it cannot be done, while reporting it done is the bold course when the tree's success completes it), each a sentence saying what the failed step's error, or for the success branch the steps' outputs, would show, plus the reply for that case. A success situation's `when` may rest only on what the outputs say as text, and its reply may state only what that `when` says about the outputs. Once the real results are known, the decision model picks the situation they show, or "none of them". The situation's reply is used only if it is not "none" and its confidence is at least `tau_choice`, or for a situation prepared for success, `tau_success` (which defaults to `tau_choice`). If the next move is the same whatever the results show, the planning model gives one situation with an empty `when`, and its reply is used without asking.
- **Whether the task is done.** A prepared reply that dispatches no `do` step closes the task. Unless it is the reply of the situation just picked, it is used only if the decision model gives probability at least `tau_done` that this turn's step results support the reply and that it finishes the task.

Otherwise the branch is discarded and the normal request runs. Both questions are asked as soon as the tree ends, while the round gathers its context, and each waits at most 5 s for its answer. The decision model reads the step results as text, so an image result goes to it as `[image, not shown]`. The defaults, `tau_choice` 0.9411 and `tau_done` 0.834, were calibrated for a local Qwen3.5-9B at a 2% error target; a task can set its own in `context_json`, e.g. `{"speculate":true,"tau_done":0.8}`. A success situation chooses between what the outputs show, which a decision model gets right more often at the same confidence than the cause of a failure, so it may warrant a lower `tau_success`. Each question logs a `decide` record with its state, question, answer, latency and whether it was accepted. Configuration:

- `TYPESAFE_BASE_URL`: the full `/v1/systemone` URL. If unset, speculation behaves as described above without a decision model.
- `TYPESAFE_MODEL`: model name sent with each question. Defaults to `jev-latest`.
- `TYPESAFE_API_KEY`: optional bearer key.

## RTDL conditions and loops

RTDL supports `if` (`condition`, `then`, optional `else`), `repeat` (`count`, `body`), and bounded `while` (`condition`, `max_iterations`, `body`). A `do.bind` stores successful, verified output in the plan's execution context. Reference it with `{"var":"name","path":["field",0]}` in arguments or conditions. Conditions accept booleans and `{"op":"eq|ne|lt|le|gt|ge|and|or|not","args":[...]}` expressions. Missing values and non-boolean conditions fail explicitly.

Variables are isolated per plan; loops update the same binding each iteration. Bound calls wait for verification even under overlap mode, and loop iterations wait for all descendant verification. Parallel siblings must be independent; their results may be consumed after the join. Repeat counts are 0–10000 and while requires a 1–10000 iteration limit. A while condition still true at the limit fails. See [RTDL Planning Flow](#rtdl-planning-flow) for the response format and examples.
During streamed generation, Pilot submits completed ordinary nodes as soon as they are safe to dispatch. It submits an `if`, `repeat`, or `while` subtree after its full JSON object closes; Executor can then evaluate its condition or repeat its body without relying on later model output.
The `[pilot/rtdl]` plan tree logs include repeat counts, while limits, condition expressions and do binding names. Expressions and binding names use bounded previews. `[pilot/forest]` logs every Executor node-state event at info level and summarizes the completed plan. Compare the planned fields and forest events with Executor's `[executor/rtdl]` records to check what actually ran.
