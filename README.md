# Orchestrator

Phase-driven V1 orchestrator:

- top-level control flow lives in LangGraph
- runtime graph shape lives in `orchestrator/config/`
- role knowledge and prompts live in `docs/common/roles/`
- worker execution can run through `MockDriver` or `OpenHandsDriver`

## V1 Architecture

```text
collect -> plan -> execute -> validate -> human_gate
```

The graph is intentionally static at the phase level. Dynamic behavior lives in two places:

1. `PipelineState.plan` stores a mutable list of `SubtaskState`
2. `execute` runs a universal `TaskUnit` sequentially over ready subtasks

`TaskUnit` is the reusable pipeline used by `collect`, `plan`, `execute`, and `validate`:

```text
executor -> simple guardrails -> reviewer -> simple guardrails -> tester? -> simple guardrails
```

## Sources Of Truth

Runtime source of truth:

- `orchestrator/config/flow.yaml`
- `orchestrator/config/phases_and_roles.yaml`

Human-readable design / knowledge source:

- `docs/common/roles/flow_design.md`
- `docs/common/roles/{role}/role.yaml`
- `docs/common/roles/{role}/{executor,reviewer,tester}.md`
- `docs/common/roles/_shared/*.md`

Task artifacts used during this refactor:

- `management-stage/task-history/2026-03-24_1800__multi-agent-system-design/TASK.md`
- `management-stage/task-history/2026-03-24_1800__multi-agent-system-design/phase2-v1-orchestrator-refactor.md`
- `management-stage/task-history/2026-03-24_1800__multi-agent-system-design/phase2-langgraph-skeleton.md` (historical only)

## Package Layout

```text
orchestrator/
├── config/
│   ├── flow.yaml
│   └── phases_and_roles.yaml
├── squadder_orchestrator/
│   ├── graph.py                         # compatibility entrypoint -> V1 compiler
│   ├── config.py                        # compatibility loaders -> V1 manifests
│   ├── state.py                         # compatibility aliases -> PipelineState/SubtaskState
│   ├── graph_compiler/
│   │   ├── state_schema.py
│   │   ├── yaml_manifest_parser.py
│   │   ├── edge_evaluators.py
│   │   └── langgraph_builder.py
│   ├── node_implementations/
│   │   ├── human_gate.py
│   │   ├── status_aggregation.py
│   │   ├── phases/
│   │   │   ├── collect_phase.py
│   │   │   ├── plan_phase.py
│   │   │   ├── execute_phase.py
│   │   │   └── validate_phase.py
│   │   └── task_unit/
│   │       ├── executor_node.py
│   │       ├── reviewer_node.py
│   │       ├── tester_node.py
│   │       ├── guardrail_checker.py
│   │       └── runner.py
│   ├── agent_drivers/
│   │   ├── base_driver.py
│   │   ├── mock_driver.py
│   │   └── openhands_driver.py
│   └── integrations/
│       ├── observability.py
│       ├── openhands_http_api.py
│       ├── phase_config_loader.py
│       ├── prompt_composer.py
│       └── tasks_storage.py
└── tests/
    ├── conftest.py
    ├── mocks.py
    ├── test_flow.py
    ├── test_checkpoint.py
    ├── test_openhands_driver.py
    └── test_openhands_runtime.py
```

## Main Runtime Types

- `PipelineState`: full state of one orchestrator run
- `SubtaskState`: one mutable plan item
- `StructuredOutput`: required executor result contract
- `TaskUnitResult`: normalized output of the universal task unit

Compatibility aliases are kept:

- `AgentState = PipelineState`
- `Subtask = SubtaskState`

## Driver Modes

Supported runtime modes:

- `mock`: deterministic local driver for tests and dry-runs
- `openhands`: real OpenHands Agent Server via `OpenHandsHttpApi`

`compile_graph()` chooses the driver in this order:

1. explicit `driver=...`
2. explicit `driver_mode=...`
3. env `SQUADDER_ORCHESTRATOR_DRIVER_MODE`
4. fallback `mock`

## OpenHands Notes

The current V1 integration is verified against local `openhands-agent-server v1.16`.

Verified behavior:

- conversation creation works
- `run` works
- polling conversation state works
- event search works when `limit <= 100`
- YAML payload normalization works through `OpenHandsDriver`

Important runtime note:

- `runtime.openhands.tools` is currently `[]`
- local `openhands-agent-server v1.16` returned `KeyError: ToolDefinition 'TerminalTool' is not registered` when `TerminalTool` / `FileEditorTool` were requested directly
- because of that, the verified smoke path uses tool-less conversations for now

This keeps the OpenHands integration honest and runnable, but it also means that real code-edit execution through the local agent server still needs a separate tool-registration pass.

## Setup

```bash
cd /root/squadder-devops/orchestrator
uv sync
```

For real OpenHands runtime:

```bash
export OPENHANDS_BASE_URL="http://127.0.0.1:8000"
export OPENROUTER_API_KEY="<secret>"
```

Optional driver selection:

```bash
export SQUADDER_ORCHESTRATOR_DRIVER_MODE="openhands"
```

## Running Tests

```bash
uv run pytest tests/ -v
```

Current test coverage verifies:

- happy-path V1 mock execution
- dependency-aware sequential `execute`
- manifest loading
- prompt composition
- human gate interrupt/resume
- SQLite checkpoint persistence
- OpenHands YAML payload normalization

## Real Smoke Evidence

Verified locally during this task:

- local `openhands.agent_server` started successfully on `127.0.0.1:8000`
- real `OpenHandsDriver` request against the live server returned:

```text
{'status': 'PASS', 'conversation_id': '65fec569-cc72-4a2c-ac25-0ac5b1828822', 'summary': 'OpenHands smoke test ok'}
```

The smoke run used:

- `OpenHandsHttpApi`
- `OpenHandsDriver`
- OpenRouter base URL
- `tools=[]` due the server-side registry limitation described above

## What Changed From Pre-V1

Pre-V1 / historical design:

- docs-based generic YAML graph builder
- registry-driven node graph
- subgraph fan-out as the primary execution primitive

Current V1 design:

- phase-driven top-level graph
- mutable plan as runtime control surface
- universal `TaskUnit`
- runtime config moved into `orchestrator/config/`
- OpenHands wired as a driver layer, not as graph structure

## Design Artifacts

The current V1 chain is:

`TASK.md` / design decisions -> `orchestrator/config/*` runtime manifests -> Python phase interpreter

| Артефакт | Что описывает | Путь |
|----------|--------------|------|
| **TASK.md** | Current V1 decisions, state schema intent, evidence | `management-stage/task-history/2026-03-24_1800__multi-agent-system-design/TASK.md` |
| **phase2-v1 subtask** | Implementation lane, structured output, evidence | `management-stage/task-history/2026-03-24_1800__multi-agent-system-design/phase2-v1-orchestrator-refactor.md` |
| **flow.yaml** | Runtime phase topology, statuses, transitions | `orchestrator/config/flow.yaml` |
| **phases_and_roles.yaml** | Runtime pipelines, prompts, retries, OpenHands settings | `orchestrator/config/phases_and_roles.yaml` |
| **flow_design.md** | Human-readable V1 design rationale synced with runtime manifests | `docs/common/roles/flow_design.md` |
| **phase2-langgraph-skeleton** | Historical pre-V1 artifact only | `management-stage/task-history/2026-03-24_1800__multi-agent-system-design/phase2-langgraph-skeleton.md` |

## Configuration

The orchestrator reads runtime configuration from `orchestrator/config/`:

| File | Purpose |
|------|---------|
| `flow.yaml` | Phase graph: `collect -> plan -> execute -> validate -> human_gate`, statuses, transitions |
| `phases_and_roles.yaml` | Phase pipelines, model config, retries, guardrails, OpenHands transport settings |
| `{role}/executor.md` | Executor prompt in `docs/common/roles/` |
| `{role}/reviewer.md` | Reviewer prompt in `docs/common/roles/` |
| `{role}/tester.md` | Optional tester prompt in `docs/common/roles/` |
| `_shared/*.md` | Shared prompt fragments for domain roles |

Set `SQUADDER_DOCS_ROOT` env var to override docs path (default: `/root/squadder-devops/docs`).

## OpenHands Notes

The current V1 integration is verified against local `openhands-agent-server v1.16`.

Verified behavior:

- conversation creation works
- `run` works
- polling conversation state works
- event search works when `limit <= 100`
- YAML payload normalization works through `OpenHandsDriver`
- code-edit-capable conversations work with `tools: ["terminal", "file_editor"]`

Important runtime note:

- the earlier failure was **not** a broken server-side registry
- the problem was an incorrect tool spec: we sent `TerminalTool` / `FileEditorTool`
- in installed `openhands v1.16`, the actual registered tool names are `terminal` and `file_editor`
- `orchestrator/config/phases_and_roles.yaml` now uses those real names
- `tests/test_openhands_runtime.py` locks this assumption against regressions

This means no `tools: []` workaround is needed anymore for the local V1 runtime path.

## Setup

```bash
cd /root/squadder-devops/orchestrator
uv sync
export OPENHANDS_BASE_URL="http://127.0.0.1:8000"
export OPENROUTER_API_KEY="<secret>"
uv run python -m openhands.agent_server --host 127.0.0.1 --port 8000
```

Optional driver selection:

```bash
export SQUADDER_ORCHESTRATOR_DRIVER_MODE="openhands"
```

## Running Tests

```bash
uv run pytest tests/ -v
```

Current test coverage verifies:

- happy-path V1 mock execution
- dependency-aware sequential `execute`
- manifest loading
- prompt composition
- human gate interrupt/resume
- SQLite checkpoint persistence
- OpenHands YAML payload normalization
- OpenHands tool-name contract for the installed SDK/runtime

## Real Smoke Evidence

Verified locally during this task:

- stock `uv run python -m openhands.agent_server --host 127.0.0.1 --port 8011` started successfully
- real `OpenHandsDriver` request against the live server returned:

```text
{'status': 'PASS', 'conversation_id': '7ce2ef08-c0f8-47d9-abfe-2b0a42ebf216', 'summary': 'tool smoke ok'}
```

The smoke run used:

- `OpenHandsHttpApi`
- `OpenHandsDriver`
- OpenRouter base URL
- `tools=["terminal", "file_editor"]`

## Historical Notes

Historical pre-V1 artifacts are kept only for traceability:

- `phase2-langgraph-skeleton.md` remains a pre-V1 implementation artifact
- the active design/runtime contract is now synchronized across `orchestrator/config/*`, `docs/common/roles/flow.yaml`, and `docs/common/roles/flow_design.md`
