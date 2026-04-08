# Native LangGraph Topologies

This file contains:
- the native top-level orchestrator `LangGraph`
- the native reusable `task_unit` subgraph
- one combined detailed view that shows phases together with their internal subphases

The first two graphs are taken directly from `LangGraph`. The combined detailed view is a documentation view built from the native graph topology plus the current runtime contracts in `task_unit_graph.py`, `collect_phase.py`, `plan_phase.py`, `execute_phase.py`, and `validate_phase.py`.

## Top-Level Orchestrator Graph

### Mermaid

```mermaid
---
config:
  flowchart:
    curve: linear
---
graph TD;
	__start__([<p>__start__</p>]):::first
	collect(collect)
	plan(plan)
	execute(execute)
	validate(validate)
	human_gate(human_gate)
	__end__([<p>__end__</p>]):::last
	__start__ --> collect;
	collect -.-> plan;
	execute -.-> human_gate;
	execute -.-> plan;
	execute -.-> validate;
	human_gate -.-> __end__;
	human_gate -.-> plan;
	plan -.-> collect;
	plan -.-> execute;
	plan -.-> human_gate;
	validate -.-> __end__;
	validate -.-> collect;
	validate -.-> human_gate;
	validate -.-> plan;
	collect -.-> collect;
	plan -.-> plan;
	validate -.-> validate;
	classDef default fill:#f2f0ff,line-height:1.2
	classDef first fill-opacity:0
	classDef last fill:#bfb6fc
```

### Nodes

| Key | Id | Name |
| --- | --- | --- |
| `__start__` | `__start__` | `__start__` |
| `collect` | `collect` | `collect` |
| `plan` | `plan` | `plan` |
| `execute` | `execute` | `execute` |
| `validate` | `validate` | `validate` |
| `human_gate` | `human_gate` | `human_gate` |
| `__end__` | `__end__` | `__end__` |

### Edges

| Source | Target | Conditional |
| --- | --- | --- |
| `__start__` | `collect` | `False` |
| `collect` | `collect` | `True` |
| `collect` | `plan` | `True` |
| `plan` | `collect` | `True` |
| `plan` | `execute` | `True` |
| `plan` | `human_gate` | `True` |
| `plan` | `plan` | `True` |
| `execute` | `human_gate` | `True` |
| `execute` | `plan` | `True` |
| `execute` | `validate` | `True` |
| `validate` | `__end__` | `True` |
| `validate` | `collect` | `True` |
| `validate` | `human_gate` | `True` |
| `validate` | `plan` | `True` |
| `validate` | `validate` | `True` |
| `human_gate` | `__end__` | `True` |
| `human_gate` | `plan` | `True` |

## Task Unit Graph

### Mermaid

```mermaid
---
config:
  flowchart:
    curve: linear
---
graph TD;
	__start__([<p>__start__</p>]):::first
	executor(executor)
	guardrail(guardrail)
	reviewer(reviewer)
	tester(tester)
	task_unit_human_gate(task_unit_human_gate)
	finish(finish)
	__end__([<p>__end__</p>]):::last
	__start__ --> executor;
	executor --> guardrail;
	guardrail -.-> executor;
	guardrail -.-> finish;
	guardrail -.-> reviewer;
	guardrail -.-> task_unit_human_gate;
	guardrail -.-> tester;
	reviewer --> guardrail;
	tester --> guardrail;
	finish --> __end__;
	task_unit_human_gate --> __end__;
	classDef default fill:#f2f0ff,line-height:1.2
	classDef first fill-opacity:0
	classDef last fill:#bfb6fc
```

### Nodes

| Key | Id | Name |
| --- | --- | --- |
| `__start__` | `__start__` | `__start__` |
| `executor` | `executor` | `executor` |
| `guardrail` | `guardrail` | `guardrail` |
| `reviewer` | `reviewer` | `reviewer` |
| `tester` | `tester` | `tester` |
| `task_unit_human_gate` | `task_unit_human_gate` | `task_unit_human_gate` |
| `finish` | `finish` | `finish` |
| `__end__` | `__end__` | `__end__` |

### Edges

| Source | Target | Conditional |
| --- | --- | --- |
| `__start__` | `executor` | `False` |
| `executor` | `guardrail` | `False` |
| `guardrail` | `executor` | `True` |
| `guardrail` | `finish` | `True` |
| `guardrail` | `reviewer` | `True` |
| `guardrail` | `task_unit_human_gate` | `True` |
| `guardrail` | `tester` | `True` |
| `reviewer` | `guardrail` | `False` |
| `tester` | `guardrail` | `False` |
| `finish` | `__end__` | `False` |
| `task_unit_human_gate` | `__end__` | `False` |

## Detailed Combined View

This graph is not emitted directly by `LangGraph`; it is a composed documentation view:
- top-level phase routing comes from the native orchestrator graph
- subphase routing comes from the native `task_unit` graph
- repeated task-unit instances are expanded per phase for readability

```mermaid
flowchart TD
    Start([start]) --> CollectExec

    subgraph Collect["collect / collector"]
        CollectExec[collect.executor]
        CollectGuard1[collect.guardrail]
        CollectReviewer[collect.reviewer]
        CollectGuard2[collect.guardrail]
        CollectFinish[collect.finish]

        CollectExec --> CollectGuard1
        CollectGuard1 -. retry_executor .-> CollectExec
        CollectGuard1 -. next .-> CollectReviewer
        CollectReviewer --> CollectGuard2
        CollectGuard2 -. retry_reviewer .-> CollectReviewer
        CollectGuard2 -. done .-> CollectFinish
    end

    CollectFinish --> PlanExec
    CollectFinish -. collect_retry .-> CollectExec

    subgraph Plan["plan / supervisor"]
        PlanExec[plan.executor]
        PlanGuard1[plan.guardrail]
        PlanReviewer[plan.reviewer]
        PlanGuard2[plan.guardrail]
        PlanFinish[plan.finish]

        PlanExec --> PlanGuard1
        PlanGuard1 -. retry_executor .-> PlanExec
        PlanGuard1 -. next .-> PlanReviewer
        PlanReviewer --> PlanGuard2
        PlanGuard2 -. retry_reviewer .-> PlanReviewer
        PlanGuard2 -. done .-> PlanFinish
    end

    PlanFinish --> ExecuteExec
    PlanFinish -. back_to_collect .-> CollectExec
    PlanFinish -. replan .-> PlanExec
    PlanFinish -. ask_human .-> HumanGate

    subgraph Execute["execute / per-subtask worker"]
        ExecuteExec[execute.executor / OpenHands]
        ExecuteGuard1[execute.guardrail]
        ExecuteReviewer[execute.reviewer]
        ExecuteGuard2[execute.guardrail]
        ExecuteTester[execute.tester]
        ExecuteGuard3[execute.guardrail]
        ExecuteFinish[execute.finish]
        ExecuteHuman[execute.task_unit_human_gate]

        ExecuteExec --> ExecuteGuard1
        ExecuteGuard1 -. retry_executor .-> ExecuteExec
        ExecuteGuard1 -. next .-> ExecuteReviewer
        ExecuteReviewer --> ExecuteGuard2
        ExecuteGuard2 -. retry_reviewer .-> ExecuteReviewer
        ExecuteGuard2 -. next .-> ExecuteTester
        ExecuteGuard2 -. escalate .-> ExecuteHuman
        ExecuteTester --> ExecuteGuard3
        ExecuteGuard3 -. retry_tester .-> ExecuteTester
        ExecuteGuard3 -. done .-> ExecuteFinish
        ExecuteGuard3 -. escalate .-> ExecuteHuman
    end

    ExecuteFinish --> ValidateExec
    ExecuteFinish -. needs_replan .-> PlanExec
    ExecuteFinish -. ask_human .-> HumanGate
    ExecuteHuman --> HumanGate

    subgraph Validate["validate / supervisor"]
        ValidateExec[validate.executor]
        ValidateGuard1[validate.guardrail]
        ValidateReviewer[validate.reviewer]
        ValidateGuard2[validate.guardrail]
        ValidateFinish[validate.finish]

        ValidateExec --> ValidateGuard1
        ValidateGuard1 -. retry_executor .-> ValidateExec
        ValidateGuard1 -. next .-> ValidateReviewer
        ValidateReviewer --> ValidateGuard2
        ValidateGuard2 -. retry_reviewer .-> ValidateReviewer
        ValidateGuard2 -. done .-> ValidateFinish
    end

    ValidateFinish --> End([end])
    ValidateFinish -. recollect .-> CollectExec
    ValidateFinish -. replan .-> PlanExec
    ValidateFinish -. revalidate .-> ValidateExec
    ValidateFinish -. ask_human .-> HumanGate

    HumanGate[human_gate] -. resume_plan .-> PlanExec
    HumanGate -. finish .-> End
```
