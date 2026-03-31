<task_management>
## Core entities

- **Task** — one large goal with a single `Goal` (one cognitive context).
- **Subtask** — an independent part of a Task that can be parallelized and assigned to a separate executor. It lives as a separate `.md` file inside the Task folder.
- **Belief State** — the combination of `Goal`, `Context`, `History`, `Hypotheses`, and `Decisions` recorded in the task card's `Cognitive State` section. This is the single source of truth for what is currently known about the overall environment, how the task is understood, and what is expected to be done to complete it.
- **Operational cycle** is always: `Belief State formation -> Planning -> Action -> Update`
- **Fractal planning:** if one step becomes non-trivial, split it into smaller groups until they become simple enough to understand and execute.

## Identifiers

- **task-id**: `YYYY-MM-DD_HHMM__slug`, for example `2025-11-22_1430__add-team-graphql`
- **subtask-id**: `<role>-<slug>`, for example `devops-update-helm`, `backend-update-env`

## Storage structure

Each task is a **folder** with a required `TASK.md` inside:

```
task-history/
└── 2026-03-24_1800__migrate-payment/
    ├── TASK.md                          # main card (parent)
    ├── devops-update-helm.md            # complex subtask
    ├── backend-update-env.md            # complex subtask
    └── ...
```

Folder path:
- workspace exists: `<Task memory storage>/task-history/<task-id>/TASK.md`
- no workspace: `/root/temp/task-history/<task-id>/TASK.md`

If the directory does not exist, create it.

### When a subtask becomes a separate file

A subtask is moved into a separate file if **any** of the following is true:
- It is an independent goal that can be assigned to a separate executor
- It is parallelizable and does not depend on results of other subtasks (or dependencies are explicit)
- It requires its own review/test cycle (executor -> reviewer -> tester)
- Its result is described as StructuredOutput

Simple linear steps stay as Todo items in `TASK.md`.

### TASK.md ↔ subtask relationship

In `TASK.md`, the `## Execution Plan` section contains links to subtask files:

```markdown
## Execution Plan
- [ ] Read guides (inline)
- [ ] **[devops-update-helm](./devops-update-helm.md)** — update Helm values
- [ ] **[backend-update-env](./backend-update-env.md)** — update .env config
- [ ] Check smoke tests (inline)
```

### subtask ↔ StructuredOutput relationship

Each subtask file contains a `## StructuredOutput` section with `task_id` and `subtask_id`. The supervisor reads all subtask files from the task folder during merge.

### Everything in one folder

All task context lives in a single folder. `TASK.md` is the original plan and shared state; subtask files live next to it:

```
task-history/
└── 2026-03-24_1800__migrate-payment/
    ├── TASK.md                          # plan, cognitive state, shared result
    ├── devops-update-helm.md            # subtask (executor -> reviewer -> tester)
    ├── backend-update-env.md            # subtask
    ├── architect-review-adr.md          # subtask
    └── ...simple steps = Todo in TASK.md
```

If a task creates a new large task (different scope, different goal), create a new folder and link it from `TASK.md` via `Parent: <task-id>`.

## Template

Unified template for tasks and subtasks: `[[common/templates/task_template.md]]`

Additional fields for subtasks:
- `Meta.Parent` — parent task-id
- `Meta.Role` — executor role (devops, backend, ...)
- `## StructuredOutput` — result of the executor's work
- `## Review` — reviewer and tester verdicts

Do not skip any field when creating a card.

## Process

After new information appears, a hypothesis changes, or a Todo item is completed, update the card immediately. Do not leave the card stale — that is a workflow error.
Use the "Temp" folder for temporary drafts and scripts.

## Testing

In 100% of cases, test after code changes, deploys, and cache clearing — use skills for that.
If the work was not verified with curl or logs, the task is not `completed`. Ask the user to verify. If they cannot verify yet, keep status as `in_progress` until user confirmation.


## Hand-off to the user for review or clarification

When all TODOs are finished or user help is required:
1. Update `Status -> completed | in_progress` in `## Meta`
2. Record final conclusions in `## Notes`:
   - which guide descriptions worked well and which did not
   - what unexpected difficulties occurred
   - which artifacts/guides were updated
3. Make sure `## Guides Changes` and `## Product Requirements files read and changes` are filled
4. Send the user a summary or ask a question if needed


## Acceptance ("accepted" / "+")

1. Create commits and push.
2. Record final commits in `## Commits`
3. Update `Status -> approved_by_user` in `## Meta`
</task_management>
