# Task Framework Reference Artifacts

This folder contains English reference translations of the task-management artifacts used around the workflow runtime.

Purpose:
- make the task framework readable from the runtime repository without copying the whole management system into runtime code
- keep the original Russian documents as the authoritative source
- provide a stable reference for readers of `orchestrator/README.md`

Included files:
- `task_template.en.md` — full English reference translation of `docs/common/templates/task_template.md`
- `task_management.en.md` — full English reference translation of `docs/common/procedures/task_management.md`

Authoritative originals:
- `docs/common/templates/task_template.md`
- `docs/common/procedures/task_management.md`

## Worktree and branch model

Recommended operational model for task execution:

- one **task** gets one dedicated git branch
- one **active executor** works in one dedicated git worktree
- if a task is split into truly parallel subtasks, each subtask may get its own branch and worktree derived from the parent task branch

Recommended naming:

- task branch: `task/<task-id>`
- subtask branch: `task/<task-id>/<subtask-id>`
- worktree path: `workspace/tasks/<task-id>/`
- parallel subtask worktree path: `workspace/tasks/<task-id>/<subtask-id>/`

Why this model is useful:

- task history and git history stay aligned
- parallel agents do not overwrite each other
- each worktree has a clear relationship to one task card
- review, rollback, and cleanup are simpler because each branch/worktree maps to one execution scope

Minimal lifecycle:

1. Create the task folder and `TASK.md`
2. Create branch `task/<task-id>`
3. Create worktree for that branch
4. Execute inline steps or spawn subtask files
5. For parallel subtasks, create subtask branch/worktree pairs
6. Merge subtask outputs back into the parent task
7. Commit, review, and push from the corresponding task branch

This is guidance for orchestration and operator workflows. The runtime itself does not require git worktrees internally, but this model fits the task-card system well and keeps execution isolated.
