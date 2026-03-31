# Task <task-id>: <short title>

## Meta
- Status: planned | in_progress | completed | cancelled | approved_by_user
- Parent: <task-id> | none
- Role: <role-name> | none
- Areas: product | backend | devops | frontend | design
- Repos:
  - /root/dev-stage-squadder/app
  - /root/squadder-devops
  - ...
- Created at: ...
- Updated at: ...
- Template ver 0.3

## Cognitive State

### Goal
- ...
  
### Task context
- ...  

## Execution Plan

Simple steps stay as inline Todo items. Complex subtasks (independent, parallelizable) become separate files inside the task folder.

- [ ] Simple step (inline)
- [ ] **[devops-update-helm](./devops-update-helm.md)** — complex subtask (separate file)
- [ ] **[backend-update-env](./backend-update-env.md)** — complex subtask (separate file)

## StructuredOutput

> Filled by the executor on completion. For parent tasks, this section is not filled directly and is aggregated from subtasks.

<structured_output role="{role-name}">

```yaml
task_id: "<task-id>"
subtask_id: "<this-subtask-id>"
role: "<role-name>"
status: "pending"                    # done | failed | escalated
changes: []
commands_executed: []
tests_passed: []
commits: []
warnings: []
escalation: null                     # {reason: "...", to: "supervisor"}
summary: ""
```

</structured_output>

## Review

> Filled by the reviewer/tester. For standalone tasks, review is performed by a human.

- Reviewer verdict: pending | pass | fail
- Reviewer feedback: ...
- Tester result: pending | pass | fail | skipped

## Result/Answer
- ...

### Evidence
- [ ] API Tests passed (output)
- [ ] Logs confirm (command/screenshot)
- [ ] Reviewed by: Human | Agent

### Rollback Plan
- [ ] ...

## Details

### History / Decisions / Contracts
- D1 ... (what was done, what result it produced, what was learned / what changed in understanding)
- D2 ... (for example: "Tool X did not work, switching to strategy Y because ...")
  
### Hypotheses
- H1: ...
- H2: ...
  
### Product Requirements files read and changes
- ...

### Open Questions and Notes
- Q1: ...
- N1: ...

## Commits
- [backend] /root/dev-stage-squadder/app: <hash> — <summary>
- [devops]  /root/squadder-devops:      <hash> — <summary>

### Commit Message Format

```
<type>: <short description, imperative mood, <=72 characters>

Task: [<task-id>] <relative path to the task card>
Goal: <data from ## Goal>

Done:
- <meaningful change 1>
- <meaningful change 2>

Pending:
- <important remaining open item within the task scope> (omit the section if everything is done)
```

**Types (`type`):**
| type | when to use |
|------|-------------|
| `feat` | new functionality |
| `fix` | bug fix / incident fix |
| `chore` | routine work: config, dependencies, CI, infrastructure without business-logic changes |
| `refactor` | refactoring without behavior changes |
| `perf` | performance optimization |
| `docs` | documentation / guides only |
| `test` | tests |
| `revert` | revert of changes |

**Rules:**
- Subject line: `type(task-id): verb what`, entire block <=72 characters
- `Task:` — path to the task card, so context can be found from any `git log`
- `Goal:` — why the change exists, which is not always obvious from the diff
- `Done:` — meaningful units, not a code retelling; they should match execution plan steps
- `Pending:` — only critically important unfinished work; omit the whole section if everything is complete

## Guides Changes
- BACKEND_GUIDE.md — added section ... (why)
