---
description: Audit the current project state and surface what's next, what's missing, and what's at risk.
---

Audit this project's current state and tell me three things, in this order:

1. **What's next.** The next 3 actionable items, ranked by leverage. For each, name the file or component it touches and the single thing that would close it.
2. **What's missing.** Anything tracked as deferred, partial, scaffolded, or TODO that should be revisited. Cross-reference these against the ROADMAP and any `🟡` markers.
3. **What's at risk.** Items likely to silently rot — stale docs that contradict the code, untested code paths, ADRs the codebase has drifted from, dependencies overdue for a bump, exit criteria not yet verified by a test.

## How to gather the picture

Read, in order:

- `docs/steering/ROADMAP.md` if present, otherwise `docs/ROADMAP.md`.
- The most recent `## [Unreleased]` block of `CHANGELOG.md`.
- The last 5 commits — `git log --oneline -5`.
- Anything in `TaskList` that's still `pending` or `in_progress`.
- `git status --short` for uncommitted work.

If this is a multi-repo session (e.g., both the server `muninn` and the SDK `muninn-py` are checked out), audit whichever working tree the current shell is in. Note the other one only if its state is directly load-bearing for an item.

## How to report

Keep the whole reply under ~250 words. Format:

```
## What's next
1. <action> — <file or component> — <closing condition>
2. ...
3. ...

## What's missing
- <item> — <why it matters / what would close it>
- ...

## What's at risk
- <item> — <how it might rot>
- ...

## Recommended next move
<one sentence — usually pointing at the top "What's next" item>
```

Don't dispatch agents. Don't run the test suite. Don't write or modify files. This is an audit, not work. If you can't tell the state of something from the docs + git log alone, say so explicitly.
