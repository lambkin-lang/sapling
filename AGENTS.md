# AGENTS.md

Top-level agent rules for this repository.

If there is a conflict between this file and higher-priority system/developer
instructions, follow higher-priority instructions.

## Greenfield Project Policy

This repository is a greenfield project in a very early stage.

Default behavior for agents:
- Do not add backward-compatibility shims, alias layers, migration wrappers, or
  deprecation scaffolding unless the user explicitly asks for them.
- Do not add "legacy" labels, naming, or compatibility clutter for code paths
  that are part of the current design.
- Prefer direct, simple implementations optimized for current requirements.
- Apply software engineering principles based on project context; do not enforce
  mature-project compatibility patterns by default.

## Roadmap-Driven Architecture Policy

This project has a known product vision and planned next steps. Agents must
optimize for a conceptually coherent system, not just local short-term patches.

Required mindset:
- Do not apply YAGNI or "avoid over-engineering" as blanket rules.
- Evaluate changes against near-term roadmap impact across multiple subsystems.
- Prefer foundational, general solutions when upcoming work is likely to reuse
  the same concepts, invariants, or interfaces.
- Favor semantic correctness and whole-system consistency over local minimalism.

Decision rubric (use explicitly in reasoning and plan reviews):
- If a change will affect two or more subsystems in upcoming milestones, design
  the shared abstraction now.
- If a narrow fix is likely to create rework, conceptual mismatch, or duplicate
  logic soon, do the broader structural change now.
- If a proposed generalization is not connected to the roadmap, avoid it.

Execution expectations:
- Document cross-component implications (APIs, data model invariants, error
  semantics, protocol boundaries, tests) before implementation.
- When reviewing plans, challenge advice that is generically conservative but
  misaligned with this project's stage, goals, or architecture trajectory.
- Present tradeoffs in terms of project success risk, including the risk of
  under-designing critical foundations.

## Git Honesty Policy

Never use history-modifying git commands unless a user explicitly asks for them.
This includes:
- `git commit --amend`
- `git rebase` (interactive or otherwise)
- `git push --force` and `git push --force-with-lease`

Expected behavior:
- If a mistake is made, commit a fix as a new additive commit.
- If a merge is needed, use a standard `git merge`.
- If linting or formatting needs fixing, add a separate follow-up commit.

## Output Conventions

### Plaintext Contract

When the user says "plaintext", treat that as an output transport constraint,
not a style constraint.

Rules:
- Use 7-bit ASCII only (no curly quotes, no control characters, no emoji).
- Put all important user-facing content inside one or more five-backtick fenced
  code blocks with `text` as the info string.
- Markdown is allowed inside that fenced payload because Markdown is plain text.
- Text outside the fenced block is allowed for side-channel communication to the
  user and is not intended to be forwarded to the agent.
- The content inside the five-backtick `text` block is the only content intended
  to be sent to the agent.
- Default to a single block; use multiple blocks only for explicit alternatives.

For branching decisions:
- You may provide multiple alternative agent messages.
- Put each alternative in its own five-backtick `text` block.
- Use normal text before/between/after blocks for side-channel guidance about
  when to choose each alternative.

Required default structure:

`````text
PASTEABLE CONTENT HERE
`````

### Plan Review Tone Contract

When reviewing another coding agent's plan, the reply must be short, kind,
actionable, and realistic. If concerns exist, state them concretely. If
remaining feedback has diminishing returns, say it is minor and not a blocker.
If "plaintext" is requested, place the review content in the five-backtick
`text` block defined above.

### Review Scope Contract

When reviewing work for a specific task, focus on files and behavior that are
directly in scope for that task.

Rules:
- Treat unrelated edits in the workspace as likely parallel development.
- Do not flag unrelated edits as findings unless they directly conflict with, or
  create risk for, the task under review.
- If unrelated edits are noticed, mention them briefly only as context, not as
  primary findings.

### Path Reference Contract

When referring to files, prefer paths relative to the project root.
For reusable shell commands, do not hardcode user-specific absolute home paths.
Use `~` or `$HOME` when a home-relative path is needed.

## Git Commit Message Contract

Write commit messages to a temporary file and commit with `git commit -F`.
Do not use fragile inline heredoc command substitution patterns for commit
messages.

Example:

```bash
cat > /tmp/lambkin-commit-msg.txt << 'MSGEOF'
Subject line here

Body here.
MSGEOF
git commit -F /tmp/lambkin-commit-msg.txt
```
