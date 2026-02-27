# Agent Rules & Policies

All AI agents working on this project must adhere to the following rules to ensure repository integrity and a truthful development history.

## Git Honesty Policy

**NEVER use history-modifying git commands.** This includes but is not limited to:
- `git commit --amend`
- `git rebase` (interactive or otherwise)
- `git push --force` or `--force-with-lease`

### Rationale
Git tools should reflect the **actual truth and accounting** of what happened during development. Obscuring this history with "tricks" or "vanity" commits is counter-productive and high-risk. We value an honest record over a "nice-looking" one.

### Expected Behavior
- If a mistake is made, commit a fix as a new, additive commit.
- If a merge is needed, use a standard `git merge`. Merges are an honest reflection of parallel work.
- If linting or formatting needs fixing, add a separate commit for it after the main work.

## Other Policies

- **Vanilla CSS Only**: Avoid using TailwindCSS unless explicitly requested by the user.
- **Rich Aesthetics**: Prioritize high-quality, modern web design for any frontend components.
- **Component Focused**: Keep components small, reusable, and focused on a single responsibility.
