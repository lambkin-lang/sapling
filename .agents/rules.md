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

## Agent Output Conventions

### Plaintext Contract

When the user says "plaintext", output must be primarily one fenced code block
with the `text` info string. Use five backticks to allow internal backticks
without quoting.

Required format:

Optional (short) preamble.
`````text
PASTEABLE CONTENT HERE
`````
Optional (short) follow up to the user (e.g., "I assumed X, but I can also
revise so that Y is the assumption instead")

Rules:
- Use 7-bit ASCII only (no curly quotes, no control characters, no emoji).
- If bullets are used, only simple `*` and `-` are allowed.
- Do not use markdown outside the single fenced block.
- Do not add headings, bullets, or explanatory text outside the block.

### Plan Review Tone Contract

When reviewing another coding agent's plan, the reply must be:
- Short
- Kind
- Actionable
- Encouraging in a realistic way (frame work as achievable when it is; if doubtful, state concerns kindly and concretely)
- If remaining feedback has diminishing returns, say it is a minor point and
  not a blocker to proceeding with a commit.

And it must follow the Plaintext Contract above.

### Path Reference Contract

When referring to files, prefer paths relative to the project root.

Example:
- Prefer `test/e2e/bytes_edge_cases.lamb` over `/Users/mshonle/Projects/lambkin-lang/test/e2e/bytes_edge_cases.lamb`.

For shell commands intended to be reusable across user accounts:
- Do not hardcode `/Users/mshonle` style prefixes.
- Use `~` or `$HOME` (UNIX style) when a home-relative path is needed.

### Git Commit Message Contract

Always write the commit message to a temporary file and use `git commit -F`:

```bash
cat > /tmp/lambkin-commit-msg.txt << 'MSGEOF'
Subject line here

Body here.
MSGEOF
git commit -F /tmp/lambkin-commit-msg.txt
```

Never use `git commit -m "$(cat <<'EOF'...EOF)"`. That construct is fragile:
backticks, single quotes, and certain shell metacharacters inside the heredoc
body cause unexpected parse failures even when the heredoc delimiter is quoted.
