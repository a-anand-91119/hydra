# Contributing to Hydra

Thanks for your interest in helping out 🐉. Hydra is small enough that there's no formal process — read this once and you'll know how to land a change.

## Quick start

```sh
git clone <this-repo-url>
cd hydra
python -m venv venv && source venv/bin/activate
pip install -e '.[dev]'

# Verify your setup
ruff check hydra tests
ruff format --check hydra tests
pytest
hydra --help
```

If all four green, you're set up.

## Project layout

```
hydra/                  # the package
├── cli.py              # Typer commands (create, configure, status, config-path)
├── wizard.py           # interactive Questionary wizards
├── config.py           # YAML config loader (~/.config/hydra/config.yaml)
├── secrets.py          # token resolver: keyring → env → .env → prompt
├── errors.py           # HydraAPIError + raise_for_response (HTTP→message)
├── gitlab.py           # GitLab project + group API
├── github.py           # GitHub repo creation API
├── mirrors.py          # remote-mirror setup on the self-hosted GitLab
└── utils.py            # small pure helpers (slug)

tests/                  # pytest suite — pure functions only, no network
.gitlab-ci.yml          # lint → unit-tests → smoke → build → publish
pyproject.toml          # deps, ruff, pytest, coverage config
```

## Where to put things

| If you're adding... | Put it in... |
| --- | --- |
| A new CLI command or flag                 | `hydra/cli.py` (and a wizard step in `wizard.py` if interactive) |
| New HTTP API call                          | the matching `gitlab.py` / `github.py` / `mirrors.py` |
| New host-error mapping (e.g. 429, 451)     | `hydra/errors.py:raise_for_response` |
| Config key                                 | `hydra/config.py` (also update `config.yaml.example`) |
| Pure helper                                | `hydra/utils.py` |

API modules **must** route every response through `raise_for_response(...)` — never inline-raise a generic exception. Pass `host=` so the error message names the right service, and `host_url=` so the rotation hint can render the real URL.

## Running checks

```sh
ruff check hydra tests          # lint
ruff format hydra tests         # auto-format (in-place)
ruff format --check hydra tests # CI's check; passes if no changes needed
pytest                          # full suite
pytest --cov=hydra --cov-report=term-missing  # with coverage
```

CI runs the same commands. If `ruff check` is clean and `pytest` is green locally, CI will be too.

## Code style

- **Formatter is authoritative.** Don't argue with ruff format. Run it before committing.
- **Type hints everywhere.** `from __future__ import annotations` is at the top of every module so you can use `X | None` freely — **except** in `hydra/cli.py`, where Typer evaluates annotations at runtime via `get_type_hints()`. Stick to `Optional[X]` there.
- **Comments are rare.** Default to none. Only write a comment when the *why* isn't obvious from the code (a hidden invariant, a workaround, a tricky edge case).
- **No print().** Use Rich's `Console` for user-facing output; route errors through `_render_api_error` in `cli.py`.
- **No mutable module-level state.** Token caches, console singletons, etc. should be created per-command.

## Tests

Tests live in `tests/` and **must not touch the network**. Use the `fake_response` fixture in `conftest.py` to simulate `requests.Response` objects.

```python
def test_404_returns_not_found(fake_response):
    r = fake_response(404, {"message": "Not found"})
    with pytest.raises(HydraAPIError) as exc_info:
        raise_for_response(r, host="github", action="searching")
    assert exc_info.value.status_code == 404
```

Coverage floor is 30% (set in `pyproject.toml`). New code that adds executable lines without tests will lower coverage; don't ship below the floor.

What we don't test (yet):
- Live `cli.py` command flows — Typer's `CliRunner` works but no tests exist yet, contributions welcome.
- Wizard rendering — would need a Questionary-aware harness.

## Commit messages

We use [Conventional Commits](https://www.conventionalcommits.org). Look at `git log` for examples — common prefixes:

- `feat(scope): ...` — new functionality
- `fix(scope): ...` — bug fix
- `chore(scope): ...` — non-functional change (deps, config, formatting)
- `ci: ...` — pipeline tweaks
- `docs: ...` — README/CONTRIBUTING/comments
- `refactor: ...` — internal change, no behaviour shift
- `release: 0.X.Y` — version bump (see *Releasing* below)

Keep the subject line under ~70 chars. Body is for the **why**, not the **what** — diffs already show what.

## Pull requests / merge requests

This repo lives on a self-hosted GitLab; PRs are MRs there. Workflow:

1. Branch from `main`: `git switch -c feat/your-thing`.
2. Make focused commits — one logical change per commit, atomic-ish.
3. `pytest` and `ruff check` pass locally.
4. Push and open an MR.
5. CI must be green (`lint`, `unit-tests`, `smoke`).
6. One approving review + merge to `main`.

If you're touching the publish pipeline (`.gitlab-ci.yml` `publish:` job): test on a throwaway tag like `vtest-1` first — the rules in CI will only kick in for `^v\d+\.\d+\.\d+$`, so use a tag matching that pattern only when you're sure.

## Releasing

Releases go to PyPI as `hydra-repo-syncer` via the OIDC → GCP Workload Identity → PyPI Trusted Publisher flow in `.gitlab-ci.yml`.

To cut a release:

1. Bump the version in **two places** (these are currently out of sync intentionally — pick one and keep them aligned):
   - `pyproject.toml` → `[project] version = "X.Y.Z"`
   - `hydra/__init__.py` → `__version__ = "X.Y.Z"`
2. Commit: `chore: bump version to X.Y.Z`.
3. Tag: `git tag vX.Y.Z`.
4. `git push && git push --tags`.

CI will run `lint → unit-tests → smoke → build → publish` and the last stage uploads to PyPI. Watch the `publish` job logs to confirm.

> **Heads up:** the OIDC publish stage talks to GitLab → GCP STS → IAM Credentials → PyPI in five steps. If any of those break, the failure tends to be cryptic. Check `97fdb14`, `520ed5c` in the git log for prior fixes.

## Reporting bugs

Open an issue on the GitLab project with:

1. What you ran (`hydra create ...`).
2. What happened — paste the **whole** Hydra error block (including the `Partial progress` section if shown).
3. What you expected.
4. `hydra --version` and `python --version`.

If a token or URL appears in the output, scrub it before pasting.

## Security

If you find something with security implications (e.g. a way to leak a token, a URL injection in mirror setup), please don't open a public issue. Email the maintainer or use GitLab's *Submit vulnerability* flow on the project page.

## Questions

For anything not covered here, open a discussion-style issue on GitLab — chances are the next contributor will have the same question and we should document the answer.
