<p align="center">
  <img src="images/hydra-2.png" alt="Hydra" width="180">
</p>

> **One source, many mirrors.** Provision a single repo on self-hosted GitLab, GitLab.com, and GitHub in one shot — with push mirroring wired up so every push fans out automatically.

Hydra is a small Python CLI for teams who keep code on a **self-hosted GitLab** but also need it on **GitLab.com** and/or **GitHub** — for open-source releases, customer access, vendor integrations, or backup. You run one command and Hydra creates the project on all three hosts, then configures GitLab's built-in push mirrors so the self-hosted copy is the only place you ever push.

```
                                       ┌──────────────────┐
                                  ┌──▶ │   GitLab.com     │
                                  │    └──────────────────┘
   ┌────────────────────────┐  push
   │  Self-hosted GitLab    │ ──┤
   │  (source of truth)     │  push
   └────────────────────────┘    │    ┌──────────────────┐
            ▲                    └──▶ │     GitHub       │
            │                         └──────────────────┘
       git push (you)
```

---

## Requirements

- **Python 3.9 or newer**
- Permission to create projects/repos on each host you want to use (self-hosted GitLab, GitLab.com, GitHub)
- A personal access token for each host (Hydra tells you exactly which scopes during setup — see [Token scopes](#token-scopes))

---

## Install

From PyPI:

```sh
pip install hydra-repo-syncer
hydra --version
```

From source (if you'd rather pin to a checkout, or want to hack on Hydra itself):

```sh
git clone <this-repo-url>
cd hydra
python -m venv venv && source venv/bin/activate
pip install -e .
hydra --version
```

After installing, the `hydra` command is on your `PATH`.

---

## Quickstart

```sh
# 1. One-time setup — pick hosts, defaults, and store tokens
hydra configure

# (optional) shell tab-completion for bash/zsh/fish
hydra --install-completion

# 2. See what *would* happen, without making any API calls
hydra create my-first-repo --dry-run

# 3. Do it for real
hydra create my-first-repo
```

That's it. The repo now exists on all three hosts, and any future `git push` to the self-hosted GitLab will mirror automatically to the other two.

---

## Configure (one-time)

```sh
hydra configure
```

A four-step wizard walks you through:

| Step | What you provide |
| ---- | ---------------- |
| 1. Hosts          | URLs for self-hosted GitLab, GitLab.com, and GitHub |
| 2. GitHub account | Your GitHub user, or an organisation name |
| 3. Defaults       | Default group path; default visibility (private/public) |
| 4. Tokens         | API tokens for each host, plus where to store them |

Non-secret settings are saved to `~/.config/hydra/config.yaml`. **Tokens go to your OS keyring** (macOS Keychain, Linux Secret Service) — never to the YAML.

### Token scopes

When you mint personal access tokens, use these scopes:

| Host | Required scope | Mint a token at |
| ---- | -------------- | --------------- |
| Self-hosted GitLab | `api` | `<your-host>/-/user_settings/personal_access_tokens` |
| GitLab.com         | `api` | https://gitlab.com/-/user_settings/personal_access_tokens |
| GitHub             | `repo` (plus `admin:org` if creating under an organisation) | https://github.com/settings/tokens |

### Token resolution order

For each host, Hydra looks up the token in this order and stops at the first hit:

1. **OS keyring** — set via `hydra configure`, or directly: `keyring set hydra <github|gitlab|self_hosted_gitlab>`
2. **Environment variable** — `HYDRA_GITHUB_TOKEN`, `HYDRA_GITLAB_TOKEN`, `HYDRA_SELF_HOSTED_GITLAB_TOKEN`
3. **`.env` file** in the current working directory (see `.env.example`)
4. **Interactive prompt** (only if attached to a TTY)

This lets you use the keyring on your laptop and env vars in CI without changing anything else.

---

## Creating repos

Two modes — interactive wizard (good for one-offs), or flag-driven (good for scripts).

### Interactive

```sh
hydra create
```

The wizard collects the repo name, description, group, visibility, GitHub destination, and mirror toggle, shows a review summary, then asks you to **create now**, **dry-run**, or **cancel**.

### Flags

```sh
# Dry-run — recommended for the first try; renders the plan, no API calls
hydra create my-repo -d "demo" -g platform/services --dry-run

# Real run — renders the plan first, then prompts y/N before any mutation
hydra create my-repo -d "demo" -g platform/services

# Skip the prompt (useful in CI / scripts)
hydra create my-repo -d "demo" -g platform/services --yes

# Public repo, under a GitHub org, skip mirror setup
hydra create my-repo --public --host-option github.org=acme --no-mirror
```

Omit the name to launch the wizard; pass a name to stay in flag mode.

Every mutating run starts by printing the **plan** — the ordered list of
namespaces / repos / mirrors / journal entries that would be created. With
`--dry-run` it stops there. Without it, you get one confirmation prompt
before any provider call. `--yes` skips the prompt.

| Flag | Meaning |
| ---- | ------- |
| `-d`, `--description`   | Repo description |
| `-g`, `--group`         | Group path on self-hosted GitLab |
| `--public`              | Create as public (default is private) |
| `--host-option <id.k=v>`| Per-host override, e.g. `github.org=acme` |
| `--no-mirror`           | Skip push-mirror setup |
| `--dry-run`             | Print the plan and exit; no API calls |
| `-y`, `--yes`           | Skip the confirmation prompt |
| `--config <path>`       | Use a non-default config file |
| `-v`, `--verbose`       | Print extra detail (group IDs, etc.) |

---

## Inspecting mirrors

```sh
hydra status my-repo            # offline — reads the journal cache
hydra status my-repo --refresh  # re-query the primary, then show
```

Shows per-mirror last status and last error inline for one repo, straight from the journal — no network unless you pass `--refresh`. Exits non-zero if any mirror is unhealthy, so it doubles as a CI health gate. When a mirror is broken, `hydra repair` re-establishes it without a full `scan`.

---

## Commands

| Command | Description |
| ------- | ----------- |
| `hydra create [name]` | Create the repo across all three hosts. Without `name`, runs the wizard. Renders a plan + prompts before applying (skip with `--yes`). |
| `hydra configure`     | Onboarding wizard — config + tokens. |
| `hydra status <name>` | Per-mirror health for one repo from the journal (offline). `--refresh` re-queries the primary first. Exits non-zero if any mirror is unhealthy. |
| `hydra list`          | List journaled repos and last-known mirror status. `--refresh` re-queries the primary (uses `--max-workers`, default 8). |
| `hydra scan`          | Diff the journal against the primary. `--apply` adopts unknowns and resyncs drifted ids (renders a plan + prompts; skip with `--yes`). `--interactive` filters the plan per-repo first. `--max-workers <N>` controls concurrent HTTP calls (default 8, env `HYDRA_SCAN_WORKERS`). |
| `hydra repair [name]` | Re-establish mirrors the journal marks unhealthy (broken/missing/failed/error): re-adds gone mirrors, replaces failing ones. Renders a plan + prompts (skip with `--yes`); supports `--dry-run` and `--host <id>`. |
| `hydra rotate-token`  | Rotate a host PAT in the keyring and rewrite every push-mirror that embeds the old token. |
| `hydra doctor`        | Diagnose configuration, tokens, and topology. `--fix` runs safe migrations. |
| `hydra config-path`   | Print the resolved config-file path. |
| `hydra journal-path`  | Print the resolved journal database path. |

Run `hydra <cmd> --help` for full flags.

---

## Error handling

Hydra translates HTTP failures into actionable messages:

```
✗ GitLab.com authentication failed (401) while searching for group 'platform/services'

  The GitLab.com token was rejected. Rotate it at
  https://gitlab.com/-/user_settings/personal_access_tokens
  and re-run `hydra configure`, or set HYDRA_GITLAB_TOKEN in your environment.
```

If a failure happens **after** some resources have been created, the partial state is reported so you can clean up before retrying:

```
⚠ Partial progress before the failure:
  • self-hosted GitLab repo: https://gitlab.example.com/sandbox/demo
  • gitlab.com group: https://gitlab.com/repo-syncer-managed-groups/sandbox-20260508131245

  These resources exist now. Delete them manually before retrying,
  or use a different repo name.
```

---

## Config file

Lives at `~/.config/hydra/config.yaml` by default. Override with `--config <path>` or the `HYDRA_CONFIG` environment variable. See [`config.yaml.example`](./config.yaml.example) for the full schema:

```yaml
self_hosted_gitlab:
  url: https://gitlab.example.com

gitlab:
  url: https://gitlab.com
  managed_group_prefix: repo-syncer-managed-groups

github:
  url: https://api.github.com
  org: null         # null = create under your user; or set an org name

defaults:
  private: true
  group: ""         # optional default group path on the self-hosted GitLab
```

---

## Security notes

- Tokens are **never** written to the YAML config.
- Tokens injected into mirror URLs (`https://oauth2:<token>@host/...`) are stored on the self-hosted GitLab's `remote_mirrors` table. Anyone with project admin access can read them back via the GitLab API — use **scoped** tokens.
- Keep `.env` gitignored. It already is in this repo.

---

## Development

Clone the repo and install with the `dev` extras:

```sh
git clone <this-repo-url>
cd hydra
python -m venv venv && source venv/bin/activate
pip install -e '.[dev]'
pytest
```

Unit tests cover error translation, slug generation, wizard validators, and credential injection. CI runs the same suite plus a `hydra --help` smoke test on every push (`.gitlab-ci.yml`).

---

## License

MIT. See [`LICENSE`](./LICENSE).
