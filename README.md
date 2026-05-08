# 🐉 Hydra

> One source, many mirrors — provision a repo across self-hosted GitLab, GitLab.com, and GitHub in one shot, with push mirroring wired up automatically.

The self-hosted GitLab is the source of truth. Hydra creates the repo on all three hosts, then sets up push mirrors on the self-hosted project so pushes fan out to GitLab.com and GitHub.

## ⚠️ If you're forking from `repo-syncer`

The original `syncer.py` had API tokens committed in plaintext. **Revoke them immediately**:

- GitHub: https://github.com/settings/tokens
- GitLab.com: https://gitlab.com/-/user_settings/personal_access_tokens
- Your self-hosted GitLab: `<your-host>/-/user_settings/personal_access_tokens`

Hydra never writes tokens to the YAML config.

## Install

```sh
python -m venv venv && source venv/bin/activate
pip install -e .
hydra --version
```

For development (includes pytest):

```sh
pip install -e '.[dev]'
pytest
```

## Onboarding

```sh
hydra configure
```

A four-step wizard walks you through hosts, defaults, and tokens:

1. **Hosts** — self-hosted GitLab, GitLab.com, and GitHub URLs.
2. **GitHub account** — user account or organization.
3. **Defaults** — default group path, default visibility.
4. **API tokens** — choose where to store: OS keyring (recommended), shell-export lines, or skip and set env vars yourself.

The wizard writes non-secret settings to `~/.config/hydra/config.yaml`. Tokens go to your OS keyring (macOS Keychain, Linux Secret Service) — **never** to the YAML.

### Token resolution order

For each host, Hydra looks up the token in this order:

1. OS keyring (set via `hydra configure`, or directly with `keyring set hydra <github|gitlab|self_hosted_gitlab>`)
2. Environment variable: `HYDRA_GITHUB_TOKEN`, `HYDRA_GITLAB_TOKEN`, `HYDRA_SELF_HOSTED_GITLAB_TOKEN`
3. `.env` file in the current working directory (see `.env.example`)
4. Interactive prompt (only if attached to a TTY)

## Creating repos

Two modes — interactive wizard, or flag-driven for scripting.

### Interactive

```sh
hydra create
```

The wizard collects the repo name, description, group, visibility, GitHub destination, and mirror toggle, then shows a review summary. At the end you choose between **create now**, **dry-run**, or **cancel**.

### Flags

```sh
# Dry-run (no API calls)
hydra create my-repo -d "demo" -g platform/services --dry-run

# Real run (defaults from config.yaml)
hydra create my-repo -d "demo" -g platform/services

# Public repo, under a GitHub org, no mirrors
hydra create my-repo --public --github-org acme --no-mirror
```

If you omit the name, the wizard launches; with a name you stay in flag mode.

## Inspecting mirrors

```sh
hydra status my-repo --group platform/services
```

Shows enabled state, last-sync status, and any errors per mirror.

## Commands

| Command | Description |
| --- | --- |
| `hydra create [name]` | Create the repo across hosts. Without `name`, runs the wizard. |
| `hydra configure` | Onboarding wizard — config + tokens. |
| `hydra status <name>` | Show the self-hosted project's mirror state. |
| `hydra config-path` | Print the resolved config-file path. |

Run `hydra <cmd> --help` for full flags.

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

## Config file

Lives at `~/.config/hydra/config.yaml` by default. Override with `--config <path>` or `$HYDRA_CONFIG`. See `config.yaml.example`.

## Security notes

- Tokens are never written to the YAML config.
- Tokens injected into mirror URLs (`https://oauth2:<token>@host/...`) are stored on the self-hosted GitLab's `remote_mirrors` table. Anyone with project admin access can read them back via the GitLab API. Use scoped tokens.
- Keep `.env` gitignored. It already is in this repo.

## Testing

Unit tests cover error translation, slug generation, wizard validators, and credential injection:

```sh
pip install -e '.[dev]'
pytest
```

CI runs the same suite plus a `hydra --help` smoke test on every push (`.gitlab-ci.yml`).
