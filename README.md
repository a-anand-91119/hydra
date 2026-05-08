# Hydra

> One source, many mirrors — create a repo across self-hosted GitLab, GitLab.com, and GitHub in one shot, with push mirroring wired up automatically.

The self-hosted GitLab is the source of truth. Hydra creates the repo on all three hosts, then sets up push mirrors on the self-hosted project so pushes fan out to GitLab.com and GitHub.

## ⚠️ If you're forking from `repo-syncer`

The original `syncer.py` had API tokens committed in plaintext. **Revoke them immediately**:

- GitHub: https://github.com/settings/tokens
- GitLab.com: https://gitlab.com/-/profile/personal_access_tokens
- Your self-hosted GitLab: `<your-host>/-/profile/personal_access_tokens`

Hydra never writes tokens to the YAML config.

## Install

```sh
python -m venv venv && source venv/bin/activate
pip install -e .
hydra --version
```

## Configure

```sh
hydra configure
```

The wizard:
1. Writes non-secret settings to `~/.config/hydra/config.yaml`.
2. Stores API tokens in your OS keyring (macOS Keychain on Darwin, Secret Service on Linux).

To export tokens as shell `export` lines instead of using the keyring:

```sh
hydra configure --store env
```

### Token resolution order

For each host, Hydra looks for the token in this order:

1. OS keyring (`keyring get hydra <github|gitlab|self_hosted_gitlab>`)
2. Environment variable: `HYDRA_GITHUB_TOKEN`, `HYDRA_GITLAB_TOKEN`, `HYDRA_SELF_HOSTED_GITLAB_TOKEN`
3. `.env` file in the current working directory (see `.env.example`)
4. Interactive prompt (only if attached to a TTY)

## Usage

```sh
# Dry-run (no API calls)
hydra create my-repo --description "demo" --group platform/services --dry-run

# For real
hydra create my-repo -d "demo" -g platform/services

# Public repo, under a GitHub org, no mirrors
hydra create my-repo --public --github-org acme --no-mirror

# Inspect mirror status
hydra status my-repo --group platform/services
```

### Commands

| Command | Description |
| --- | --- |
| `hydra create <name>` | Create the repo on all three hosts and configure mirrors. |
| `hydra configure` | Interactive wizard for config + tokens. |
| `hydra status <name>` | Show the self-hosted project's mirror state. |
| `hydra config-path` | Print the resolved config file path. |

Run `hydra <cmd> --help` for full flags.

## Config file

Lives at `~/.config/hydra/config.yaml` by default. Override with `--config <path>` or `$HYDRA_CONFIG`. See `config.yaml.example`.

## Security notes

- Tokens are never written to the YAML config.
- Tokens injected into mirror URLs (`https://oauth2:<token>@host/...`) are stored on the self-hosted GitLab — be aware that anyone with project admin access can read them back via the API.
- Keep `.env` gitignored. It already is in this repo.
