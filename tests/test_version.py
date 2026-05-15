"""hydra.__version__ must be sourced from the installed package metadata,
not a hardcoded literal that drifts from pyproject.toml between releases.
"""

from importlib.metadata import version

import hydra


def test_version_matches_installed_metadata():
    assert hydra.__version__ == version("hydra-repo-syncer")
