"""Private run configuration: payer list, subject identity, market roster.

The subject is the organization whose own contracted rates a run tracks; any
other matched organization from the roster is "market". None of that belongs
in code. A configuration directory holds ``config.toml`` and the files it
names (payer list, subject identifiers, roster, reference amounts, extra CA
certificates), with paths relative to that directory.

The directory is ``MRF_CONFIG_DIR`` when set (the hosted scanner decrypts its
configuration there), otherwise this package's own directory.
"""
from __future__ import annotations

import os
import tomllib
from functools import lru_cache
from pathlib import Path

PACKAGE = Path(__file__).resolve().parent


class ConfigError(RuntimeError):
    pass


def directory() -> Path:
    value = os.environ.get('MRF_CONFIG_DIR')
    return Path(value).resolve() if value else PACKAGE


@lru_cache(maxsize=None)
def _load(root: Path) -> dict:
    path = root / 'config.toml'
    if not path.is_file():
        raise ConfigError(f'No config.toml in {root}; set MRF_CONFIG_DIR to the private configuration directory')
    return tomllib.loads(path.read_text())


def settings() -> dict:
    return _load(directory())


def path(key: str) -> Path | None:
    """Resolve a [paths] entry relative to the configuration directory (None when unset)."""
    value = settings().get('paths', {}).get(key)
    return (directory() / value).resolve() if value else None


def subject() -> dict:
    """role: value written to provider_role; name: used in reader-facing labels."""
    configured = settings().get('subject', {})
    return {'role': configured.get('role', 'subject'), 'name': configured.get('name', 'Subject organization'),
            'user_agent': configured.get('user_agent', 'tic-rate-scanner/1.0 (price transparency research)')}


def market_states() -> frozenset[str]:
    return frozenset(settings().get('market', {}).get('states', []))


def quiet_log() -> bool:
    """Public hosts print counts only; names, amounts and identifiers stay in the outputs."""
    return os.environ.get('MRF_QUIET_LOG') == '1'
