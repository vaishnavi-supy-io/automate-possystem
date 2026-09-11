"""
tests/test_set_credential_atomic.py
-----------------------------------
Regression tests for the .env write path in set_credential.py.

On 11 Sep 2026 .env lost 27 of its 31 keys — every portal credential plus the
Gmail sender — and because .env is gitignored there was no copy to restore from
beyond a three-week-old manual backup. The old set_key() wrote straight onto
the live file with write_text(), which truncates before it writes.

These tests pin the properties that make that unrecoverable again:
a failed write must leave the original intact, and a write must never
shrink the file to nothing.

No credentials, no network. Values used here are dummies.

Run:
    /path/to/.venv/bin/python -m pytest tests/test_set_credential_atomic.py -v
"""

import os
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

import set_credential as sc  # noqa: E402

EXISTING = "ALPHA_USERNAME=one\nALPHA_PASSWORD=two\nBETA_USERNAME=three\n"


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Point set_credential at a throwaway .env."""
    path = tmp_path / ".env"
    path.write_text(EXISTING)
    monkeypatch.setattr(sc, "ENV_PATH", path)
    return path


def keys(path):
    return [l.split("=", 1)[0] for l in path.read_text().splitlines()
            if "=" in l and not l.startswith("#")]


# ── Normal operation ─────────────────────────────────────────────────────────

def test_adding_a_key_preserves_every_existing_key(env):
    assert sc.set_key("GAMMA_TOKEN", "four") == "added"

    assert keys(env) == ["ALPHA_USERNAME", "ALPHA_PASSWORD",
                         "BETA_USERNAME", "GAMMA_TOKEN"]


def test_updating_a_key_replaces_it_in_place(env):
    assert sc.set_key("ALPHA_PASSWORD", "changed") == "updated"

    assert keys(env) == ["ALPHA_USERNAME", "ALPHA_PASSWORD", "BETA_USERNAME"]
    assert "ALPHA_PASSWORD=changed" in env.read_text()


def test_previous_contents_are_backed_up(env):
    sc.set_key("GAMMA_TOKEN", "four")

    backups = list(env.parent.glob(".env.bak.*"))
    assert len(backups) == 1
    assert backups[0].read_text() == EXISTING


def test_env_stays_owner_only(env):
    sc.set_key("GAMMA_TOKEN", "four")

    assert oct(env.stat().st_mode)[-3:] == "600"


def test_no_temp_files_are_left_behind(env):
    sc.set_key("GAMMA_TOKEN", "four")

    assert list(env.parent.glob("*.tmp")) == []


# ── Failure modes — the ones that cost us the file ───────────────────────────

def test_a_failed_write_leaves_the_original_untouched(env, monkeypatch):
    """The whole point of the rename: a crash mid-write must not truncate .env."""
    def boom(*_a, **_kw):
        raise OSError("disk full")

    monkeypatch.setattr(sc.os, "replace", boom)

    with pytest.raises(OSError):
        sc.set_key("GAMMA_TOKEN", "four")

    assert env.read_text() == EXISTING          # nothing lost
    assert list(env.parent.glob("*.tmp")) == []  # nothing left behind


def test_a_short_read_is_refused_rather_than_written(env, monkeypatch):
    """
    The exact shape of the 11 Sep loss: the file is still on disk with all its
    keys, but it reads back as nothing. Writing that result would keep only the
    key being set and drop every other credential.
    """
    monkeypatch.setattr(sc, "read_env_lines", lambda: [])

    with pytest.raises(RuntimeError, match="read back as empty"):
        sc.set_key("GAMMA_TOKEN", "four")

    assert env.read_text() == EXISTING   # every credential still there


def test_write_helper_refuses_to_leave_env_shorter_than_a_single_line(env):
    """_write_env_atomically is the only writer — exercise it directly."""
    sc._write_env_atomically("ALPHA_USERNAME=one\n")

    assert env.read_text() == "ALPHA_USERNAME=one\n"
    backups = list(env.parent.glob(".env.bak.*"))
    assert backups and backups[0].read_text() == EXISTING


def test_backup_is_skipped_when_there_is_nothing_to_back_up(tmp_path, monkeypatch):
    path = tmp_path / ".env"
    monkeypatch.setattr(sc, "ENV_PATH", path)

    sc.set_key("ALPHA_USERNAME", "one")

    assert path.read_text() == "ALPHA_USERNAME=one\n"
    assert list(tmp_path.glob(".env.bak.*")) == []
