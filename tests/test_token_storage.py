import sqlite3
from pathlib import Path

import click
import pytest

from mds_exporter import token_storage


def test_generate_name_uses_known_words() -> None:
    adjective, noun = token_storage.generate_name().split("-")
    assert adjective in token_storage.ADJECTIVES
    assert noun in token_storage.NOUNS


def test_get_db_path_creates_directory(isolated_home: Path) -> None:
    db_path = token_storage.get_db_path()
    assert db_path == isolated_home / ".local" / "share" / "mds-exporter" / "tokens.db"
    assert db_path.parent.is_dir()


def test_add_token_with_explicit_name() -> None:
    assert token_storage.add_token("abc123", "my-token") == "my-token"
    assert token_storage.get_token("my-token") == "abc123"


def test_add_token_generates_name_when_omitted() -> None:
    name = token_storage.add_token("abc123")
    adjective, noun = name.split("-")
    assert adjective in token_storage.ADJECTIVES
    assert noun in token_storage.NOUNS


def test_add_token_seeds_all_versions() -> None:
    token_storage.add_token("abc123", "my-token")
    assert token_storage.get_token("my-token:base") == "abc123"
    assert token_storage.get_token("my-token:last") == "abc123"
    assert token_storage.get_token("my-token:latest") == "abc123"


def test_add_token_rejects_duplicate_name() -> None:
    token_storage.add_token("abc123", "my-token")
    with pytest.raises(sqlite3.IntegrityError):
        token_storage.add_token("def456", "my-token")


def test_get_token_defaults_to_last_version() -> None:
    token_storage.add_token("abc123", "my-token")
    token_storage.update_token("my-token", "def456", remaining=10)
    assert token_storage.get_token("my-token") == "def456"


def test_get_token_rejects_unknown_version() -> None:
    token_storage.add_token("abc123", "my-token")
    with pytest.raises(click.ClickException, match="Invalid version"):
        token_storage.get_token("my-token:newest")


def test_get_token_rejects_unknown_name() -> None:
    with pytest.raises(click.ClickException, match="not found"):
        token_storage.get_token("no-such-token")


def test_update_token_advances_latest_when_fewer_remain() -> None:
    token_storage.add_token("abc123", "my-token")
    token_storage.update_token("my-token", "def456", remaining=10)
    assert token_storage.get_token("my-token:latest") == "def456"

    token_storage.update_token("my-token", "ghi789", remaining=5)
    assert token_storage.get_token("my-token:latest") == "ghi789"


def test_update_token_keeps_latest_when_more_remain() -> None:
    token_storage.add_token("abc123", "my-token")
    token_storage.update_token("my-token", "def456", remaining=5)
    token_storage.update_token("my-token", "ghi789", remaining=50)

    assert token_storage.get_token("my-token:latest") == "def456"
    assert token_storage.get_token("my-token:last") == "ghi789"


def test_update_token_never_changes_base() -> None:
    token_storage.add_token("abc123", "my-token")
    token_storage.update_token("my-token", "def456", remaining=1)
    assert token_storage.get_token("my-token:base") == "abc123"


def test_remove_token() -> None:
    token_storage.add_token("abc123", "my-token")
    token_storage.remove_token("my-token")
    with pytest.raises(click.ClickException, match="not found"):
        token_storage.get_token("my-token")


def test_remove_unknown_token_reports_rather_than_raising(capsys: pytest.CaptureFixture[str]) -> None:
    token_storage.remove_token("no-such-token")
    assert "not found" in capsys.readouterr().out


def test_list_tokens_shows_names_and_truncates_secrets(capsys: pytest.CaptureFixture[str]) -> None:
    token_storage.add_token("x" * 60, "my-token")
    token_storage.list_tokens()
    output = capsys.readouterr().out
    assert "my-token" in output
    assert "x" * 60 not in output
