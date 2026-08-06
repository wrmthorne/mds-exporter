import asyncio
import json
from pathlib import Path
from typing import Any

import click
import httpx
import pytest
import zstandard as zstd
from click.testing import CliRunner

from mds_exporter import token_storage
from mds_exporter.main import (
    EXTRACT_URLS,
    download_data,
    fetch_extract,
    main,
    resolve_output_path,
    write_data_batch,
)

CURRENT_URL, NEW_URL = EXTRACT_URLS

# Held aside so make_client still works once httpx.AsyncClient has been monkeypatched.
AsyncClient = httpx.AsyncClient


def read_zst(path: Path) -> str:
    with open(path, "rb") as f:
        return zstd.ZstdDecompressor().stream_reader(f, read_across_frames=True).read().decode()


def make_client(handler: Any) -> httpx.AsyncClient:
    return AsyncClient(transport=httpx.MockTransport(handler))


def extract_page(records: list[Any], **extra: Any) -> dict[str, Any]:
    page: dict[str, Any] = {
        "found": True,
        "data": records,
        "stats": {"total": len(records), "remaining": 0},
        "has_next": False,
    }
    page.update(extra)
    return page


class TestResolveOutputPath:
    @pytest.mark.parametrize(
        ("given", "expected"),
        [
            ("downloads", "downloads.jsonl"),
            ("downloads.jsonl", "downloads.jsonl"),
            ("downloads.json", "downloads.jsonl"),
            ("out/my.data", "out/my.data.jsonl"),
        ],
    )
    def test_uncompressed(self, given: str, expected: str) -> None:
        assert resolve_output_path(Path(given), compress=False) == Path(expected)

    @pytest.mark.parametrize(
        ("given", "expected"),
        [
            ("downloads", "downloads.jsonl.zst"),
            ("downloads.jsonl", "downloads.jsonl.zst"),
            ("downloads.zst", "downloads.jsonl.zst"),
            ("downloads.zstd", "downloads.jsonl.zst"),
            ("downloads.jsonl.zstd", "downloads.jsonl.zst"),
            ("out/my.data", "out/my.data.jsonl.zst"),
        ],
    )
    def test_compressed(self, given: str, expected: str) -> None:
        assert resolve_output_path(Path(given), compress=True) == Path(expected)

    def test_is_idempotent(self) -> None:
        once = resolve_output_path(Path("downloads"), compress=True)
        assert resolve_output_path(once, compress=True) == once


class TestWriteDataBatch:
    def test_writes_json_lines(self, tmp_path: Path) -> None:
        output = tmp_path / "out.jsonl"
        write_data_batch([{"id": 1}, {"id": 2}], output, None)
        assert output.read_text() == '{"id": 1}\n{"id": 2}\n'

    def test_appends_successive_batches(self, tmp_path: Path) -> None:
        output = tmp_path / "out.jsonl"
        write_data_batch([{"id": 1}], output, None)
        write_data_batch([{"id": 2}], output, None)
        assert output.read_text() == '{"id": 1}\n{"id": 2}\n'

    def test_compressed_round_trip(self, tmp_path: Path) -> None:
        output = tmp_path / "out.jsonl.zst"
        compressor = zstd.ZstdCompressor()
        write_data_batch([{"id": 1}], output, compressor)
        write_data_batch([{"id": 2}], output, compressor)
        assert read_zst(output) == '{"id": 1}\n{"id": 2}\n'

    def test_empty_batch_writes_nothing(self, tmp_path: Path) -> None:
        output = tmp_path / "out.jsonl"
        write_data_batch([], output, None)
        assert output.read_text() == ""


class TestFetchExtract:
    def test_uses_current_url_when_available(self) -> None:
        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(str(request.url.copy_with(query=None)))
            return httpx.Response(200, json=extract_page([{"id": 1}]))

        result = asyncio.run(fetch_extract(make_client(handler), "abc123"))

        assert seen == [CURRENT_URL]
        assert result["data"] == [{"id": 1}]

    @pytest.mark.parametrize("failure", [httpx.ConnectError("down"), httpx.Response(503)])
    def test_falls_back_to_new_url(self, failure: Any) -> None:
        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url.copy_with(query=None))
            seen.append(url)
            if url == CURRENT_URL:
                if isinstance(failure, Exception):
                    raise failure
                return failure
            return httpx.Response(200, json=extract_page([{"id": 1}]))

        result = asyncio.run(fetch_extract(make_client(handler), "abc123"))

        assert seen == [CURRENT_URL, NEW_URL]
        assert result["data"] == [{"id": 1}]

    def test_passes_resumption_token_as_query_param(self) -> None:
        seen: list[str | None] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request.url.params.get("resume"))
            return httpx.Response(200, json=extract_page([]))

        asyncio.run(fetch_extract(make_client(handler), "abc123"))
        assert seen == ["abc123"]

    def test_raises_when_both_urls_fail(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500)

        with pytest.raises(click.ClickException, match="Could not reach the MDS extract API"):
            asyncio.run(fetch_extract(make_client(handler), "abc123"))


class TestDownloadData:
    @staticmethod
    def install_transport(monkeypatch: pytest.MonkeyPatch, handler: Any) -> None:
        monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: make_client(handler))

    def test_writes_all_pages(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        next_url = f"{CURRENT_URL}?page=2"

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.params.get("page") == "2":
                return httpx.Response(200, json=extract_page([{"id": 2}]))
            page = extract_page([{"id": 1}])
            page["stats"] = {"total": 2, "remaining": 1}
            page["has_next"] = True
            page["next_url"] = next_url
            return httpx.Response(200, json=page)

        self.install_transport(monkeypatch, handler)
        output = tmp_path / "nested" / "out"
        asyncio.run(download_data(output, "abc123"))

        written = (tmp_path / "nested" / "out.jsonl").read_text()
        assert [json.loads(line) for line in written.splitlines()] == [{"id": 1}, {"id": 2}]

    def test_writes_compressed_output(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self.install_transport(monkeypatch, lambda request: httpx.Response(200, json=extract_page([{"id": 1}])))
        asyncio.run(download_data(tmp_path / "out", "abc123", compress=True))

        assert read_zst(tmp_path / "out.jsonl.zst") == '{"id": 1}\n'
        assert not (tmp_path / "out.zstd").exists()

    def test_updates_stored_token_after_each_page(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        token_storage.add_token("abc123", "my-token")
        page = extract_page([{"id": 1}], resume="def456")
        self.install_transport(monkeypatch, lambda request: httpx.Response(200, json=page))

        asyncio.run(download_data(tmp_path / "out", "abc123", token_name="my-token"))

        assert token_storage.get_token("my-token:last") == "def456"
        assert token_storage.get_token("my-token:base") == "abc123"

    def test_continues_when_the_token_database_is_locked(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        token_storage.add_token("abc123", "my-token")
        page = extract_page([{"id": 1}], resume="def456")
        self.install_transport(monkeypatch, lambda request: httpx.Response(200, json=page))

        monkeypatch.setattr(token_storage, "BUSY_TIMEOUT_SECONDS", 0)

        with token_storage.connect() as blocker:
            blocker.execute("BEGIN EXCLUSIVE")
            asyncio.run(download_data(tmp_path / "out", "abc123", token_name="my-token"))

        assert (tmp_path / "out.jsonl").read_text() == '{"id": 1}\n'
        assert "def456" in capsys.readouterr().out

    def test_rejects_empty_resumption_token(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="Resumption token is required"):
            asyncio.run(download_data(tmp_path / "out", ""))

    def test_reports_expired_token(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self.install_transport(monkeypatch, lambda request: httpx.Response(200, json={"found": False}))
        with pytest.raises(click.ClickException, match="Token not found or expired"):
            asyncio.run(download_data(tmp_path / "out", "abc123"))

    def test_reports_empty_dataset(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self.install_transport(monkeypatch, lambda request: httpx.Response(200, json=extract_page([])))
        with pytest.raises(click.ClickException, match="No data available"):
            asyncio.run(download_data(tmp_path / "out", "abc123"))


class TestTokenCommands:
    def test_add_reports_assigned_name(self) -> None:
        result = CliRunner().invoke(main, ["token", "add", "--name", "my-token", "abc123"])
        assert result.exit_code == 0
        assert "Added token 'my-token'" in result.output

    def test_add_reports_duplicate_name(self) -> None:
        runner = CliRunner()
        runner.invoke(main, ["token", "add", "--name", "my-token", "abc123"])
        result = runner.invoke(main, ["token", "add", "--name", "my-token", "def456"])
        assert result.exit_code == 0
        assert "already exists" in result.output

    def test_show_prints_token_in_full(self) -> None:
        secret = "x" * 60
        runner = CliRunner()
        runner.invoke(main, ["token", "add", "--name", "my-token", secret])
        result = runner.invoke(main, ["token", "show", "my-token"])
        assert result.exit_code == 0
        assert result.output.strip() == secret

    def test_show_accepts_a_version(self) -> None:
        runner = CliRunner()
        runner.invoke(main, ["token", "add", "--name", "my-token", "abc123"])
        token_storage.update_token("my-token", "def456", remaining=1)

        assert runner.invoke(main, ["token", "show", "my-token:base"]).output.strip() == "abc123"
        assert runner.invoke(main, ["token", "show", "my-token:last"]).output.strip() == "def456"

    def test_show_fails_for_unknown_token(self) -> None:
        result = CliRunner().invoke(main, ["token", "show", "no-such-token"])
        assert result.exit_code != 0
        assert "not found" in result.output

    def test_remove(self) -> None:
        runner = CliRunner()
        runner.invoke(main, ["token", "add", "--name", "my-token", "abc123"])
        result = runner.invoke(main, ["token", "remove", "my-token"])
        assert "Removed token 'my-token'" in result.output


class TestExtractCommand:
    @pytest.mark.parametrize("command", ["extract", "download"])
    def test_requires_a_token_source(self, command: str) -> None:
        result = CliRunner().invoke(main, [command])
        assert result.exit_code != 0
        assert "Must specify either --name or --token" in result.output

    @pytest.mark.parametrize("command", ["extract", "download"])
    def test_rejects_both_token_sources(self, command: str) -> None:
        result = CliRunner().invoke(main, [command, "--name", "my-token", "--token", "abc123"])
        assert result.exit_code != 0
        assert "Cannot specify both --name and --token" in result.output

    @pytest.mark.parametrize("command", ["extract", "download"])
    def test_extracts_with_a_direct_token(self, command: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        TestDownloadData.install_transport(
            monkeypatch, lambda request: httpx.Response(200, json=extract_page([{"id": 1}]))
        )
        output = tmp_path / "out"
        result = CliRunner().invoke(main, [command, "--token", "abc123", "--output", str(output)])

        assert result.exit_code == 0
        assert (tmp_path / "out.jsonl").read_text() == '{"id": 1}\n'

    def test_download_warns_that_it_is_deprecated(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        TestDownloadData.install_transport(
            monkeypatch, lambda request: httpx.Response(200, json=extract_page([{"id": 1}]))
        )
        runner = CliRunner()
        result = runner.invoke(main, ["download", "--token", "abc123", "--output", str(tmp_path / "out")])

        assert result.exit_code == 0
        assert "deprecated" in result.output.lower()
        assert "mds extract" in result.output

    def test_extract_does_not_warn(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        TestDownloadData.install_transport(
            monkeypatch, lambda request: httpx.Response(200, json=extract_page([{"id": 1}]))
        )
        result = CliRunner().invoke(main, ["extract", "--token", "abc123", "--output", str(tmp_path / "out")])

        assert result.exit_code == 0
        assert "deprecated" not in result.output.lower()

    def test_both_commands_are_listed_in_help(self) -> None:
        output = CliRunner().invoke(main, ["--help"]).output
        assert "extract" in output
        assert "download" in output
