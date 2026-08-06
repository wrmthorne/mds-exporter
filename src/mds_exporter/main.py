from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any
import json
import asyncio
import sqlite3

import click
import httpx
import zstandard as zstd
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn

from .token_storage import add_token, list_tokens, remove_token, get_token, update_token


# The API is moving to api.museumdata.uk. Try the current host first and fall back to the new one
# so the tool keeps working either side of the switch.
EXTRACT_URLS = (
    "https://mds-data.ciim.k-int.com/api/v1/extract",
    "https://api.museumdata.uk/api/v1/extract",
)

DATA_SUFFIXES = {".json", ".jsonl", ".zst", ".zstd"}


def resolve_output_path(output_file: Path, compress: bool) -> Path:
    """Normalise an output path to .jsonl or .jsonl.zst, replacing any data suffixes already present."""
    while output_file.suffix in DATA_SUFFIXES:
        output_file = output_file.with_suffix("")
    suffix = ".jsonl.zst" if compress else ".jsonl"
    return output_file.with_name(output_file.name + suffix)


def write_data_batch(data: Iterable[Any], output_file: Path, compressor: zstd.ZstdCompressor | None) -> None:
    """Append a batch of records as JSON lines, compressing them when a compressor is given."""
    lines = "".join(json.dumps(item) + "\n" for item in data)
    if compressor is None:
        with open(output_file, "a") as f:
            f.write(lines)
    else:
        with open(output_file, "ab") as f:
            with compressor.stream_writer(f) as writer:
                writer.write(lines.encode())


async def fetch_extract(client: httpx.AsyncClient, resumption_token: str) -> dict[str, Any]:
    """Fetch the first batch, trying each known API host in turn."""
    last_error: httpx.HTTPError | None = None
    for url in EXTRACT_URLS:
        try:
            response = await client.get(url, params={"resume": resumption_token})
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as error:
            last_error = error

    raise click.ClickException(f"Could not reach the MDS extract API: {last_error}")


async def download_data(
    output_file: Path,
    resumption_token: str,
    token_name: str | None = None,
    compress: bool = False,
) -> None:
    if not resumption_token:
        raise ValueError("Resumption token is required")

    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file = resolve_output_path(output_file, compress)

    compressor = zstd.ZstdCompressor() if compress else None
    unsaved_resume: str | None = None

    try:
        async with httpx.AsyncClient(timeout=120) as client:
            resp_json = await fetch_extract(client, resumption_token)

            if not resp_json.get("found", True):
                raise click.ClickException(
                    "Token not found or expired. The API could not locate data for this resumption token."
                )

            stats = resp_json.get("stats", {})
            total = stats.get("total", 0)
            remaining = stats.get("remaining", 0)

            if total == 0:
                raise click.ClickException("No data available for this token.")

            # remaining excludes the batch it arrives with, so the first batch is not yet accounted for.
            completed = total - remaining - len(resp_json.get("data", []))

            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TextColumn("({task.completed}/{task.total})"),
                TimeRemainingColumn(),
            ) as progress:
                task = progress.add_task("Downloading", total=total, completed=completed)

                # Process all batches (initial + pagination)
                while True:
                    # Write current batch
                    data = resp_json.get("data", [])
                    write_data_batch(data, output_file, compressor)
                    progress.update(task, advance=len(data))

                    # Update token after each batch
                    if resp_json.get("resume") and token_name:
                        remaining = resp_json.get("stats", {}).get("remaining", 0)
                        try:
                            update_token(token_name, resp_json["resume"], remaining)
                            unsaved_resume = None
                        except sqlite3.Error as error:
                            # Losing the bookkeeping write is not worth abandoning a download of this length.
                            if unsaved_resume is None:
                                progress.console.print(f"[yellow]Could not record the resume token: {error}[/yellow]")
                            unsaved_resume = resp_json["resume"]

                    # Check if more pages exist
                    if not resp_json.get("has_next"):
                        break

                    # Fetch next page
                    response = await client.get(resp_json["next_url"])
                    response.raise_for_status()
                    resp_json = response.json()
    finally:
        if unsaved_resume:
            click.echo(f"Resume this extract with: --token {unsaved_resume}")


@click.group()
def main() -> None:
    """MDS Exporter - Manage tokens and download MDS data"""
    pass


@main.group()
def token() -> None:
    """Manage MDS API tokens"""
    pass


@token.command()
@click.option("--name", help="Optional name for the token (random name generated if not provided)")
@click.argument("mds_token")
def add(name: str | None, mds_token: str) -> None:
    """Add a new MDS token"""
    try:
        assigned_name = add_token(mds_token, name)
        click.echo(f"Added token '{assigned_name}'")
    except sqlite3.IntegrityError:
        click.echo(f"Token '{name}' already exists")


@token.command()
def list() -> None:
    """List all stored tokens"""
    list_tokens()


@token.command()
@click.argument("name")
def show(name: str) -> None:
    """Print a stored token in full, for copying elsewhere.

    Accepts name:version, defaulting to the 'last' version.
    """
    click.echo(get_token(name))


@token.command()
@click.argument("name")
def remove(name: str) -> None:
    """Remove a token by name"""
    remove_token(name)


EXTRACT_OPTIONS = (
    click.option("--name", help="Name of stored token (mutually exclusive with --token)"),
    click.option("--token", help="MDS API token (mutually exclusive with --name)"),
    click.option("--output", default="downloads.jsonl", help="Output JSONL file path"),
    click.option("--compress", is_flag=True, help="Compress output using zstd"),
)


def extract_options(command: Callable[..., Any]) -> Callable[..., Any]:
    for option in reversed(EXTRACT_OPTIONS):
        command = option(command)
    return command


def run_extract(name: str | None, token: str | None, output: str, compress: bool) -> None:
    if name and token:
        raise click.ClickException("Cannot specify both --name and --token")

    if name:
        resumption_token = get_token(name)
        token_name = name.split(":")[0]  # Extract base name for updates
    elif token:
        resumption_token = token
        token_name = None
    else:
        raise click.ClickException("Must specify either --name or --token")

    output_file = Path(output)
    asyncio.run(download_data(output_file, resumption_token, token_name, compress))


@main.command()
@extract_options
def extract(name: str | None, token: str | None, output: str, compress: bool) -> None:
    """Extract MDS data"""
    run_extract(name, token, output, compress)


@main.command(
    deprecated="`download` is deprecated and will be removed in a future version. Use 'mds extract' instead."
)
@extract_options
def download(name: str | None, token: str | None, output: str, compress: bool) -> None:
    """Deprecated alias for 'extract'"""
    run_extract(name, token, output, compress)
