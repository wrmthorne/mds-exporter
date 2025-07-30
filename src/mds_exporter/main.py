from pathlib import Path
import json
import asyncio
import sqlite3

import click
import httpx
import zstandard as zstd
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn

try:
    from .token_storage import (
        add_token,
        list_tokens,
        remove_token,
        get_token,
        update_token,
    )
except ImportError:
    from token_storage import (
        add_token,
        list_tokens,
        remove_token,
        get_token,
        update_token,
    )


EXTRACT_URL = "https://mds-data-1.ciim.k-int.com/api/v1/extract"


def write_data_batch(data, output_file, compress, compressor):
    """Write a batch of data to file, handling both compressed and uncompressed formats."""
    if compress:
        with open(output_file, "ab") as f:
            with compressor.stream_writer(f) as writer:
                for item in data:
                    writer.write((json.dumps(item) + "\n").encode())
    else:
        with open(output_file, "a") as f:
            for item in data:
                f.write(json.dumps(item) + "\n")


async def download_data(
    output_file: Path,
    resumption_token: str,
    token_name: str = None,
    compress: bool = False,
):
    if not resumption_token:
        raise ValueError("Resumption token is required")

    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Set file extension based on compression
    if compress:
        output_file = output_file.with_suffix(".zstd")
    else:
        output_file = output_file.with_suffix(".jsonl")

    # Create compressor if needed
    compressor = zstd.ZstdCompressor() if compress else None

    async with httpx.AsyncClient() as client:
        response = await client.get(EXTRACT_URL, params={"resume": resumption_token})
        response.raise_for_status()

        resp_json = response.json()
        stats = resp_json.get("stats", {})
        total = stats.get("total", 0)
        completed = total - stats.get("remaining", 0)

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
                write_data_batch(data, output_file, compress, compressor)
                progress.update(task, advance=len(data))

                # Update token after each batch
                if resp_json.get("resume") and token_name:
                    remaining = resp_json.get("stats", {}).get("remaining", 0)
                    update_token(token_name, resp_json.get("resume"), remaining)

                # Check if more pages exist
                if not resp_json.get("has_next"):
                    break

                # Fetch next page
                response = await client.get(resp_json.get("next_url"))
                response.raise_for_status()
                resp_json = response.json()


@click.group()
def main():
    """MDS Exporter - Manage tokens and download MDS data"""
    pass


@main.group()
def token():
    """Manage MDS API tokens"""
    pass


@token.command()
@click.option(
    "--name", help="Optional name for the token (random name generated if not provided)"
)
@click.argument("mds_token")
def add(name, mds_token):
    """Add a new MDS token"""
    try:
        assigned_name = add_token(mds_token, name)
        click.echo(f"Added token '{assigned_name}'")
    except sqlite3.IntegrityError:
        click.echo(f"Token '{name}' already exists")


@token.command()
def list():
    """List all stored tokens"""
    list_tokens()


@token.command()
@click.argument("name")
def remove(name):
    """Remove a token by name"""
    remove_token(name)


@main.command()
@click.option("--name", help="Name of stored token (mutually exclusive with --token)")
@click.option("--token", help="MDS API token (mutually exclusive with --name)")
@click.option("--output", default="downloads.jsonl", help="Output JSONL file path")
@click.option("--compress", is_flag=True, help="Compress output using zstd")
def download(name, token, output, compress):
    """Download MDS data"""
    if not name and not token:
        raise click.ClickException("Must specify either --name or --token")
    if name and token:
        raise click.ClickException("Cannot specify both --name and --token")

    if name:
        resumption_token = get_token(name)
        token_name = name.split(":")[0]  # Extract base name for updates
    else:
        resumption_token = token
        token_name = None

    output_file = Path(output)
    asyncio.run(download_data(output_file, resumption_token, token_name, compress))


if __name__ == "__main__":
    main()
