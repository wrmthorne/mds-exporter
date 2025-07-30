from pathlib import Path
import json
import asyncio
import sqlite3
import random

import click
import httpx
import zstandard as zstd
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.table import Table
from rich.console import Console


EXTRACT_URL = "https://mds-data-1.ciim.k-int.com/api/v1/extract"

ADJECTIVES = [
    "aged",
    "ancient",
    "autumn",
    "billowing",
    "bitter",
    "black",
    "blue",
    "bold",
    "broad",
    "broken",
    "calm",
    "cold",
    "cool",
    "crimson",
    "curly",
    "damp",
    "dark",
    "dawn",
    "delicate",
    "divine",
    "dry",
    "empty",
    "falling",
    "fancy",
    "flat",
    "floral",
    "fragrant",
    "frosty",
    "gentle",
    "green",
    "hidden",
    "holy",
    "icy",
    "jolly",
    "late",
    "lingering",
    "little",
    "lively",
    "long",
    "lucky",
    "misty",
    "morning",
    "muddy",
    "old",
    "orange",
    "patient",
    "plain",
    "polished",
    "proud",
    "purple",
    "quiet",
    "rapid",
    "red",
    "restless",
    "rough",
    "round",
    "royal",
    "shiny",
    "shy",
    "silent",
    "small",
    "snowy",
    "soft",
    "solitary",
    "sparkling",
    "spring",
    "square",
    "steep",
    "still",
    "summer",
    "super",
    "sweet",
    "throbbing",
    "tight",
    "tiny",
    "twilight",
    "wandering",
    "weathered",
    "white",
    "wild",
    "winter",
    "wispy",
    "withered",
    "yellow",
    "young",
]

NOUNS = [
    "art",
    "band",
    "bar",
    "base",
    "bird",
    "block",
    "boat",
    "box",
    "bread",
    "breeze",
    "brook",
    "bush",
    "butterfly",
    "cake",
    "cell",
    "cherry",
    "cloud",
    "credit",
    "darkness",
    "dawn",
    "dew",
    "disk",
    "dream",
    "dust",
    "feather",
    "field",
    "fire",
    "firefly",
    "flower",
    "fog",
    "forest",
    "frog",
    "frost",
    "glade",
    "glitter",
    "grass",
    "hall",
    "hat",
    "haze",
    "heart",
    "hill",
    "king",
    "lab",
    "lake",
    "leaf",
    "limit",
    "math",
    "meadow",
    "mode",
    "moon",
    "morning",
    "mountain",
    "mouse",
    "mud",
    "night",
    "paper",
    "pine",
    "poetry",
    "pond",
    "queen",
    "rain",
    "recipe",
    "resonance",
    "rice",
    "river",
    "salad",
    "scene",
    "sea",
    "shadow",
    "shape",
    "silence",
    "sky",
    "smoke",
    "snow",
    "snowflake",
    "sound",
    "star",
    "sun",
    "sunset",
    "surf",
    "term",
    "thunder",
    "tooth",
    "tree",
    "truth",
    "union",
    "unit",
    "violet",
    "voice",
    "water",
    "waterfall",
    "wave",
    "wildflower",
    "wind",
    "wood",
]


def generate_name():
    return f"{random.choice(ADJECTIVES)}-{random.choice(NOUNS)}"


def get_db_path():
    home = Path.home()
    db_dir = home / ".local" / "share" / "mds-exporter"
    db_dir.mkdir(parents=True, exist_ok=True)
    return db_dir / "tokens.db"


def init_db():
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tokens (
            name TEXT PRIMARY KEY,
            base TEXT NOT NULL,
            last TEXT NOT NULL,
            latest TEXT NOT NULL,
            least_remaining INTEGER NOT NULL
        )
    """)
    conn.commit()
    conn.close()


def add_token(token: str, name: str = None):
    init_db()
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)

    if not name:
        # Generate a unique name
        while True:
            name = generate_name()
            cursor = conn.execute("SELECT name FROM tokens WHERE name = ?", (name,))
            if not cursor.fetchone():
                break

    try:
        conn.execute(
            "INSERT INTO tokens (name, base, last, latest, least_remaining) VALUES (?, ?, ?, ?, ?)",
            (name, token, token, token, float("inf")),
        )
        conn.commit()
        return name
    finally:
        conn.close()


def list_tokens():
    init_db()
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            "SELECT name, base, last, latest, least_remaining FROM tokens"
        )
        rows = cursor.fetchall()

        console = Console()
        table = Table()
        table.add_column("Name")
        table.add_column("Base")
        table.add_column("Last")
        table.add_column("Latest")
        table.add_column("Least Remaining")

        for row in rows:
            table.add_row(
                row[0],
                row[1][:20] + "...",
                row[2][:20] + "...",
                row[3][:20] + "...",
                str(row[4]),
            )

        console.print(table)
    finally:
        conn.close()


def remove_token(name: str):
    init_db()
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute("DELETE FROM tokens WHERE name = ?", (name,))
        if cursor.rowcount == 0:
            click.echo(f"Token '{name}' not found")
        else:
            click.echo(f"Removed token '{name}'")
        conn.commit()
    finally:
        conn.close()


def get_token(name_spec: str):
    init_db()
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    try:
        if ":" in name_spec:
            name, version = name_spec.split(":", 1)
            if version == "base":
                cursor = conn.execute("SELECT base FROM tokens WHERE name = ?", (name,))
            elif version == "last":
                cursor = conn.execute("SELECT last FROM tokens WHERE name = ?", (name,))
            elif version == "latest":
                cursor = conn.execute(
                    "SELECT latest FROM tokens WHERE name = ?", (name,)
                )
            else:
                raise click.ClickException(
                    f"Invalid version '{version}'. Use base, last, or latest"
                )
        else:
            cursor = conn.execute(
                "SELECT last FROM tokens WHERE name = ?", (name_spec,)
            )

        row = cursor.fetchone()
        if not row:
            raise click.ClickException(f"Token '{name_spec}' not found")
        return row[0]
    finally:
        conn.close()


def update_token(name: str, new_token: str, remaining: int):
    db_path = get_db_path()
    conn = sqlite3.connect(db_path)
    try:
        # Update last token
        conn.execute("UPDATE tokens SET last = ? WHERE name = ?", (new_token, name))

        # Update latest if this has fewer remaining
        cursor = conn.execute(
            "SELECT least_remaining FROM tokens WHERE name = ?", (name,)
        )
        row = cursor.fetchone()
        if row and remaining < row[0]:
            conn.execute(
                "UPDATE tokens SET latest = ?, least_remaining = ? WHERE name = ?",
                (new_token, remaining, name),
            )

        conn.commit()
    finally:
        conn.close()


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

            # Write initial batch
            data = resp_json.get("data", [])
            if compress:
                with open(output_file, "ab") as f:
                    with compressor.stream_writer(f) as writer:
                        for item in data:
                            writer.write((json.dumps(item) + "\n").encode())
            else:
                with open(output_file, "a") as f:
                    for item in data:
                        f.write(json.dumps(item) + "\n")
            progress.update(task, advance=len(data))

            # Update token after initial batch
            if resp_json.get("resume") and token_name:
                remaining = resp_json.get("stats", {}).get("remaining", 0)
                update_token(token_name, resp_json.get("resume"), remaining)

            # Continue with sequential pagination
            if compress:
                with open(output_file, "ab") as f:
                    with compressor.stream_writer(f) as writer:
                        while resp_json.get("has_next"):
                            response = await client.get(resp_json.get("next_url"))
                            response.raise_for_status()

                            resp_json = response.json()
                            data = resp_json.get("data", [])
                            for item in data:
                                writer.write((json.dumps(item) + "\n").encode())
                            progress.update(task, advance=len(data))

                            # Update token after each batch
                            if resp_json.get("resume") and token_name:
                                remaining = resp_json.get("stats", {}).get(
                                    "remaining", 0
                                )
                                update_token(
                                    token_name, resp_json.get("resume"), remaining
                                )
            else:
                with open(output_file, "a") as f:
                    while resp_json.get("has_next"):
                        response = await client.get(resp_json.get("next_url"))
                        response.raise_for_status()

                        resp_json = response.json()
                        data = resp_json.get("data", [])
                        for item in data:
                            f.write(json.dumps(item) + "\n")
                        progress.update(task, advance=len(data))

                        # Update token after each batch
                        if resp_json.get("resume") and token_name:
                            remaining = resp_json.get("stats", {}).get("remaining", 0)
                            update_token(token_name, resp_json.get("resume"), remaining)


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
