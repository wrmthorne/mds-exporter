import sqlite3
import random
from pathlib import Path

import click
from rich.table import Table
from rich.console import Console


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
    """Generate a random name in the format 'adjective-noun'."""
    return f"{random.choice(ADJECTIVES)}-{random.choice(NOUNS)}"


def get_db_path():
    """Get the path to the token database file."""
    home = Path.home()
    db_dir = home / ".local" / "share" / "mds-exporter"
    db_dir.mkdir(parents=True, exist_ok=True)
    return db_dir / "tokens.db"


def init_db():
    """Initialize the token database with the required schema."""
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
    """Add a new token to the database with optional custom name."""
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
    """List all stored tokens in a formatted table."""
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
    """Remove a token from the database by name."""
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
    """Get a token by name and version (name:version format supported)."""
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
    """Update a token's last and potentially latest values based on remaining count."""
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
