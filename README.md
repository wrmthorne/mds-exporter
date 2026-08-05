# MDS Exporter

A simple CLI tool, written in Python, to manage [Museum Data Service](https://museumdata.uk/) (MDS) resume API tokens and download data from the MDS API.

## Installation

Install with [uv](https://docs.astral.sh/uv/). This makes the `mds` command available system-wide:

```bash
uv tool install mds-exporter
```

To run it once without installing anything:

```bash
uvx mds-exporter --help
```

To add it to an existing project instead:

```bash
uv add mds-exporter
```

Or with pip:

```bash
pip install mds-exporter
```

## Usage

### Token Versions

Each stored token keeps track of three versions to help you pick up where you left off:

`base`: Your original starting token (never changes) - lets you restart the whole download from scratch if needed <br/>
`last`: Whatever token was used most recently - keeps track of where you are in any current download <br/>
`latest`: The token from your most complete download (the one that got the farthest) <br/>

This setup gives you options when things go wrong or go missing. You can always use `base` to redownload the whole dataset from the start. If a download gets interrupted, `last` lets you resume right where you stopped. And `latest` stays safe and untouched, so you can always go back to refreshing your best/most complete dataset without messing up your current download progress.


### Token Management

Store and manage MDS API tokens:

```bash
# Add a token with auto-generated name (e.g., "ancient-river")
mds token add YOUR_MDS_TOKEN

# Add a token with custom name
mds token add --name my-token YOUR_MDS_TOKEN

# List all stored tokens
mds token list

# Print a token in full so it can be copied (defaults to the 'last' version)
mds token show my-token
mds token show my-token:base

# Remove a token
mds token remove my-token
```

`mds token list` truncates tokens so they stay readable in a table. Use `mds token show` when you need the whole value.

### Extracting Data

Extract MDS data using stored tokens or direct tokens:

```bash
# Extract using stored token (uses 'last' version by default)
mds extract --name my-token

# Extract using specific token version
mds extract --name my-token:latest
mds extract --name my-token:base

# Extract using direct token
mds extract --token YOUR_MDS_TOKEN

# Specify output file
mds extract --name my-token --output my-data.jsonl

# Compress output using zstd (recommended for large datasets)
mds extract --name my-token --compress --output my-data
```

The extension on `--output` is normalised for you. Data is written as `.jsonl`, or `.jsonl.zst` with `--compress`.

`mds download` still works as an alias but is deprecated and prints a warning. Use `mds extract`.

## Development

```bash
uv sync
uv run pre-commit install
```

`pre-commit` runs ruff, [pyrefly](https://pyrefly.org/) type checking, and the pytest suite. To run any of them on their own:

```bash
uv run pytest
uv run pyrefly check
uv run pre-commit run --all-files
```

Or with pip, using the exported `requirements.txt`:

```bash
pip install -r requirements.txt
pip install -e . --no-deps
pytest
pyrefly check
```
