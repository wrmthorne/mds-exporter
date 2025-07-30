# MDS Exporter

A simple CLI tool, written in Python, to manage MDS resume tokens and download data from the MDS API.

## Installation

The recommended way to install the tool is to use the `pipx` package manager. This will make the `mds-exporter` command available system-wide.

```bash
pipx install mds-exporter
```

It can also be installed to a specific environment:

```bash
pip install mds-exporter
```

## Usage

### Token Versions

Each stored token keeps track of three versions to help you pick up where you left off:

`base`: Your original starting token (never changes) - lets you restart the whole download from scratch if needed <br/>
`last`: Whatever token was used most recently - keeps track of where you are in any current download <br/>
`latest`: The token from your most complete download (the one that got the farthest) <br/>

This setup gives you options when things go wrong. You can always use `base` to start over completely. If a download gets interrupted, `last` lets you resume right where you stopped. And `latest` stays safe and untouched, so you can always go back to refreshing your best/most complete dataset without messing up your current download progress.


### Token Management

Store and manage MDS API tokens:

```bash
# Add a token with auto-generated name (e.g., "ancient-river")
mds token add YOUR_MDS_TOKEN

# Add a token with custom name
mds token add --name my-token YOUR_MDS_TOKEN

# List all stored tokens
mds token list

# Remove a token
mds token remove my-token
```

### Downloading Data

Download MDS data using stored tokens or direct tokens:

```bash
# Download using stored token (uses 'last' version by default)
mds download --name my-token

# Download using specific token version
mds download --name my-token:latest
mds download --name my-token:base

# Download using direct token
mds download --token YOUR_MDS_TOKEN

# Specify output file
mds download --name my-token --output my-data.jsonl

# Compress output using zstd (recommended for large datasets)
mds download --name my-token --compress --output my-data
```