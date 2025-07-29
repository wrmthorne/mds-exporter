import os
from pathlib import Path
import json
import asyncio

import click
import httpx
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn



EXTRACT_URL = "https://mds-data-1.ciim.k-int.com/api/v1/extract"


def save_resumption_token(resume_file: Path, token: str):
    with open(resume_file, 'w') as f:
        f.write(token)



async def download_data(output_file: Path, resumption_token: str, resume_file: Path):
    if not resumption_token:
        raise ValueError("Resumption token is required")

    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.with_suffix(".jsonl").touch()

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
            
            # Write initial batch and save resumption token
            data = resp_json.get("data", [])
            with open(output_file.with_suffix(".jsonl"), "a") as f:
                for item in data:
                    f.write(json.dumps(item) + "\n")
            progress.update(task, advance=len(data))
            
            # Save resumption token after initial batch
            if resp_json.get("resume"):
                save_resumption_token(resume_file, resp_json.get("resume"))
            
            # Continue with sequential pagination
            with open(output_file.with_suffix(".jsonl"), "a") as f:
                while resp_json.get("has_next"):
                    response = await client.get(resp_json.get("next_url"))
                    response.raise_for_status()

                    resp_json = response.json()
                    data = resp_json.get("data", [])
                    for item in data:
                        f.write(json.dumps(item) + "\n")
                    progress.update(task, advance=len(data))
                    
                    # Save resumption token after each batch
                    if resp_json.get("resume"):
                        save_resumption_token(resume_file, resp_json.get("resume"))
    
@click.command()
@click.option('--resumption-token', required=True, help='MDS API resumption token')
@click.option('--output', default='downloads.jsonl', help='Output JSONL file path')
@click.option('--resume-file', default='resume.txt', help='File to save resumption tokens for recovery')
def main(resumption_token, output, resume_file):
    output_file = Path(output)
    resume_file_path = Path(resume_file)
    asyncio.run(download_data(output_file, resumption_token, resume_file_path))


if __name__ == "__main__":
    main()