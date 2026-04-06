"""
CLI entry point for the RF CNPJ scraping pipeline.

Usage examples
--------------
# Run the full pipeline (latest snapshot, all groups)
python main.py run

# Download and process only reference tables
python main.py run --reference-only

# Process specific groups with more parallelism
python main.py run --groups Empresas --groups Socios --download-workers 6

# Force re-download and re-extract everything
python main.py run --force

# List all available snapshots
python main.py list-snapshots

# Show info about the latest snapshot without downloading
python main.py info
"""

from __future__ import annotations

import click

from src.logger import logger, setup_logging


@click.group(invoke_without_command=True)
@click.option(
    "--log-level",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
    show_default=True,
    help="Minimum log level for console output.",
)
@click.pass_context
def cli(ctx: click.Context, log_level: str) -> None:
    """RF CNPJ data scraping pipeline."""
    setup_logging(level=log_level.upper())
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

@cli.command()
@click.option(
    "--groups",
    "-g",
    multiple=True,
    help=(
        "Restrict to these file groups. "
        "Choices: Empresas, Estabelecimentos, Socios, Simples, "
        "Cnaes, Motivos, Municipios, Naturezas, Paises, Qualificacoes. "
        "Repeat for multiple groups. Default: all."
    ),
)
@click.option(
    "--reference-only",
    is_flag=True,
    default=False,
    help="Only download and process the small reference/lookup tables.",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Re-download and re-extract even if files already exist locally.",
)
@click.option(
    "--download-workers",
    default=4,
    show_default=True,
    type=int,
    help="Number of parallel download threads.",
)
@click.option(
    "--process-workers",
    default=4,
    show_default=True,
    type=int,
    help="Number of parallel processing threads.",
)
@click.option(
    "--snapshot",
    default=None,
    help="Target a specific snapshot date (YYYY-MM-DD). Default: latest.",
)
@click.option(
    "--format",
    "storage_format",
    default=None,
    type=click.Choice(["parquet", "csv", "duckdb"], case_sensitive=False),
    help="Output storage format. Overrides config.STORAGE_BACKEND. Default: parquet.",
)
def run(
    groups: tuple[str, ...],
    reference_only: bool,
    force: bool,
    download_workers: int,
    process_workers: int,
    snapshot: str | None,
    storage_format: str | None,
) -> None:
    """Run the full scraping pipeline."""
    from src.pipeline import run_pipeline

    run_pipeline(
        groups=list(groups) if groups else None,
        snapshot_date=snapshot,
        force_download=force,
        force_extract=force,
        download_workers=download_workers,
        process_workers=process_workers,
        reference_only=reference_only,
        storage_backend=storage_format,
    )


# ---------------------------------------------------------------------------
# info
# ---------------------------------------------------------------------------

@cli.command()
def info() -> None:
    """Show information about the latest available snapshot."""
    import requests
    from src.config import HEADERS
    from src.crawler import discover_latest_snapshot

    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        snapshot = discover_latest_snapshot(session=session)
    finally:
        session.close()

    click.echo(f"\nLatest snapshot : {snapshot.date}")
    click.echo(f"URL             : {snapshot.url}")
    click.echo(f"Files           : {len(snapshot.files)}")
    total_gb = snapshot.total_size_bytes / 1_024**3
    click.echo(f"Total size      : {total_gb:.1f} GB (compressed)\n")

    groups: dict[str, list] = {}
    for f in snapshot.files:
        groups.setdefault(f.group, []).append(f)

    click.echo(f"{'Group':<22} {'Files':>5}  {'Size':>10}")
    click.echo("-" * 42)
    for group, gfiles in sorted(groups.items()):
        total = sum(f.size_bytes or 0 for f in gfiles)
        size_str = f"{total / 1_024**2:.0f} MB" if total < 1_024**3 else f"{total / 1_024**3:.1f} GB"
        click.echo(f"{group:<22} {len(gfiles):>5}  {size_str:>10}")
    click.echo()


# ---------------------------------------------------------------------------
# list-snapshots
# ---------------------------------------------------------------------------

@cli.command("list-snapshots")
def list_snapshots() -> None:
    """List all available snapshot dates on the server."""
    import requests
    from src.config import HEADERS
    from src.crawler import list_all_snapshots

    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        dates = list_all_snapshots(session=session)
    finally:
        session.close()

    click.echo(f"\n{len(dates)} snapshots available:\n")
    for d in dates:
        click.echo(f"  {d}")
    click.echo()


# ---------------------------------------------------------------------------
# download-only
# ---------------------------------------------------------------------------

@cli.command("download-only")
@click.option("--groups", "-g", multiple=True, help="File groups to download.")
@click.option("--reference-only", is_flag=True, default=False)
@click.option("--force", is_flag=True, default=False)
@click.option("--workers", default=4, show_default=True, type=int)
def download_only(
    groups: tuple[str, ...],
    reference_only: bool,
    force: bool,
    workers: int,
) -> None:
    """Download ZIP files without extraction or processing."""
    import requests
    from src.config import HEADERS, REFERENCE_FILES
    from src.crawler import discover_latest_snapshot
    from src.downloader import download_all

    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        snapshot = discover_latest_snapshot(session=session)
        files = snapshot.files

        if reference_only:
            files = [f for f in files if f.group in REFERENCE_FILES]
        elif groups:
            groups_set = {g.lower() for g in groups}
            files = [f for f in files if f.group.lower() in groups_set]

        download_all(files, workers=workers, force=force)
    finally:
        session.close()


if __name__ == "__main__":
    import sys
    # Allow running with no arguments (shows help) without raising SystemExit(2)
    cli(standalone_mode=False, args=sys.argv[1:] or ["--help"])
