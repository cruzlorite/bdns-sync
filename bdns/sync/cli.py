# -*- coding: utf-8 -*-
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
# You should have received a copy of the GNU General Public License along
# with this program. If not, see <https://www.gnu.org/licenses/>.

"""bdns-sync is a pure parameterized tool: one endpoint per invocation, no
config file, no cadence knowledge. Which endpoints to sync and when is an
orchestration concern that lives outside this package (see scripts/).
"""

import logging
from datetime import date
from typing import Optional

import typer

from bdns.fetch import BDNSClient
from bdns.sync import __version__
from bdns.sync.generic import WINDOWS
from bdns.sync.sinks import get_sink
from bdns.sync.syncers import (
    CONVOCATORIAS_ENDPOINT,
    FULL_SYNCERS,
    SEARCH_SYNCERS,
    sync_convocatorias,
)

app = typer.Typer(
    name="bdns-sync",
    help="Sync one BDNS API endpoint into a target database in SCD2 form.",
    add_completion=False,
)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"bdns-sync {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        False,
        "--version",
        callback=_version_callback,
        is_eager=True,
        help="Show the version and exit.",
    ),
) -> None:
    """BDNS Sync command line interface.

    Configures logging here, not in `__main__.py`'s `if __name__ ==
    "__main__":` guard. The installed `bdns-sync` console script
    (`pyproject.toml`) imports and calls this Typer `app` directly, so that
    guard never runs. Typer always runs this callback before any
    subcommand regardless of entry point, so this is the one place
    guaranteed to run every time.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        force=True,
    )


TARGET_URL_OPTION = typer.Option(
    ...,
    "--target-url",
    envvar="BDNS_SYNC_TARGET_URL",
    help="SQLAlchemy target DB URL (e.g. bigquery://project/dataset).",
)


def _parse_iso_date(value: Optional[str], flag: str) -> Optional[date]:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise typer.BadParameter(f"{flag} must be an ISO date (YYYY-MM-DD), got {value!r}")


@app.command()
def sync(
    endpoint: str = typer.Argument(..., help="Endpoint/entity name to sync."),
    target_url: str = TARGET_URL_OPTION,
    window: str = typer.Option(
        None,
        "--window",
        help=f"Cascade window for incremental endpoints: one of {', '.join(WINDOWS)}.",
    ),
    since: str = typer.Option(
        None,
        "--since",
        help="Backfill start date (YYYY-MM-DD) for incremental endpoints. "
        "Overrides --window; syncs [since, until]. See scripts/full_load.sh.",
    ),
    until: str = typer.Option(
        None,
        "--until",
        help="Backfill end date (YYYY-MM-DD). Defaults to yesterday. Only with --since.",
    ),
) -> None:
    """Sync one endpoint.

    Incremental endpoints (the big search endpoints plus convocatorias) need
    a reg-date range: either a cascade `--window` (daily/weekly/monthly/annual)
    or an explicit `--since [--until]` backfill range. Full-replace endpoints
    ignore all of these.
    """
    sink = get_sink(target_url)
    client = BDNSClient()

    since_date = _parse_iso_date(since, "--since")
    until_date = _parse_iso_date(until, "--until")

    if endpoint == CONVOCATORIAS_ENDPOINT or endpoint in SEARCH_SYNCERS:
        sync_fn = sync_convocatorias if endpoint == CONVOCATORIAS_ENDPOINT else SEARCH_SYNCERS[endpoint]
        if since_date is not None:
            if window is not None:
                raise typer.BadParameter("use either --window or --since, not both")
            if until_date is not None and until_date < since_date:
                raise typer.BadParameter("--until must not be before --since")
            stats = sync_fn(sink, client, since=since_date, until=until_date)
        elif window is not None:
            if window not in WINDOWS:
                raise typer.BadParameter(f"window must be one of {', '.join(WINDOWS)}")
            stats = sync_fn(sink, client, window)
        else:
            raise typer.BadParameter(f"{endpoint} requires --window or --since")
    elif endpoint in FULL_SYNCERS:
        stats = FULL_SYNCERS[endpoint](sink, client)
    else:
        raise typer.BadParameter(f"unknown endpoint: {endpoint}")

    typer.echo(f"{endpoint}: {stats}")


@app.command(name="list")
def list_endpoints(
    kind: str = typer.Option(
        "full", "--kind", help="one of: full, search, convocatorias"
    ),
) -> None:
    """List known endpoint names, one per line, for scripting (no hardcoded lists)."""
    if kind == "full":
        for name in FULL_SYNCERS:
            typer.echo(name)
    elif kind == "search":
        for name in SEARCH_SYNCERS:
            typer.echo(name)
    elif kind == "convocatorias":
        typer.echo(CONVOCATORIAS_ENDPOINT)
    else:
        raise typer.BadParameter("kind must be one of: full, search, convocatorias")
