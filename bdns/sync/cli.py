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

import typer
from sqlalchemy import create_engine

from bdns.fetch import BDNSClient
from bdns.sync import __version__
from bdns.sync.generic import WINDOWS
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


@app.command()
def sync(
    endpoint: str = typer.Argument(..., help="Endpoint/entity name to sync."),
    target_url: str = TARGET_URL_OPTION,
    window: str = typer.Option(
        None,
        "--window",
        help=f"Required for incremental endpoints: one of {', '.join(WINDOWS)}.",
    ),
) -> None:
    """Sync one endpoint. Incremental endpoints (the big search endpoints plus
    convocatorias) require --window; everything else ignores it.
    """
    engine = create_engine(target_url)
    client = BDNSClient()

    if endpoint == CONVOCATORIAS_ENDPOINT or endpoint in SEARCH_SYNCERS:
        if window is None:
            raise typer.BadParameter(f"{endpoint} requires --window")
        if window not in WINDOWS:
            raise typer.BadParameter(f"window must be one of {', '.join(WINDOWS)}")
        sync_fn = sync_convocatorias if endpoint == CONVOCATORIAS_ENDPOINT else SEARCH_SYNCERS[endpoint]
        stats = sync_fn(engine, client, window)
    elif endpoint in FULL_SYNCERS:
        stats = FULL_SYNCERS[endpoint](engine, client)
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
