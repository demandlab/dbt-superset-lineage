"""
dbt-superset-lineage CLI
"""

__version__ = "0.3.5"

from typing import Optional
import typer
from .pull_dashboards import main as pull_dashboards_main
from .push_descriptions import main as push_descriptions_main

app = typer.Typer()


@app.command()
def pull_dashboards(
    superset_url: str = typer.Argument(
        ...,
        help="URL of your Superset, e.g. https://mysuperset.mycompany.com"
    ),
    *,
    dbt_project_dir: str = typer.Option(
        ".",
        help="Directory path to dbt project."
    ),
    exposures_path: str = typer.Option(
        "/models/exposures/superset_dashboards.yml",
        help=(
            "Where within PROJECT_DIR the exposure file should be stored. If you set "
            "this to go outside /models, it then needs to be added to source-paths "
            "in dbt_project.yml."
        )
    ),
    dbt_db_name: Optional[str] = typer.Option(
        None,
        help="Name of your database within dbt towards which the pull should be reduced to run."
    ),
    superset_db_id: Optional[int] = typer.Option(
        None,
        help="ID of your database within Superset towards which the pull should be reduced to run."
    ),
    sql_dialect: str = typer.Option(
        "ansi",
        help=(
            "Database SQL dialect; used for parsing queries. Consult docs of SQLFluff "
            "for details: https://docs.sqlfluff.com/en/stable/dialects.html"
        )
    ),
    superset_access_token: Optional[str] = typer.Option(
        None,
        envvar="SUPERSET_ACCESS_TOKEN",
        help="Access token to Superset API. Can autogenerate if SUPERSET_REFRESH_TOKEN is provided."
    ),
    superset_refresh_token: Optional[str] = typer.Option(
        None,
        envvar="SUPERSET_REFRESH_TOKEN",
        help="Refresh token to Superset API."
    ),
):
    """
    Pull published dashboards from Superset and generate a dbt exposures YAML.
    """
    pull_dashboards_main(
        dbt_project_dir=dbt_project_dir,
        exposures_path=exposures_path,
        dbt_db_name=dbt_db_name,
        superset_url=superset_url,
        superset_db_id=superset_db_id,
        sql_dialect=sql_dialect,
        superset_access_token=superset_access_token,
        superset_refresh_token=superset_refresh_token,
    )


@app.command()
def push_descriptions(
    superset_url: str = typer.Argument(
        ...,
        help="URL of your Superset, e.g. https://mysuperset.mycompany.com"
    ),
    *,
    dbt_project_dir: str = typer.Option(
        ".",
        help="Directory path to dbt project."
    ),
    dbt_db_name: Optional[str] = typer.Option(
        None,
        help="Name of your database within dbt to which the script should be reduced to run."
    ),
    superset_db_id: Optional[int] = typer.Option(
        None,
        help="ID of your database within Superset towards which the push should be reduced to run."
    ),
    superset_refresh_columns: bool = typer.Option(
        False,
        help="Whether columns in Superset should be refreshed from database before the push."
    ),
    superset_pause_after_update: Optional[int] = typer.Option(
        None,
        help=(
            "Number of seconds for which the script pauses after any update of Superset columns. "
            "This is to allow databases to catch up in the meantime."
        )
    ),
    superset_access_token: Optional[str] = typer.Option(
        None,
        envvar="SUPERSET_ACCESS_TOKEN",
        help="Access token to Superset API. Can autogenerate if SUPERSET_REFRESH_TOKEN is provided."
    ),
    superset_refresh_token: Optional[str] = typer.Option(
        None,
        envvar="SUPERSET_REFRESH_TOKEN",
        help="Refresh token to Superset API."
    ),
):
    """
    Push table and column descriptions from dbt into Superset via the API.
    """
    push_descriptions_main(
        dbt_project_dir=dbt_project_dir,
        dbt_db_name=dbt_db_name,
        superset_url=superset_url,
        superset_db_id=superset_db_id,
        superset_refresh_columns=superset_refresh_columns,
        superset_pause_after_update=superset_pause_after_update,
        superset_access_token=superset_access_token,
        superset_refresh_token=superset_refresh_token,
    )


if __name__ == "__main__":
    app()
