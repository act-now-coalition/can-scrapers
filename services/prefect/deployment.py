import logging
import click
from services.prefect.flows.generated_flows import deploy_scraper_flows
from services.prefect.flows.github_pipeline_trigger import (
    deploy_github_pipeline_trigger_flow,
)
from services.prefect.flows.update_api_view import deploy_update_parquet_flow
from services.prefect.flows.clean_sql import deploy_clean_sql_flows

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)


@click.group()
def main():
    pass


@main.command()
@click.option(
    "--choice",
    "-c",
    type=click.Choice(
        ["all", "scraper", "clean-sql", "update-parquet", "github-pipeline-trigger"]
    ),
)
def deploy_flows(choice: str):
    if choice == "scraper" or choice == "all":
        _logger.info("Deploying scraper flows...")
        deploy_scraper_flows()
    if choice == "clean-sql" or choice == "all":
        _logger.info("Deploying clean sql flows...")
        deploy_clean_sql_flows()
    if choice == "update-parquet" or choice == "all":
        _logger.info("Deploying update parquet flow...")
        deploy_update_parquet_flow()
    if choice == "github-pipeline-trigger" or choice == "all":
        _logger.info("Deploying github pipeline trigger...")
        deploy_github_pipeline_trigger_flow()


if __name__ == "__main__":
    main()
