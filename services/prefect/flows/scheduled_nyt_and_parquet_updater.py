from prefect import Flow
from prefect.schedules import CronSchedule

from prefect.tasks.prefect import create_flow_run, wait_for_flow_run


def init_nyt_scheduled_flow():
    """Flow to check the NYT source for new data, ingest the data, and update the parquet file.

    Flow runs every 30 minutes. If no new data is detected then UpdateParquetFiles is skipped.
    If new data is detected, UpdateParquetFiles is executed.

    """

    with Flow("NYTParquetUpdater", schedule=CronSchedule("*/30 * * * *")) as flow:

        # Note that the below code relies on NYTimesCasesDeaths and UpdateParquetFiles flows
        # already being registered via generated_flows.py and update_api_view.py.
        # If the MainFlow flow is running, this generally means that these flows have been registered.
        nyt_flow = create_flow_run(
            flow_name="NYTimesCasesDeaths", project_name="can-scrape"
        )
        wait_for_nyt = wait_for_flow_run(nyt_flow, raise_final_state=True)

        parquet_flow = create_flow_run(
            flow_name="UpdateParquetFiles", project_name="can-scrape"
        )

        # Wait for the parquet flow to finish before setting parent flow state to success.
        finish_parquet_flow = wait_for_flow_run(parquet_flow, raise_final_state=True)

        # flow will only trigger if wait_for_nyt finishes with Success state.
        # so if the NYT scraper is skipped, the parquet flow will not execute.
        parquet_flow.set_upstream(wait_for_nyt)

    flow.register(project_name="can-scrape")


if __name__ == "__main__":
    init_nyt_scheduled_flow()
