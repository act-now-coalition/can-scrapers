from prefect import Flow
from prefect.schedules import CronSchedule
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run


def init_scheduled_nyt_updater_flow():
    """Flow to check the NYT source for new data, ingest the data, and update the parquet file.

    Flow runs every 30 minutes. If no new data is detected then UpdateParquetFiles is skipped.
    This is the main updater responsible for updating and ingesting the NYT cases and deaths data.

    """
    with Flow(
        "NYTParquetScheduledUpdater", schedule=CronSchedule("*/30 * * * *")
    ) as parent_flow:

        # Note that the below code relies on NYTimesCasesDeaths and UpdateParquetFiles flows
        # already being registered via generated_flows.py and update_api_view.py.
        # If the MainFlow flow is running, this generally means that these flows have been registered.
        nyt_flow = create_flow_run(
            flow_name="NYTimesCasesDeaths",
            project_name="can-scrape",
            task_args=dict(name="Create NYTimesCasesDeaths flow"),
        )
        wait_for_nyt = wait_for_flow_run(
            nyt_flow,
            raise_final_state=True,
            stream_logs=True,
            task_args=dict(name="run NYTimesCasesDeaths flow"),
        )

        # Execute on success, skip on skipped result of wait_for_nyt
        parquet_flow = create_flow_run(
            flow_name="UpdateParquetFiles",
            project_name="can-scrape",
            upstream_tasks=[wait_for_nyt],
            task_args=dict(
                name="Create UpdateParquetFiles flow", skip_on_upstream_skip=True
            ),
        )
        # Wait for this flow to succeed/fail before setting the final state of parent_flow
        run_parquet_flow = wait_for_flow_run(
            parquet_flow,
            raise_final_state=True,
            stream_logs=True,
            task_args=dict(name="Run UpdateParquetFiles flow"),
        )

    parent_flow.register("can-scrape")


if __name__ == "__main__":
    init_scheduled_nyt_updater_flow()
