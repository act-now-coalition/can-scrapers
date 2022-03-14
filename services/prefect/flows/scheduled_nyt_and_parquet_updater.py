import requests
from datetime import timedelta
from prefect import Flow, task
from prefect.engine.state import Failed
from prefect.schedules import CronSchedule
from prefect.tasks.secrets import PrefectSecret
from prefect.utilities.notifications import slack_notifier
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def make_request(github_token):
    dispatch_url = (
        "https://api.github.com/repos/covid-projections/covid-data-model/"
        "actions/workflows/update_repo_datasets.yml/dispatches"
    )
    response = requests.post(
        url=dispatch_url,
        headers={
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json",
        },
        data='{"ref": "main"}',
    )
    response.raise_for_status()  # raise status in case of failure


def init_update_datasets_flow():
    """Initialize a flow to kick off the covid-data-model Update Combined Datasets Github action"""
    with Flow("TriggerUpdateCombinedDatasets") as flow:
        github_token = PrefectSecret("GITHUB_ACTION_PAT")
        make_request(github_token=github_token)
    flow.register("can-scrape")


def init_scheduled_nyt_updater_flow():
    """Flow to check the NYT source for new data, ingest the data, and update the parquet file.

    Flow runs every 30 minutes. If no new data is detected then UpdateParquetFiles and
    TriggerUpdateCombinedDatasets are skipped.
    
    This is the main updater responsible for updating and ingesting the NYT cases and deaths data.
    """
    with Flow(
        "NYTParquetScheduledUpdater",
        schedule=CronSchedule("*/30 * * * *"),
        state_handlers=[
            slack_notifier(only_states=[Failed])
        ],  # only alert us on failure
    ) as parent_flow:

        # Note that the below code relies on NYTimesCasesDeaths and UpdateParquetFiles flows
        # already being registered via generated_flows.py and update_api_view.py.
        # If the MainFlow flow is running, this generally means that these flows have been registered.
        nyt_flow = create_flow_run(
            flow_name="NYTimesCasesDeaths",
            project_name="can-scrape",
            task_args=dict(name="Create NYTimesCasesDeaths flow"),
        )

        # NOTE (sean) 3/14/2022: Temporarily adding USAFacts flow to track at what time of day
        # USA Facts is typically updated. To be removed after a few days of data collection. 
        usa_facts_flow = create_flow_run(
            flow_name="USAFactsCases",
            project_name="can-scrape",
            task_args=dict(name="Create USAFactsCases flow"),
        )
        wait_for_usa_facts = wait_for_flow_run(
            usa_facts_flow,
            raise_final_state=False,
            stream_logs=True,
            task_args=dict(name="run USAFactsCases flow"),
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
        wait_for_parquet_flow = wait_for_flow_run(
            parquet_flow,
            raise_final_state=True,
            stream_logs=True,
            task_args=dict(name="Run UpdateParquetFiles flow"),
        )

        trigger_update_datasets_flow = create_flow_run(
            flow_name="TriggerUpdateCombinedDatasets",
            project_name="can-scrape",
            upstream_tasks=[wait_for_parquet_flow],
            task_args=dict(
                name="Create TriggerUpdateCombinedDatasets flow",
                skip_on_upstream_skip=True,
            ),
        )

        # Wait for this flow to succeed/fail before setting the final state of parent_flow
        run_trigger_update_datasets_flow = wait_for_flow_run(
            trigger_update_datasets_flow,
            raise_final_state=True,
            stream_logs=True,
            task_args=dict(name="Run TriggerUpdateCombinedDatasets flow"),
        )

    parent_flow.register("can-scrape")


if __name__ == "__main__":
    init_update_datasets_flow()
    init_scheduled_nyt_updater_flow()
