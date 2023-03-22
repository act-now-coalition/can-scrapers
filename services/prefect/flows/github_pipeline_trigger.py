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


def init_scheduled_updater_flow():
    """Initialize a flow to update the Parquet file and kick off the covid-data-model pipeline.

    Runs every day at 2am ET. Kicks off the Update Combined Datasets Github action on successful parquet update."""
    with Flow(
        "ParquetGithubPipelineUpdater",
        schedule=CronSchedule("0 6 * * *"),  # every day at 2am ET
        state_handlers=[
            slack_notifier(only_states=[Failed])
        ],  # only alert us on failure
    ) as parent_flow:

        # Execute on success, skip on skipped result of wait_for_nyt
        parquet_flow = create_flow_run(
            flow_name="UpdateParquetFiles",
            project_name="can-scrape",
            task_args=dict(
                name="Create UpdateParquetFiles flow",
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
    init_scheduled_updater_flow()
