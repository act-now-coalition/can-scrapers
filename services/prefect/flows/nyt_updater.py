import requests
from prefect import Flow, task
from prefect.schedules import CronSchedule
from prefect.tasks.secrets import PrefectSecret
from datetime import timedelta

from prefect.tasks.prefect import create_flow_run, wait_for_flow_run


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def make_request(github_token):
    workflow_url = (
        "https://api.github.com/covid-projections/"
        "covid-data-model/actions/workflows/update_repo_datasets.yml/dispatches"
    )

    requests.post(
        url=workflow_url,
        headers={'authorization': f'Bearer {github_token}',}, 
        data={"event_type": "trigger update datasets"}
    )

def trigger_data_model_flow():
    with Flow("TriggerUpdateDatasets") as flow:
        github_token=PrefectSecret("GITHUB_ACTION_PAT")
        make_request(github_token=github_token)
    flow.register("can-scrape")


def init_updater_flow():
    with Flow("NYTParquetUpdater", schedule=CronSchedule("30 6 * * *")) as flow:

        # NYTimesCasesDeaths and UpdateParquetFiles flows must already be registered via
        # generated_flows.py and update_api_view.py
        nyt_flow = create_flow_run(
            flow_name="NYTimesCasesDeaths", project_name="can-scrape"
        )
        wait_for_nyt = wait_for_flow_run(nyt_flow, raise_final_state=True)

        parquet_flow = create_flow_run(
            flow_name="UpdateParquetFiles", project_name="can-scrape"
        )
        # wait_for_parquet = wait_for_flow_run(parquet_flow, raise_final_state=True)

        data_update_flow = create_flow_run(
            flow_name="TriggerUpdateDatasets", project_name="can-scrape"
        )
        # wait_for_data_update = wait_for_flow_run(data_update_flow, raise_final_state=True)

        parquet_flow.set_upstream(wait_for_nyt)
        # data_update_flow.set_upstream(wait_for_parquet)

    flow.register(project_name="can-scrape")


if __name__ == "__main__":
    # trigger_data_model_flow()
    init_updater_flow()
