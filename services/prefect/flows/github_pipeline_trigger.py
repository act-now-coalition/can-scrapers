import requests
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.deployments import Deployment
from services.prefect.flows.update_api_view import update_parquet_flow


@task
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


@flow
def github_pipeline_trigger():
    github_token = Secret.load("github-action-pat").get()
    update_parquet_flow()
    make_request(github_token)


def deploy_github_pipeline_trigger():
    deployment: Deployment = Deployment.build_from_flow(
        flow=github_pipeline_trigger,
        name="github_pipeline_trigger",
    )
    deployment.apply()
