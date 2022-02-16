from prefect import Flow
from prefect.schedules import CronSchedule

from prefect.tasks.prefect import create_flow_run, wait_for_flow_run


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
        wait_for_parquet = wait_for_flow_run(parquet_flow, raise_final_state=True)

        parquet_flow.set_upstream(wait_for_nyt)

    flow.register(project_name="can-scrape")


if __name__ == "__main__":
    init_updater_flow()
