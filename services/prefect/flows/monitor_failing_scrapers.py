"""Creates a flow to check on scrapers that are consistently failing."""
from typing import Optional, List
import datetime
import dataclasses

import requests
import prefect
from prefect import Flow, task
from prefect.schedules import CronSchedule
from prefect.tasks.secrets import EnvVarSecret

# To run locally, I found the easiest way was to copy the Authorization header
# from a graphql request in the web inspector after vising the Prefect UI.
# I think there's probably a better way to do this using tokens, but worked for me.
AUTH_TOKEN = None
API_SERVER = None
# When running on Prefect, dont need to set the server. but if you're running locally,
# uncomment to hit the prod server.
# API_SERVER = "https://can-prefect.valorumdata.com/graphql"


# Query to pull some aggregated stats about flows.
FLOW_RUN_QUERY = """
query FlowRuns($flow_group_id: uuid!, $flow_id: uuid, $heartbeat: timestamptz) {

  Details: flow_group_by_pk(id:$flow_group_id) {
    flows(limit:1, order_by:{created:desc_nulls_last}){
      name}
  }
  LastRunTime: flow_run_aggregate(
    where: {flow: {flow_group_id: {_eq: $flow_group_id}}}
  ) {
    aggregate {
      max{start_time}
    }
  }
  Success: flow_run_aggregate(
    where: {flow: {flow_group_id: {_eq: $flow_group_id}}, state: {_eq: "Success"}}
  ) {
    aggregate {
      max{start_time}
    }
  }
  Failed: flow_run_aggregate(
    where: {flow: {flow_group_id: {_eq: $flow_group_id}, id: {_eq: $flow_id}}, updated: {_gte: $heartbeat}, state: {_eq: "Failed"}}
  ) {
    aggregate {
      count
    }
  }
}

"""


def graphql_query(client, *args, **kwargs):

    headers = {}
    if AUTH_TOKEN:
        headers["Authorization"] = AUTH_TOKEN
    return client.graphql(*args, headers=headers, **kwargs)


@dataclasses.dataclass(frozen=True)
class RunResult:
    name: str
    last_run_at: datetime.datetime
    last_success_at: Optional[datetime.datetime]

    def is_recently_run(self, hours=24):
        if not self.last_run_at:
            return False
        one_day_ago = datetime.datetime.utcnow() - datetime.timedelta(hours=hours)
        return self.last_run_at > one_day_ago

    def last_success_delta_seconds(self):
        if not self.last_success_at:
            return None
        return (datetime.datetime.utcnow() - self.last_success_at).total_seconds()

    def is_recent_success(self, hours=24):
        if not self.last_run_at or not self.last_success_at:
            return False

        one_day_ago = datetime.datetime.utcnow() - datetime.timedelta(hours=hours)
        return self.last_success_at > one_day_ago


def run_and_parse_flow_group_results(client, flow_group_id):
    response = graphql_query(
        client, FLOW_RUN_QUERY, variables={"flow_group_id": flow_group_id}
    )
    if not response["data"]["Details"]["flows"]:
        return None
    name = response["data"]["Details"]["flows"][0]["name"]
    last_run_time = response["data"]["LastRunTime"]["aggregate"]["max"]["start_time"]
    last_success_time = response["data"]["Success"]["aggregate"]["max"]["start_time"]
    last_run_time = datetime.datetime.fromisoformat(last_run_time[:19])
    if last_success_time:
        last_success_time = datetime.datetime.fromisoformat(last_success_time[:19])
    return RunResult(name, last_run_time, last_success_time)


def find_failing_flows():
    client = prefect.Client(api_server=API_SERVER)

    flow_groups_query = {"query": {"flow_group": {"id"}}}

    response = graphql_query(client, flow_groups_query)
    flow_group_ids = [data["id"] for data in response["data"]["flow_group"]]

    failing_flows = []
    for flow_group_id in flow_group_ids:
        flow_results = run_and_parse_flow_group_results(client, flow_group_id)
        if not flow_results:
            continue

        if not flow_results.is_recently_run():
            continue

        if flow_results.is_recent_success():
            continue

        delta_seconds = flow_results.last_success_delta_seconds()
        if not delta_seconds:
            failing_flows.append(flow_results)
            continue

        last_success_hours = delta_seconds / 3600
        if last_success_hours > 24:
            failing_flows.append(flow_results)

    return failing_flows


def build_slack_blocks(failing_flows: List[RunResult]):

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "*Failing Scrapers*\nThe following scrapers have either "
                    "never succeeded or have not succeeded in more than a day."
                ),
            },
        },
        {"type": "divider"},
    ]
    # Sort by longest failing, with scrapers having never succeeded at the beginning
    failing_flows = sorted(
        failing_flows,
        key=lambda x: x.last_success_delta_seconds() or 10000000,
        reverse=True,
    )

    if not failing_flows:
        message = ":sparkles: Congrats! Scrapers are looking good :sparkles:"
        block = {
            "type": "section",
            "text": {"type": "mrkdwn", "text": message},
        }
        blocks.append(block)
        return {"blocks": blocks}

    for failing_flow in failing_flows:
        if not failing_flow.last_success_at:
            message = "Scraper has never succeeded"
        else:
            fail_days = round(failing_flow.last_success_delta_seconds() / 86400, 1)
            message = f"Scraper failing for {fail_days} days"

        block = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{failing_flow.name}*\n_{message}_",
            },
        }
        blocks.append(block)

    return {"blocks": blocks}


def send_slack_message(slack_webhook_url, slack_request_data):
    requests.post(
        slack_webhook_url,
        json=slack_request_data,
    )


@task()
def run_monitor_scrapers(slack_webhook_url):
    failing_flows = find_failing_flows()
    blocks = build_slack_blocks(failing_flows)
    send_slack_message(slack_webhook_url, blocks)


def create_scraper_monitor_flow():
    # Run every day at 15 UTC (or 10 ET)
    schedule = CronSchedule("0 15 * * *")
    with Flow("MonitorFailingScrapers", schedule) as flow:
        slack_webhook_url = EnvVarSecret("SLACK_WEBHOOK_URL")
        run_monitor_scrapers(slack_webhook_url)

    return flow


def init_flow():
    flow = create_scraper_monitor_flow()
    flow.register(project_name="can-scrape")


if __name__ == "__main__":
    init_flow()
