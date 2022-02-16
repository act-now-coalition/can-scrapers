import prefect
from prefect.client import Client
from prefect.engine.state import Skipped

def skip_if_running_handler(obj, old_state, new_state):
    """State handler to skip flow if another instance is already in progress.
    
    see: https://github.com/PrefectHQ/prefect/discussions/5373
    """
    
    logger = prefect.context.get("logger")
    if new_state.is_running():
        client = Client()
        query = """
            query($flow_id: uuid) {
              flow_run(
                where: {_and: [{flow_id: {_eq: $flow_id}},
                {state: {_eq: "Running"}}]}
                limit: 1
              ) {
                name
                state
                start_time
              }
            }
        """
        response = client.graphql(
            query=query, variables=dict(flow_id=prefect.context.flow_id)
        )
        active_flow_runs = response["data"]["flow_run"]
        if active_flow_runs:
            message = "Flow already has a run in progress, skipping..."
            logger.info(message)
            return Skipped(message)
    return new_state