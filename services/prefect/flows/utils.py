import prefect
from prefect.client import Client
from prefect.engine.state import Skipped, State
from prefect import Flow
from typing import Optional, Set


def skip_if_running_handler(obj, old_state, new_state):
    """State handler to skip flow if another instance is already in progress.
    
    see: https://github.com/PrefectHQ/prefect/discussions/5373
    """
    logger = prefect.context.get("logger")

    # queries the graphql server to see if any instances of the same flow
    # is already running, and if so, sets the flow state to Skipped
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
            message = (
                "Flow already has a run in progress..."
                f"Skipping due to run in progress: {active_flow_runs}"
            )
            logger.info(message)
            return Skipped(message)
    return new_state


def etag_caching_terminal_state_handler(
    flow: Flow, state: State, reference_task_states: Set[State],
) -> Optional[State]:
    """Update final state to Skipped if a reference task is skipped with context item etag_skip_flag = True.

    This is used in conjunction with the skip_cached_flow() task to manually skip a
    flow if the Etag caching does not detect new data. This allows us to conditionally
    run flows downstream of a scraper flow, depending on whether or not there is new data.
    This conditional logic is used in the NYTParquetUpdater flow in
    scheduled_nyt_and_parquet_updater.py.

    For more info on terminal state handlers:
        https://docs.prefect.io/core/concepts/flows.html#terminal-state-handlers
    """

    # look through all the flow's tasks to see if any were skipped.
    # If so, and the etag_skip_flag context item exists and is True, set the final state of the flow to Skipped.
    # etag_skip_flag is a custom flag passed to a state signal's context during the etag cache checking process.
    # Any tasks that normally finish with the Skipped state will not trigger this state handler.
    for task_state in reference_task_states:
        if task_state.is_skipped():
            if dict.get(task_state.context, "etag_skip_flag", False) is True:
                return Skipped(
                    "Setting final state to skipped due to reference task. "
                    "Skipped due to state context item etag_skip_flag = True"
                )
    return state

