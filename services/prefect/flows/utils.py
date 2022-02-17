from prefect import Flow
from prefect.engine.state import Skipped, State
from typing import Optional, Set


def etag_caching_terminal_state_handler(
    flow: Flow,
    state: State,
    reference_task_states: Set[State],
) -> Optional[State]:
    """Update flow's final state to Skipped if a reference task is skipped with etag_skip_flag = True.

    This is used in conjunction with the skip_cached_flow() task to manually skip a
    flow if the Etag caching does not detect new data. This allows us to conditionally
    run flows downstream of a scraper flow, depending on whether or not there is new data.
    This conditional logic is used in the NYTParquetUpdater flow in
    scheduled_nyt_and_parquet_updater.py.

    For more info on terminal state handlers:
        https://docs.prefect.io/core/concepts/flows.html#terminal-state-handlers
    """

    # look through all the flow's tasks to see if any we're skipped.
    # If so, and the etag_skip_flag attribute is True, set the final state of the flow to Skipped.
    # etag_skip_flag is a custom flag passed to a task's skip signal during the etag cache checking process.
    # Any tasks that normally finish with the Skipped state will not trigger this state handler.
    for task_state in reference_task_states:
        if task_state.is_skipped():
            if getattr(task_state, "etag_skip_flag", False) is True:
                return Skipped(
                    "Setting final state to skipped due to reference task"
                    "skipped with attribute etag_skip_flag = True"
                )
    return state
