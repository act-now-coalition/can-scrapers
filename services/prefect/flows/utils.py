from prefect import Flow
from prefect.engine.state import Skipped, State
from typing import Optional, Set

ETAG_CACHE_SKIP_MESSAGE = "No new source data. Skipping..."


def etag_caching_terminal_state_handler(
    flow: Flow,
    state: State,
    reference_task_states: Set[State],
) -> Optional[State]:
    """Update the flow's final state if a reference task is skipped with a specific message.

    This is used in conjunction with the skip_cached_flow() task to manually skip a
    flow if the Etag caching does not detect new data. This allows us to conditionally
    run flows downstream of a scraper flow, depending on whether or not there is new data.
    This conditional logic is used in the NYTParquetUpdater flow in
    scheduled_nyt_and_parquet_updater.py.
    """

    # look through all the flow's tasks to see if any we're skipped.
    # If so, and the message matches, set the final state of the flow to Skipped.
    for task_state in reference_task_states:
        if task_state.is_skipped():
            if task_state.message == ETAG_CACHE_SKIP_MESSAGE:
                return Skipped(
                    "Setting final state to skipped due to reference task skipped with message: "
                    f"{task_state.message}"
                )
    return state
