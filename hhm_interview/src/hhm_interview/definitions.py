from pathlib import Path

from dagster import definitions, Definitions,load_from_defs_folder

from hhm_interview.defs.resources import io_manager


@definitions
def defs():
    load_defs = load_from_defs_folder(path_within_project=Path(__file__).parent)

    return Definitions(
        load_defs.assets,
        resources={"io_manager":io_manager}
    )
