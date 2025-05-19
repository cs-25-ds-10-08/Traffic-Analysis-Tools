import argparse
import sys
import json
from typing import Callable, cast
from pandas import DataFrame, read_csv
from pathlib import Path

from helper.util import Settings, DenimSettings
from helper.sda_profiling import sda_profiling
from helper.sda_selected_profiling import sda_selected_profiling

from nsda.main import main as nsda_main
from simple_sda.main import main as ssda_main
from sda.main import main as sda_main
from bnsda.main import main as bnsda_main


def executor():
    parser = argparse.ArgumentParser(
        prog="Traffic Analysis Tool Executor",
        description="Executes the specified tool",
    )
    parser.add_argument(
        "tool",
        type=str,
        choices=["ssda", "sda", "nsda", "bnsda"],
        help="The tool to be executed",
    )
    parser.add_argument(
        "--path",
        type=Path,
        required=True,
        help="The path to folder which MUST contain a json file with the settings and a csv file containing the network trace which may be compressed with any compression algorithm which Pandas supports",
    )
    parser.add_argument("--selected", "-s", action="store_true", help="Changes the way the SDA profiling is done")

    options = parser.parse_args(sys.argv[1:])

    data_files = list(options.path.glob("*.csv*"))
    assert len(data_files) == 1, (
        f"The provided path must exactly contain one csv file containing the network trace.\nFound: {data_files}"
    )
    settings_files = list(options.path.glob("*.json"))
    assert len(settings_files) == 1, (
        f"The provided path must exactly contain one json file containing the settings with the following format: {DenimSettings.__annotations__ if options.tool == 'bnsda' else Settings.__annotations__}.\nFound: {data_files}"
    )

    data: DataFrame = read_csv(data_files[0], usecols=["Time", "Source", "Destination", "src_port", "dst_port"])
    with open(settings_files[0]) as file:
        settings: Settings | DenimSettings = json.load(file)

    sda_profiler: Callable[[Settings, DataFrame], DataFrame] = (
        sda_selected_profiling if options.selected else sda_profiling
    )

    match options.tool:
        case "ssda":
            settings = cast(Settings, settings)
            ssda_main(settings, data)
        case "sda":
            settings = cast(Settings, settings)
            sda_main(settings, data, sda_profiler)
        case "nsda":
            settings = cast(Settings, settings)
            nsda_main(settings, data, sda_profiler)
        case "bnsda":
            settings = cast(DenimSettings, settings)
            bnsda_main(settings, data)


if __name__ == "__main__":
    executor()
