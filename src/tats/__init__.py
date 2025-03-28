import argparse
import sys
import json
from typing import Callable
from pandas import DataFrame, read_csv
from pathlib import Path

from helper.util import Settings
from helper.sda_profiling import sda_profiling
from helper.sda_selected_profiling import sda_selected_profiling

from nsda.main import main as nsda_main
from pmda.main import main as pmda_main
from simple_sda.main import main as ssda_main
from sda.main import main as sda_main


def executor():
    parser = argparse.ArgumentParser(
        prog="Traffic Analysis Tool Executor",
        description="Executes the specified tool",
    )
    parser.add_argument(
        "tool",
        type=str,
        choices=["nsda", "pmda", "ssda", "sda"],
        help="The tool to be executed",
    )
    parser.add_argument(
        "--path",
        type=Path,
        required=True,
        help="The path to folder which MUST contain a data.csv and settings.json",
    )
    parser.add_argument("--selected", action="store_true", help="Changes the way the SDA profiling is done")

    options = parser.parse_args(sys.argv[1:])
    with open(options.path / "settings.json") as file:
        settings: Settings = json.load(file)
    data: DataFrame = read_csv(
        options.path / "data.csv", usecols=["Time", "Source", "Destination", "src_port", "dst_port"]
    )

    sda_profiler: Callable[[Settings, DataFrame], DataFrame] = (
        sda_selected_profiling if options.selected else sda_profiling
    )

    match options.tool:
        case "nsda":
            nsda_main(settings, data, sda_profiler)
        case "pmda":
            pmda_main(settings, data, sda_profiler)
        case "ssda":
            ssda_main(settings, data)
        case "sda":
            sda_main(settings, data, sda_profiler)


if __name__ == "__main__":
    executor()
