import argparse
import sys
import json
import pandas as pd

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
        "--data-path",
        type=str,
        required=True,
        help="The path to the data in csv format",
    )
    parser.add_argument(
        "--settings-path",
        type=str,
        required=True,
        help="The path to the settings in json format",
    )
    
    options = parser.parse_args(sys.argv[1:])
    with open(options.settings_path) as file:
        settings: dict[str, int] = json.load(file)
    data: pd.DataFrame = pd.read_csv(options.data_path)

    match options.tool:
        case "nsda":
            nsda_main(settings, data)
        case "pmda":
            pmda_main(settings, data)
        case "ssda":
            ssda_main(settings, data)
        case "sda":
            sda_main(settings, data)
