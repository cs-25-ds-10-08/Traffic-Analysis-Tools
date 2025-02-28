import re
import pandas as pd
from pandas import DataFrame
import argparse
import sys
import json


def get_src_and_dst_port(info: str) -> dict[str, int]:
    # If this fails, you most likely have an error in your dataset
    res = re.findall(r"(\d+)\s*>\s*(\d+).*", info)[0]
    return {"src": int(res[0]), "dst": int(res[1])}


def init(name: str, *, description: str = "") -> tuple[dict[str, int], DataFrame]:
    parser = argparse.ArgumentParser(
        prog=name,
        description=description,
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
    data: DataFrame = pd.read_csv(options.data_path)

    print(f"Running {name}")
    return (settings, data)
