import pandas as pd
import sys
import json
from pandas import DataFrame
import argparse
import re


def main():
    parser = argparse.ArgumentParser(
        prog="Statistical Disclosure Attack Analysis Tool",
        description="Performs an offline Statistical Disclosure Attack",
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

    counter: dict[int, int] = {}
    for _, row in data.iterrows():
        destination: int = get_src_and_dst_port(row.Info)["dst"]
        if destination == settings["target"]:
            data_epoch = data.loc[
                (row.Time < data.Time) & (data.Time <= row.Time + settings["epoch"])
            ]
            add_epoch(data_epoch, counter, settings["target"], settings["server_port"])
            data_epoch = data.loc[
                (row.Time + settings["epoch"] < data.Time)
                & (data.Time <= row.Time + 2 * settings["epoch"])
            ]
            sub_epoch(data_epoch, counter, settings["target"], settings["server_port"])

    sorted_counter = list(
        sorted(counter.items(), key=lambda item: item[1], reverse=True)
    )
    print(
        f"Target: {settings['target']}\nMost likely: {sorted_counter[0][0]}\nActual: {settings['actual']}"
    )


def add_epoch(data: DataFrame, counter: dict[int, int], target: int, server_port: int):
    for _, row in data.iterrows():
        destination: int = get_src_and_dst_port(row.Info)["dst"]
        if target != destination != server_port:
            if destination in counter.keys():
                counter[destination] += 1
            else:
                counter[destination] = 1


def sub_epoch(data: DataFrame, counter: dict[int, int], target: int, server_port: int):
    for _, row in data.iterrows():
        destination: int = get_src_and_dst_port(row.Info)["dst"]
        if target != destination != server_port:
            if destination in counter.keys():
                counter[destination] -= 1
            else:
                counter[destination] = -1


def get_src_and_dst_port(info: str) -> dict[str, int]:
    # If this fails, you most likely have an error in your dataset
    res = re.findall(r"(\d+)\s*>\s*(\d+).*", info)[0]
    return {"src": int(res[0]), "dst": int(res[1])}


if __name__ == "__main__":
    main()
