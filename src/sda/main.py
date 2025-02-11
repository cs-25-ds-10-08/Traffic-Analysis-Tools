import pandas as pd
import sys
import json
from pandas import DataFrame
import argparse
import re


def main():
    settings, data = init()
    counter: dict[int, int] = {}

    for _, row in data.iterrows():
        destination: int = get_src_and_dst_port(row.Info)["dst"]
        if destination == settings["target"]:
            # Time from target sends to now + epoch
            data.loc[(row.Time < data.Time) & (data.Time <= row.Time + settings["epoch"])]
            update_counts(1, counter, data, settings)
            # Time from end of last timeframe to now + epoch
            data.loc[(row.Time + settings["epoch"] < data.Time) & (data.Time <= row.Time + 2 * settings["epoch"])]
            update_counts(-1, counter, data, settings)

    sorted_counter = list(
        sorted(counter.items(), key=lambda item: item[1], reverse=True)
    )
    print(
        f"Target: {settings['target']}\nMost likely: {sorted_counter[0][0]}\nActual: {settings['actual']}"
    )


def update_counts(inc: int, counter: dict[int, int], data: DataFrame, settings: dict[str, int]):
    for _, row in data.iterrows():
        destination: int = get_src_and_dst_port(row.Info)["dst"]
        if settings["target"] != destination != settings["server_port"]:
            if destination in counter.keys():
                counter[destination] += inc
            else:
                counter[destination] = inc


def get_src_and_dst_port(info: str) -> dict[str, int]:
    # If this fails, you most likely have an error in your dataset
    res = re.findall(r"(\d+)\s*>\s*(\d+).*", info)[0]
    return {"src": int(res[0]), "dst": int(res[1])}


def init() -> tuple[dict[str, int], DataFrame]:
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
    
    return (settings, data)


if __name__ == "__main__":
    main()
