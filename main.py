import pandas as pd
from pandas import DataFrame
from pathlib import Path
from pprint import pprint
import re

PATH = "../data/signal_data_dump_100c_100r_01_r.csv"
TARGET = 37950
EPOCH = 2
SERVER_PORT = 4444


def main(path: Path):
    data: DataFrame = pd.read_csv(path)
    counter: dict[int, int] = {}
    for _, row in data.iterrows():
        destination: int = get_src_and_dst_port(row.Info)["dst"]
        if destination == TARGET:
            data_epoch = data.loc[
                (row.Time < data.Time) & (data.Time <= row.Time + EPOCH)
            ]
            add_epoch(data_epoch, counter)
            data_epoch = data.loc[
                (row.Time + EPOCH < data.Time) & (data.Time <= row.Time + 2 * EPOCH)
            ]
            sub_epoch(data_epoch, counter)

    sorted_counter = list(
        sorted(counter.items(), key=lambda item: item[1], reverse=True)
    )
    pprint(sorted_counter)


def add_epoch(data: DataFrame, counter: dict[int, int]):
    for _, row in data.iterrows():
        source: int = get_src_and_dst_port(row.Info)["src"]
        if TARGET != source != SERVER_PORT:
            if source in counter.keys():
                counter[source] += 1
            else:
                counter[source] = 1


def sub_epoch(data: DataFrame, counter: dict[int, int]):
    for _, row in data.iterrows():
        source: int = get_src_and_dst_port(row.Info)["src"]
        if TARGET != source != SERVER_PORT:
            if source in counter.keys():
                counter[source] -= 1
            else:
                counter[source] = -1


def get_src_and_dst_port(info: str) -> dict[str, int]:
    # If this fails, you most likely have an error in your dataset
    res = re.findall(r"(\d+)\s*+>\s*(\d+).*", info)[0]
    return {"src": int(res[0]), "dst": int(res[1])}


if __name__ == "__main__":
    main(Path(PATH))
