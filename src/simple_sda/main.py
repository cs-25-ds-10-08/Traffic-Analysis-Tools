# https://www.cs.umd.edu/users/kaptchuk/publications/ndss21.pdf

from pandas import DataFrame
import random

from helper.util import get_src_and_dst_port


def main(settings: dict[str, int], data: DataFrame):
    counter: dict[int, int] = {}
    time: float = 0

    for _, row in data.iterrows():
        if row.Time < time:
            continue
        time = row.Time

        destination: int = get_src_and_dst_port(row.Info)["dst"]
        if destination == settings["target"]:
            # Time from target sends to now + epoch
            target_epoch = data.loc[(row.Time < data.Time) & (data.Time <= row.Time + settings["epoch"])]
            update_counts(1, counter, target_epoch, settings)

            # Time from end of last timeframe to now + epoch
            rand_scalar: float = random.uniform(0, settings["epoch"])
            random_epoch = data.loc[
                (row.Time + settings["epoch"] + rand_scalar < data.Time)
                & (data.Time <= row.Time + 2 * settings["epoch"] + rand_scalar)
]
            update_counts(-1, counter, random_epoch, settings)

            time += settings["epoch"]

    result = max(counter.items(), key=lambda x: x[1])
    print(f"Target: {settings['target']}\nActual: {settings['actual']}\nMost likely: {result[0]}")


def update_counts(inc: int, counter: dict[int, int], data: DataFrame, settings: dict[str, int]):
    for _, row in data.iterrows():
        destination: int = get_src_and_dst_port(row.Info)["dst"]
        if settings["target"] != destination != settings["server_port"]:
            if destination in counter.keys():
                counter[destination] += inc
            else:
                counter[destination] = inc

