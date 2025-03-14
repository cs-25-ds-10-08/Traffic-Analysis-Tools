# https://www.cs.umd.edu/users/kaptchuk/publications/ndss21.pdf

from pandas import DataFrame
from  typing import cast
import random

from helper.util import Identifier, get_src_and_dst


def main(settings: dict[str, str | float], data: DataFrame):
    counter: dict[Identifier, int] = {}
    time: float = 0
    epoch: float = cast(float, settings["epoch"])

    for _, row in data.iterrows():
        if row.Time < time:
            continue
        time = row.Time

        destination: Identifier = get_src_and_dst(row)["dst"]
        if destination == settings["target"]:
            # Time from target sends to now + epoch
            target_epoch = data.loc[(row.Time < data.Time) & (data.Time <= row.Time + epoch)]
            update_counts(1, counter, target_epoch, settings)

            # Time from end of last timeframe to now + epoch
            rand_scalar: float = random.uniform(0, epoch)
            random_epoch = data.loc[
                (row.Time + epoch + rand_scalar < data.Time)
                & (data.Time <= row.Time + 2 * epoch + rand_scalar)
]
            update_counts(-1, counter, random_epoch, settings)

            time += epoch

    result = max(counter.items(), key=lambda x: x[1])
    print(f"Target: {settings['target']}\nActual: {settings['actual']}\nMost likely: {result[0]}")


def update_counts(inc: int, counter: dict[Identifier, int], data: DataFrame, settings: dict[str, str | float]):
    for _, row in data.iterrows():
        destination: str = get_src_and_dst(row)["dst"]
        if settings["target"] != destination != settings["server_port"]:
            if destination in counter.keys():
                counter[destination] += inc
            else:
                counter[destination] = inc

