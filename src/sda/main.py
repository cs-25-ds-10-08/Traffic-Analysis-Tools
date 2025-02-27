from pandas import DataFrame
import random

from helper.util import init, get_src_and_dst_port


def main():
    settings, data = init(
        "Statistical Disclosure Attack Analysis Tool", description="Uses timing perform de-anonymization"
    )
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

    sorted_counter = list(sorted(counter.items(), key=lambda item: item[1], reverse=True))
    print(f"Target: {settings['target']}\nMost likely: {sorted_counter[0][0]}\nActual: {settings['actual']}")
    print(sorted_counter)


def update_counts(inc: int, counter: dict[int, int], data: DataFrame, settings: dict[str, int]):
    for _, row in data.iterrows():
        destination: int = get_src_and_dst_port(row.Info)["dst"]
        if settings["target"] != destination != settings["server_port"]:
            if destination in counter.keys():
                counter[destination] += inc
            else:
                counter[destination] = inc


if __name__ == "__main__":
    main()
