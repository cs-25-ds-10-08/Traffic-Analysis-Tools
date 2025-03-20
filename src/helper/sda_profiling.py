from math import ceil
from pandas import DataFrame

from helper.util import Identifier, Profile, Settings, get_src_and_dst


def sda_profiling(settings: Settings, data: DataFrame) -> DataFrame:
    profiles: dict[Identifier, Profile] = {}
    initial_time = data.iloc[0].Time
    chunk_amount = ceil((data.iloc[-1].Time - initial_time) / settings["epoch"])
    chunks: list[dict[str, list[Identifier]]] = [
        _chunks_by_snd_rcv(
            data.loc[
                (initial_time + chunk_num * settings["epoch"] < data.Time)
                & (data.Time <= initial_time + (chunk_num + 1) * settings["epoch"])
            ],
            settings,
        )
        for chunk_num in range(0, chunk_amount)
    ]

    for chunk in chunks:
        for sender in chunk["src"]:
            _update_profile(sender, len(chunk["src"]), chunk["dst"], profiles)

    return DataFrame.from_dict(profiles).fillna(0)


def _update_profile(
    sender: Identifier, senders_amount: int, receivers: list[Identifier], profiles: dict[Identifier, Profile]
):
    for receiver in receivers:
        if sender == receiver:
            continue

        if sender not in profiles:
            profiles[sender] = {}

        if receiver not in profiles[sender]:
            profiles[sender][receiver] = 0

        profiles[sender][receiver] += 1 / senders_amount


def _chunks_by_snd_rcv(chunk: DataFrame, settings: Settings) -> dict[str, list[Identifier]]:
    sr: dict[str, list[Identifier]] = {"src": [], "dst": []}
    for _, row in chunk.iterrows():
        ports: dict[str, Identifier] = get_src_and_dst(row)
        if ports["src"] not in settings["server"]:
            sr["src"].append(ports["src"])
        else:
            sr["dst"].append(ports["dst"])
    return sr
