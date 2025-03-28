from math import ceil
from pandas import DataFrame, Series
from itertools import compress

from helper.util import Identifier, Profile, Settings

from tqdm import tqdm


def sda_profiling(settings: Settings, data: DataFrame) -> DataFrame:
    local = is_local(data.iloc[0])
    profiles: DataFrame = DataFrame().astype(np.float32)
    initial_time = data.iloc[0].Time
    chunk_amount = ceil((data.iloc[-1].Time - initial_time) / settings["epoch"])
    data["src_port"] = data["src_port"].astype(str)
    data["dst_port"] = data["dst_port"].astype(str)

    for chunk_num in tqdm(range(0, chunk_amount)):
        start_time = initial_time + chunk_num * settings["epoch"]
        end_time = start_time + settings["epoch"]

        _update_profile(
            *_chunks_by_snd_rcv(
                data[(data.Time > start_time) & (data.Time <= end_time)],
                settings,
            ),
            profiles,
        )
    print(profiles.keys())
    return DataFrame.from_dict(profiles).fillna(0)


def _update_profile(senders: list[Identifier], receivers: list[Identifier], profiles: dict[Identifier, Profile]):
    for sender in senders:
        for receiver in receivers:
            if sender == receiver:
                continue

            if sender not in profiles:
                profiles[sender] = {}

            if receiver not in profiles[sender]:
                profiles[sender][receiver] = 0

            profiles[sender][receiver] += 1 / len(senders)


def _chunks_by_snd_rcv(chunk, settings: Settings) -> tuple[list[Identifier], list[Identifier]]:
    if chunk.shape[0] <= 1:
        return [], []

    src_ports = chunk["src_port"]
    dst_ports = chunk["dst_port"]

    receivers_mask = [port in settings["server"] for port in src_ports]
    senders_mask = map(lambda x: not x, receivers_mask)

    return list(compress(src_ports, senders_mask)), list(compress(dst_ports, receivers_mask))
