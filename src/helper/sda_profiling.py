from collections import defaultdict
from math import ceil
from pandas import DataFrame, Series
from itertools import compress
from tqdm import tqdm

from helper.util import Identifier, Profile, Settings, is_local


def sda_profiling(settings: Settings, data: DataFrame) -> DataFrame:
    src, dst = ("src_port", "dst_port") if is_local(data.iloc[0]) else ("Source", "Destination")
    profiles: defaultdict[Identifier, Profile] = defaultdict(lambda: defaultdict(float))
    initial_time = data.iloc[0].Time
    chunk_amount = ceil((data.iloc[-1].Time - initial_time) / settings["epoch"])
    data[src] = data[src].astype(str)
    data[dst] = data[dst].astype(str)

    for chunk_num in tqdm(range(0, chunk_amount)):
        start_time = initial_time + chunk_num * settings["epoch"]
        end_time = start_time + settings["epoch"]
        chunk = data.iloc[data.Time.searchsorted(start_time) : data.Time.searchsorted(end_time, side="right")]

        if chunk.shape[0] <= 1:
            continue

        _update_profile(
            *_chunks_by_snd_rcv(chunk[src], chunk[dst], settings["server"]),
            profiles,
        )

    return DataFrame.from_dict(profiles).fillna(0)


def _update_profile(
    senders: list[Identifier],
    receivers: list[Identifier],
    profiles: defaultdict[Identifier, Profile],
):
    for sender in senders:
        for receiver in receivers:
            if sender == receiver:
                continue

            profiles[sender][receiver] += 1 / len(senders)


def _chunks_by_snd_rcv(
    src_ports: Series, dst_ports: Series, servers: list[str]
) -> tuple[list[Identifier], list[Identifier]]:
    receivers_mask = [port in servers for port in src_ports]
    senders_mask = map(lambda x: not x, receivers_mask)

    return list(compress(src_ports, senders_mask)), list(compress(dst_ports, receivers_mask))
