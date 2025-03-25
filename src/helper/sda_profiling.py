import contextlib
from math import ceil
from pandas import DataFrame
from tqdm import tqdm

import pandas as pd

from helper.util import Identifier, Profile, Settings, get_src_and_dst, is_local




def sda_profiling(settings: Settings, data: DataFrame) -> DataFrame:
    local = is_local(data.iloc[0])
    profiles: dict[Identifier, Profile] = {}
    initial_time = data.iloc[0].Time
    chunk_amount = ceil((data.iloc[-1].Time - initial_time) / settings["epoch"])
    for chunk_num in tqdm(range(0, chunk_amount), desc="Creating chunks"):
        _update_profile(
            data.loc[
                (initial_time + chunk_num * settings["epoch"] <= data.Time)
                & (data.Time < initial_time + (chunk_num + 1) * settings["epoch"])
            ],
            local,
            settings,
            profiles
        )

    return DataFrame.from_dict(profiles).fillna(0)


def _update_profile(
    chunk: DataFrame, local: bool, settings: Settings, profiles: DataFrame
):
    src = "src_port" if local else "Source"
    dst = "dst_port" if local else "Destination"
    print(chunk)
    print(chunk[dst].index)
    print([index for index in chunk[dst].index if index in settings["server"]])
    senders: DataFrame = chunk.iloc[[index for index in chunk[dst].index if index in settings["server"]]]
    receivers: DataFrame = chunk.loc[chunk[src].isin(settings["server"])]
    print(senders)
    if senders.shape[0] == 0:
        print(senders.shape)
        exit()
        return
    print(senders)
    odds = 1 / senders.shape[0]
    
    print(profiles)
    profiles_update = pd.DataFrame(odds, pd.Index(senders[src]), columns=pd.Index(receivers[dst]));
    profiles += profiles_update
    print("+")
    print(profiles_update)
    print("=")
    print(profiles)

    exit()



    for (_, sender) in senders.iterrows():
        for (_, receiver) in receivers.iterrows():
            if sender[src] == receiver[dst]:
                continue

            if sender[src] not in profiles:
                profiles[sender[src]] = {}

            if receiver[dst] not in profiles[sender[src]]:
                profiles[sender[src]][receiver[dst]] = 0

            profiles[sender[src]][receiver[dst]] += 1 / senders.shape[0]
    


    for receiver in receivers:
        if sender == receiver:
            continue

        if sender not in profiles:
            profiles[sender] = {}

        if receiver not in profiles[sender]:
            profiles[sender][receiver] = 0

        profiles[sender][receiver] += 1 / senders_amount


def __update_profile(
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
