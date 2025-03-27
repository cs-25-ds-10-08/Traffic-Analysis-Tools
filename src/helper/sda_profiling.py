from math import ceil
from pandas import DataFrame
from tqdm import tqdm

import pandas as pd
import numpy as np

from helper.util import Identifier, Profile, Settings, get_src_and_dst, is_local


def sda_profiling(settings: Settings, data: DataFrame) -> DataFrame:
    local = is_local(data.iloc[0])
    profiles: DataFrame = DataFrame()
    initial_time = data.iloc[0].Time
    chunk_amount = ceil((data.iloc[-1].Time - initial_time) / settings["epoch"])
    for chunk_num in tqdm(range(0, chunk_amount), desc="Creating chunks"):
        profiles = _update_profile(
            data.loc[
                (initial_time + chunk_num * settings["epoch"] <= data.Time)
                & (data.Time < initial_time + (chunk_num + 1) * settings["epoch"])
            ],
            local,
            settings,
            profiles
        )
    profiles.index = profiles.index.astype(str)
    profiles.columns = profiles.columns.astype(str)
    # profiles = profiles.sort_index(axis=0).sort_index(axis=1)
    for label in profiles.index.intersection(profiles.columns):
        profiles.loc[label, label] = 0
    # np.fill_diagonal(profiles.values, 0)
    print(profiles)

    return profiles


def _update_profile(
    chunk: DataFrame, local: bool, settings: Settings, profiles: DataFrame
) -> DataFrame:
    src = "src_port" if local else "Source"
    dst = "dst_port" if local else "Destination"
    senders: DataFrame = chunk.loc[chunk[src].isin([index for index in chunk[src] if str(index) not in settings["server"]])]
    receivers: DataFrame = chunk.loc[chunk[dst].isin([index for index in chunk[dst] if str(index) not in settings["server"]])]
    if senders.shape[0] == 0:
        return profiles
    
    profiles_update = pd.DataFrame(1 / senders.shape[0], index=pd.Index(senders[src]), columns=pd.Index(receivers[dst]))
    profiles_update = profiles_update.groupby(level=0).sum().T.groupby(level=0).sum().T
    profiles = profiles.add(profiles_update, fill_value=0).fillna(0)

    return profiles

