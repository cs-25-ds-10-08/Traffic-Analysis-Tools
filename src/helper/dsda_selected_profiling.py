from collections import Counter, defaultdict
from typing import Optional, TypedDict
from pandas import DataFrame
from tqdm import tqdm

from helper.util import Identifier, Profile, DenimSettings, get_src_and_dst

RemaningTime = float
Burst = list[float]


class Events(TypedDict):
    id: list[Identifier]
    time: list[float]
    size: Optional[list[int]]


def sda_selected_profiling(settings: DenimSettings, data: DataFrame) -> DataFrame:
    server = settings["server"]

    burst_events: Events = {"id": [], "time": [], "size": []}
    chunks_by_id: dict[str, list[Burst]] = {}
    receivers: Events = {"id": [], "time": [], "size": None}

    for row in tqdm(data.itertuples(), total=data.shape[0], desc="Preparing senders and receivers"):
        time: float = row.Time  # type: ignore

        src_dst = get_src_and_dst(row)
        sender = src_dst["src"]
        receiver = src_dst["dst"]

        if sender in server:
            receivers["id"].append(receiver)
            receivers["time"].append(time)
        else:
            if sender not in chunks_by_id:
                # First time seeing this identifier
                chunks_by_id[sender] = [[time]]
            else:
                last_chunk = chunks_by_id[sender][-1]
                # Compare with last timestamp in the last chunk
                if time - last_chunk[-1] <= settings["dt"]:
                    last_chunk.append(time)
                else:
                    # Start a new chunk
                    if burst_events["size"] is not None and len(last_chunk) >= 3:
                        burst_events["id"].append(sender)
                        burst_events["time"].append(time)
                        burst_events["size"].append(len(last_chunk))
                        chunks_by_id[sender].pop()
                    chunks_by_id[sender].append([time])

    if burst_events["size"] is not None:
        combined = list(zip(burst_events["id"], burst_events["time"], burst_events["size"]))
        sorted_combined = sorted(combined, key=lambda x: x[1])
        burst_events["id"], burst_events["time"], burst_events["size"] = map(list, zip(*sorted_combined))

    print(Counter(burst_events["id"]))

    profiles = _make_profiles(DataFrame(burst_events), DataFrame(receivers))

    print(DataFrame.from_dict(profiles).fillna(0))

    return DataFrame.from_dict(profiles).fillna(0)


def _make_profiles(burst_events: DataFrame, receivers: DataFrame) -> dict[Identifier, Profile]:
    profiles: dict[Identifier, Profile] = defaultdict(lambda: defaultdict(float))

    for row in tqdm(burst_events.itertuples(), total=burst_events.shape[0], desc="Making profiles"):
        for j in range(row.Index + 1, burst_events.shape[0]):  # type: ignore
            if row.id == burst_events.iloc[j].id:
                start = row.time
                end = burst_events.iloc[j].time

                burst_events_slice = burst_events.iloc[
                    burst_events.time.searchsorted(start) : burst_events.time.searchsorted(end, side="right")
                ]
                if burst_events_slice.empty:
                    break
                burst_events_slice = burst_events_slice.drop_duplicates(subset="id", keep="last")

                pr = receivers.iloc[receivers.time.searchsorted(start) : receivers.time.searchsorted(end, side="right")]
                if pr.empty:
                    break

                potential_receivers = _filter_potential_receivers(
                    burst_events_slice,
                    pr,
                    row.id,  # type: ignore
                    row.size,  # type: ignore
                )
                if potential_receivers.empty:
                    continue

                potential_receivers = potential_receivers[potential_receivers["id"] != row.id]
                for potential_receiver in potential_receivers.itertuples():
                    profiles[row.id][potential_receiver.id] += 1 / potential_receivers.shape[0]  # type: ignore

                break

    return profiles


def _filter_potential_receivers(burst_events: DataFrame, receivers: DataFrame, id: Identifier, n: int) -> DataFrame:
    count = 0
    counts = {}
    i = burst_events.shape[0] - 1

    for receiver in receivers.iloc[::-1].itertuples():
        while i >= 0:
            burst_event = burst_events.iloc[i]
            if receiver.time < burst_event.time:
                if burst_event["size"] <= count and burst_event.id not in counts:
                    counts[burst_event.id] = 0
                i -= 1
            else:
                break

        if receiver.id == id:
            count += 1
        elif receiver.id in counts:
            counts[receiver.id] += 1

    return burst_events[[row.id in counts and counts[row.id] >= n for row in burst_events.itertuples()]]
