from collections import Counter, defaultdict
from typing import TypedDict
from pandas import DataFrame
from tqdm import tqdm

from helper.util import Identifier, Profile, DenimSettings, get_src_and_dst

RemaningTime = float


class Events(TypedDict):
    id: list[Identifier]
    time: list[float]


def sda_selected_profiling(settings: DenimSettings, data: DataFrame) -> DataFrame:
    server = settings["server"]

    burst_events: Events = {"id": [], "time": []}
    burst_events_candidates: defaultdict[Identifier, list[RemaningTime]] = defaultdict(list[RemaningTime])
    receivers: Events = {"id": [], "time": []}

    prev_time: float = data.iloc[0].Time

    for row in tqdm(data.itertuples(), total=data.shape[0], desc="Preparing senders and receivers"):
        time: float = row.Time  # type: ignore
        elapsed_time = time - prev_time
        burst_events_candidates = _update_candidate_buffer(burst_events_candidates, elapsed_time)

        src_dst = get_src_and_dst(row)
        sender = src_dst["src"]
        receiver = src_dst["dst"]

        if sender in server:
            receivers["id"].append(receiver)
            receivers["time"].append(time)
        else:
            burst_events_candidates[sender].append(settings["dt"])
            _mayber_append(
                burst_events,
                burst_events_candidates,
                sender,
                settings["n"],
                time,
            )

        prev_time = time

    print(Counter(burst_events["id"]))

    profiles = _make_profiles(DataFrame(burst_events), DataFrame(receivers), settings["n"])

    return DataFrame.from_dict(profiles).fillna(0)


def _make_profiles(burst_events: DataFrame, receivers: DataFrame, n: int) -> dict[Identifier, Profile]:
    profiles: dict[Identifier, Profile] = defaultdict(lambda: defaultdict(float))

    for row in tqdm(burst_events.itertuples(), total=burst_events.shape[0], desc="Making profiles"):
        next = -1
        for j in range(row.Index + 1, burst_events.shape[0]):
            if burst_events.loc[row.Index].id == burst_events.iloc[j].id:
                next = j
                burst_events_slice = burst_events.iloc[
                    burst_events.time.searchsorted(row.time) : burst_events.time.searchsorted(
                        burst_events.iloc[next].time, side="right"
                    )
                ]
                if burst_events_slice.empty:
                    break
                burst_events_slice = burst_events_slice.drop_duplicates(subset="id", keep="last")

                pr = receivers.iloc[
                    receivers.time.searchsorted(row.time) : receivers.time.searchsorted(
                        burst_events_slice.iloc[-1].time, side="right"
                    )
                ]
                if pr.empty:
                    continue

                potential_receivers = _filter_potential_receivers(
                    burst_events_slice,
                    pr,
                    row.id,
                    n,
                )
                if potential_receivers.empty:
                    continue

                amount = potential_receivers.shape[0] - Counter(potential_receivers["id"])[row.id]
                for potential_receiver in potential_receivers.itertuples():
                    if row.id != potential_receiver.id:
                        profiles[row.id][potential_receiver.id] += 1 / amount

                break

    return profiles


def _filter_potential_receivers(burst_events: DataFrame, receivers: DataFrame, id: Identifier, n: int) -> DataFrame:
    count = 0
    counts = defaultdict(int)
    ids = burst_events.id.values

    for receiver in receivers.iloc[::-1].itertuples():
        if count < n:
            if receiver.id == id:
                count += 1
            continue
        if receiver.id in ids and receiver.time < burst_events.loc[burst_events.id == receiver.id].iloc[0].time:
            counts[receiver.id] += 1

    return burst_events[[counts[row.id] >= n for row in burst_events.itertuples()]]


def _update_candidate_buffer(
    sender_buffer: defaultdict[Identifier, list[RemaningTime]], elapsed_time: float
) -> defaultdict[Identifier, list[RemaningTime]]:
    for sender, times in sender_buffer.items():
        sender_buffer[sender] = [
            remaning_time - elapsed_time for remaning_time in times if remaning_time - elapsed_time > 0
        ]

    return sender_buffer


def _mayber_append(
    burst_events: Events,
    burst_events_candidates: defaultdict[Identifier, list[RemaningTime]],
    sender: Identifier,
    n: int,
    time: float,
):
    if len(burst_events_candidates[sender]) == n:
        # print(f"{sender}: {[time[1] for time in burst_events_candidates[sender]]} \n")
        burst_events["id"].append(sender)
        burst_events["time"].append(time)
        burst_events_candidates.pop(sender)
