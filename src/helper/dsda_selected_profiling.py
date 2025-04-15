from collections import defaultdict
from pandas import DataFrame
from tqdm import tqdm

from helper.util import Identifier, Profile, DenimSettings, get_src_and_dst

RemaningTime = float


def sda_selected_profiling(settings: DenimSettings, data: DataFrame) -> DataFrame:
    epoch = settings["epoch"]
    server = settings["server"]

    profiles: dict[Identifier, Profile] = {}
    sender_buffer: list[tuple[Identifier, RemaningTime]] = []
    sender_buffer_candidates: defaultdict[Identifier, list[RemaningTime]] = defaultdict(list[RemaningTime])

    foo = defaultdict(int)

    prev_time: float = data.iloc[0].Time

    avg = 0

    for row in tqdm(data.itertuples(), total=data.shape[0], desc="Selected profiling"):
        row_time: float = row.Time  # type: ignore
        elapsed_time = row_time - prev_time
        sender_buffer = _update_buffer(sender_buffer, elapsed_time)
        sender_buffer_candidates = _update_candidate_buffer(sender_buffer_candidates, elapsed_time)

        src_dst = get_src_and_dst(row)

        if src_dst["src"] in server:
            _update_profiles(sender_buffer, src_dst["dst"], profiles)
        else:
            _mayber_append(
                sender_buffer,
                sender_buffer_candidates,
                src_dst["src"],
                epoch,
                settings["dt"],
                settings["n"],
                foo,
            )

        prev_time = row_time
        avg += len(sender_buffer)

    print(f"Average sender buffer len: {avg / data.shape[0]}")

    print(foo)

    return DataFrame.from_dict(profiles).fillna(0)


def _update_profiles(
    sender_buffer: list[tuple[Identifier, RemaningTime]], receiver: Identifier, profiles: dict[Identifier, Profile]
):
    for sender, _ in sender_buffer:
        if sender == receiver:
            continue

        if sender not in profiles:
            profiles[sender] = {}

        if receiver not in profiles[sender]:
            profiles[sender][receiver] = 0

        profiles[sender][receiver] += 1 / len(sender_buffer)


def _update_buffer(
    sender_buffer: list[tuple[Identifier, RemaningTime]], elapsed_time: float
) -> list[tuple[Identifier, RemaningTime]]:
    return [(sender, time - elapsed_time) for sender, time in sender_buffer if time - elapsed_time > 0]


def _update_candidate_buffer(
    sender_buffer: defaultdict[Identifier, list[RemaningTime]], elapsed_time: float
) -> defaultdict[Identifier, list[RemaningTime]]:
    for sender, times in sender_buffer.items():
        sender_buffer[sender] = [time for time in times if time - elapsed_time > 0]

    return sender_buffer


def _mayber_append(
    sender_buffer: list[tuple[Identifier, RemaningTime]],
    sender_buffer_candidates: defaultdict[Identifier, list[RemaningTime]],
    sender: Identifier,
    epoch: float,
    dt: float,
    n: int,
    foo,
):
    sender_buffer_candidates[sender].append(dt)

    if len(sender_buffer_candidates[sender]) == n:
        foo[sender] += 1
        sender_buffer.append((sender, epoch))
        sender_buffer_candidates.pop(sender)
