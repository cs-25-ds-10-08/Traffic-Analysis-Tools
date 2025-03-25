from pandas import DataFrame

from helper.util import Identifier, Profile, Settings, get_src_and_dst

RemaningTime = float


def sda_selected_profiling(settings: Settings, data: DataFrame) -> DataFrame:
    epoch = settings["epoch"]
    server = settings["server"]

    profiles: dict[Identifier, Profile] = {}
    sender_buffer: list[tuple[Identifier, RemaningTime]] = []

    prev_time = data.iloc[0].Time

    for _, row in data.iterrows():
        sender_buffer = _update_buffer(sender_buffer, row.Time - prev_time)

        print("-----------------------------------------")
        print(len(sender_buffer))
        # print(row.Time)
        print("-----------------------------------------")

        src_dst = get_src_and_dst(row)

        if src_dst["src"] in server:
            _update_profiles(sender_buffer, src_dst["dst"], profiles)
        else:
            sender_buffer.append((src_dst["src"], epoch))

        prev_time = row.Time

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


def _update_buffer(sender_buffer: list[tuple[Identifier, RemaningTime]], elapsed_time: float):
    return [(sender, time - elapsed_time) for sender, time in sender_buffer if time - elapsed_time > 0]

