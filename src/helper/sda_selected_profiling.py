from pandas import DataFrame

from helper.util import Identifier, Profile, Settings, get_src_and_dst

RemaningTime = float


def sda_selected_profiling(settings: Settings, data: DataFrame) -> DataFrame:
    epoch = settings["epoch"]
    server = settings["server"]

    profiles: dict[Identifier, Profile] = {}
    sender_buffer: dict[Identifier, RemaningTime] = {}

    prev_time = data.iloc[0].Time

    for _, row in data.iterrows():
        _update_buffer(sender_buffer, row.Time - prev_time)

        src_dst = get_src_and_dst(row)

        if src_dst["src"] in server:
            _update_profiles(sender_buffer, src_dst["dst"], profiles)
        else:
            sender_buffer[src_dst["src"]] = epoch

        prev_time = row.Time

    return DataFrame.from_dict(profiles).fillna(0)


def _update_profiles(
    sender_buffer: dict[Identifier, RemaningTime], receiver: Identifier, profiles: dict[Identifier, Profile]
):
    senders = sender_buffer.keys()
    for sender in senders:
        if sender == receiver:
            continue

        if sender not in profiles:
            profiles[sender] = {}

        if receiver not in profiles[sender]:
            profiles[sender][receiver] = 0

        profiles[sender][receiver] += 1 / len(senders)


def _update_buffer(sender_buffer: dict[Identifier, RemaningTime], elapsed_time: float):
    for sender, time in list(sender_buffer.items()):
        if time - elapsed_time <= 0:
            sender_buffer.pop(sender)
        else:
            sender_buffer[sender] -= elapsed_time
