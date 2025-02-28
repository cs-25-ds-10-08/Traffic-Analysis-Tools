import re
from math import ceil
from pandas import DataFrame


Port = int
Profile = dict[Port, float]


def print_result(profiles: dict[Port, Profile], settings: dict[str, int]):
    result = max(profiles[settings["target"]].items(), key=lambda x: x[1])
    print(f"Target: {settings['target']}\nActual: {settings['actual']}\nMost likely: {result[0]}\nWith propability: {round(result[1] * 100, 2)}%")


def get_src_and_dst_port(info: str) -> dict[str, int]:
    # If this fails, you most likely have an error in your dataset
    res = re.findall(r"(\d+)\s*>\s*(\d+).*", info)[0]
    return {"src": int(res[0]), "dst": int(res[1])}


def sda_profiling(settings: dict[str, int], data: DataFrame) -> dict[Port, Profile]:
    profiles: dict[Port, Profile] = {}

    initial_time = data.iloc[0].Time
    chunk_amount = ceil((data.iloc[-1].Time - initial_time) / settings["epoch"])
    chunks: list[dict[str, list[Port]]] = [
        _chunks_by_snd_rcv(
            data.loc[
                (initial_time + chunk_nr * settings["epoch"] < data.Time)
                & (data.Time <= initial_time + (chunk_nr + 1) * settings["epoch"])
            ],
            settings,
        )
        for chunk_nr in range(0, chunk_amount)
    ]

    for chunk in chunks:
        for sender in chunk["snd"]:
            _update_profile(sender, len(chunk["snd"]), chunk["rcv"], profiles)
    
    return profiles

    
def _update_profile(sender: Port, senders_amount: int, receivers: list[Port], profiles: dict[Port, Profile]):
    for receiver in receivers:
        if sender == receiver:
            continue

        if sender not in profiles:
            profiles[sender] = {}

        if receiver in profiles[sender]:
            profiles[sender][receiver] += 1 / senders_amount
        else:
            profiles[sender][receiver] = 1 / senders_amount


def _chunks_by_snd_rcv(chunk: DataFrame, settings: dict[str, int]) -> dict[str, list[Port]]:
    sender: list[Port] = []
    receiver: list[Port] = []
    for _, row in chunk.iterrows():
        ports = get_src_and_dst_port(row.Info)
        if ports["src"] != settings["server_port"]:
            sender.append(ports["src"])
        else:
            receiver.append(ports["dst"])
    return {"snd": sender, "rcv": receiver}

