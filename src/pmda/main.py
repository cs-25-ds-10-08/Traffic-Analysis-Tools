from math import ceil
from pandas import DataFrame
from pprint import pprint

from helper.util import init, get_src_and_dst_port

Port = int
Profile = dict[Port, float]


def main():
    settings, data = init(
        "Perfect Matching Disclosure Attack Analysis Tool", description="Uses timing perform de-anonymization"
    )
    profiles: dict[Port, Profile] = {}
    occurences: dict[Port, int] = {}

    initial_time = data.iloc[0].Time
    chunk_amount = ceil((data.iloc[-1].Time - initial_time) / settings["epoch"])
    chunks: list[dict[str, list[Port]]] = [
        chunks_by_snd_rcv(
            data.loc[
                (initial_time + chunk_nr * settings["epoch"] < data.Time)
                & (data.Time <= initial_time + (chunk_nr + 1) * settings["epoch"])
            ],
            settings,
        )
        for chunk_nr in range(0, chunk_amount)
    ]

    for chunk in chunks:
        add_senders_to_occurences(occurences, chunk["snd"])
        for sender in chunk["snd"]:
            update_profile(sender, len(chunk["snd"]), chunk["rcv"], profiles)

    for snd_port, profile in profiles.items():
        for rcv_port, score in profile.items():
            profiles[snd_port][rcv_port] = score / occurences[snd_port]

        total = sum(profile.values())
        for rcv_port, score in profile.items():
            profiles[snd_port][rcv_port] = score / total

        profiles[snd_port] = dict(sorted(profile.items(), key=lambda item: item[1]))

    pprint(profiles[settings["target"]])

    # sorted_counter = list(sorted(counter.items(), key=lambda item: item[1], reverse=True))
    # print(f"Target: {settings['target']}\nMost likely: {sorted_counter[0][0]}\nActual: {settings['actual']}")
    # print(sorted_counter)


def add_senders_to_occurences(occurences: dict[Port, int], senders: list[Port]):
    for sender in senders:
        if sender in occurences:
            occurences[sender] += 1
        else:
            occurences[sender] = 1


def update_profile(sender: Port, senders_amount: int, receivers: list[Port], profiles: dict[Port, Profile]):
    for receiver in receivers:
        if sender == receiver:
            continue

        if sender not in profiles:
            profiles[sender] = {}

        if receiver in profiles[sender]:
            profiles[sender][receiver] += 1 / senders_amount
        else:
            profiles[sender][receiver] = 1 / senders_amount


def chunks_by_snd_rcv(chunk: DataFrame, settings: dict[str, int]) -> dict[str, list[Port]]:
    sender: list[Port] = []
    receiver: list[Port] = []
    for _, row in chunk.iterrows():
        ports = get_src_and_dst_port(row.Info)
        if ports["src"] != settings["server_port"]:
            sender.append(ports["src"])
        else:
            receiver.append(ports["dst"])
    return {"snd": sender, "rcv": receiver}


if __name__ == "__main__":
    main()
