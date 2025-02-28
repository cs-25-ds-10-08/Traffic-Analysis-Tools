from pandas import DataFrame

from helper.util import print_result, sda_profiling

def main(settings: dict[str, int], data: DataFrame):
    profiles = sda_profiling(settings, data)

    for snd_port, profile in profiles.items():
        total = sum(profile.values())
        for rcv_port, score in profile.items():
            profiles[snd_port][rcv_port] = score / total

        profiles[snd_port] = dict(sorted(profile.items(), key=lambda item: item[1]))

    print_result(profiles, settings)

