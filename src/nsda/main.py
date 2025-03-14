from pandas import DataFrame
import pygmtools as pygm

from helper.util import print_result, sda_profiling


pygm.set_backend('numpy')


def main(settings: dict[str, str], data: DataFrame):
    profiles = sda_profiling(settings, data)
        
    print_result(DataFrame(pygm.sinkhorn(profiles.to_numpy()), index=profiles.index, columns=profiles.columns).to_dict(), settings)

