from pandas import DataFrame

from helper.util import print_result, sda_profiling

def main(settings: dict[str, str], data: DataFrame):
    profiles = sda_profiling(settings, data)
    
    print_result(profiles.div(profiles.sum(axis=1), axis=0).to_dict(), settings)

