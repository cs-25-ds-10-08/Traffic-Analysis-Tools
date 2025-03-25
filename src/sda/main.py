from typing import Callable
from pandas import DataFrame

from helper.util import Settings, print_result


def main(settings: Settings, data: DataFrame, profiler: Callable[[Settings, DataFrame], DataFrame]):
    profiles = profiler(settings, data)

    print_result(profiles.div(profiles.sum(axis=1), axis=0).to_dict(), settings)
