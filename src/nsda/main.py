from typing import Callable
from pandas import DataFrame
import pygmtools as pygm

from helper.util import print_result, Settings


pygm.set_backend("numpy")


def main(settings: Settings, data: DataFrame, profiler: Callable[[Settings, DataFrame], DataFrame]):
    profiles = profiler(settings, data)

    print_result(
        DataFrame(pygm.sinkhorn(profiles.to_numpy()), index=profiles.index, columns=profiles.columns).to_dict(),
        settings,
    )
