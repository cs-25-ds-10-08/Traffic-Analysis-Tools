from pandas import DataFrame

from helper.util import print_result, DenimSettings
from helper.dsda_selected_profiling import sda_selected_profiling as profiler

import pygmtools as pygm


def main(settings: DenimSettings, data: DataFrame):
    profiles = profiler(settings, data)

    print_result(
        DataFrame(pygm.sinkhorn(profiles.to_numpy()), index=profiles.index, columns=profiles.columns).to_dict(),  # type: ignore
        settings,
    )
