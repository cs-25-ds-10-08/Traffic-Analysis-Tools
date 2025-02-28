from pandas import DataFrame
import pygmtools as pygm

from helper.util import print_result, sda_profiling


pygm.set_backend('numpy')


def main(settings: dict[str, int], data: DataFrame):
    profiles = sda_profiling(settings, data)
        
    df = DataFrame.from_dict(profiles)
    df.fillna(0, inplace=True)
    data = pygm.sinkhorn(df.to_numpy())
    df_new = DataFrame(data, index=df.index, columns=df.columns).to_dict()
    
    print_result(df_new, settings)

