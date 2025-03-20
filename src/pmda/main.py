from typing import Callable
from pandas import DataFrame

from helper.util import Settings


def main(settings: Settings, data: DataFrame, profiler: Callable[[Settings, DataFrame], DataFrame]):
    pass
