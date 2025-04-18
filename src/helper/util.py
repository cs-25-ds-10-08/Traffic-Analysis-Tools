from typing import TypedDict


class Settings(TypedDict):
    target: str
    actual: str
    epoch: float
    server: list[str]


class DenimSettings(Settings):
    dt: float
    n: int


Identifier = str
Profile = dict[Identifier, float]

LOCALHOST = ["127.0.0.1", "::1"]


def print_result(profiles: dict[Identifier, Profile], settings: Settings):
    result = max(profiles[settings["target"]].items(), key=lambda x: x[1])
    print(
        f"Target: {settings['target']}\nActual: {settings['actual']}\nMost likely: {result[0]}\nWith propability: {round(result[1] * 100, 2)}%"
    )


def get_src_and_dst(row) -> dict[str, Identifier]:
    if is_local(row):
        return {"src": str(row.src_port), "dst": str(row.dst_port)}
    else:
        return {"src": str(row.Source), "dst": str(row.Destination)}


def is_local(row) -> bool:
    return row.Source in LOCALHOST or row.Destination in LOCALHOST
