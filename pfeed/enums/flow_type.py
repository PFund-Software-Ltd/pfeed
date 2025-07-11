from enum import StrEnum


class FlowType(StrEnum):
    native = 'native'
    prefect = 'prefect'
    # TODO: spark? (spark streaming)