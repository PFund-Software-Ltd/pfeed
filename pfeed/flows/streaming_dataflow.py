from pfeed.flows.dataflow import DataFlow
from bytewax.dataflow import Dataflow as BytewaxDataFlow


class StreamingDataFlow(DataFlow, BytewaxDataFlow):
    pass