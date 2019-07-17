import operator

from arc_beam.pvalue import PBegin

__all__ = [
    'PTransform',
]


class PTransform(object):
    pipeline = None
    input_type = None
    output_type = None

    def __init__(self, label=None):
        self.label = label

    def __ror__(self, pipeline):  # pipeline | self
        self.pipeline = pipeline
        self.process(PBegin(pipeline))

    def __rrshift__(self, label):  # name >> self
        self.label = label

    def expand(self, inputs):
        raise NotImplementedError

    def process(self, inputs):
        raise NotImplementedError

    def with_input_types(self, input_type):
        self.input_type = input_type

    def with_output_types(self, output_type):
        self.output_type = output_type
