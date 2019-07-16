import operator

from arc_beam.pvalue import PBegin

__all__ = [
    'PTransform',
    # 'ptransform_fn',
    # 'label_from_callable',
]


class PTransform(object):
    pipeline = None
    input_type = None
    output_type = None

    def __init__(self, label=None):
        self.label = label

    def __or__(self, transform):  # self | transform
        return ChainedPTransform(self, transform)

    def __ror__(self, pipeline):  # pipeline | self
        # TODO: This should be extended to handle any non-pvalue
        self.pipeline = pipeline
        input = PBegin(pipeline)
        pipeline.apply(self, input)

    def __rrshift__(self, label):  # name >> self
        self.label = label

    def expand(self, inputs):
        raise NotImplementedError

    def with_input_types(self, input_type):
        self.input_type = input_type

    def with_output_types(self, output_type):
        self.output_type = output_type


class ChainedPTransform(PTransform):
    def __init__(self, *parts):
        super(ChainedPTransform, self).__init__('|'.join(part.label for part in parts))
        self.parts = parts

    def __or__(self, transform):  # self | transform
        return ChainedPTransform(*(self.parts + (transform,)))

    def expand(self, input):
        return reduce(operator.or_, self.parts, input)


class PTransformWithSideInputs(PTransform):
    def expand(self, inputs):
        pass
