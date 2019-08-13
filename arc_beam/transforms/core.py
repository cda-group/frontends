from arc_beam.transforms.ptransform import PTransform
# from grizzly.grizzly import LazyOpResult
from arc_beam.transforms.window import FixedWindows

__all__ = [
    # 'DoFn',
    # 'CombineFn',
    # 'PartitionFn',
    'ParDo',
    'FlatMap',
    'Map',
    'Filter',
    # 'CombineGlobally',
    # 'CombinePerKey',
    # 'CombineValues',
    # 'GroupByKey',
    # 'Partition',
    # 'Windowing',
    'WindowInto',
    # 'Flatten',
    # 'Create',
    # 'Impulse',
    # 'RestrictionProvider'
]


class ParDo(PTransform):

    def get_sink_type(self):
        return 'streamappender[?]'


class WindowInto(PTransform):
    def __init__(self, kind):
        if isinstance(kind, FixedWindows):
            self.kind = kind
        else:
            raise TypeError('Unsupported window kind', kind)

    def get_sink_type(self):
        return self.kind.get_sink_type()

    def stage(self, input_elem):
        return self.kind.generate(input_elem)


class Map(ParDo):
    def __init__(self, fn):
        self.fn = fn

    def stage(self, input_elem):
        output_elem = self.fn(input_elem)
        return output_elem, "|b,i,e| merge(b,\n{}\n)".format(output_elem.generate())


class Filter(ParDo):
    def __init__(self, fn):
        self.fn = fn

    def stage(self, input_elem):
        output_elem = input_elem
        return output_elem, "|b,i,e| if(\n{}\n, merge(b, e), b)".format(output_elem.generate())


class Flatten(ParDo):
    def stage(self, input_elem):
        return input_elem, "|b,i,e| for(\n{}\n, b, |b2,i2,e2| merge(b2,e2))".format(input_elem.generate())


class FlatMap(ParDo):
    def __init__(self, fn):
        self.fn = fn

    def stage(self, input_elem):
        output_elem = self.fn(input_elem)
        return output_elem, "|b,i,e| for(\n{}\n, b, |b2,i2,x2| merge(b2,x2))".format(output_elem.generate())
