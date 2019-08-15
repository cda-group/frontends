from arc_beam.transforms.ptransform import PTransform
from baloo.core.utils import LazyArrayResult
from arc_beam.transforms.window import FixedWindows
from baloo.weld import python_expr_to_weld_expr

__all__ = [
    # 'DoFn',
    'CombineFn',
    # 'PartitionFn',
    'ParDo',
    'FlatMap',
    'Map',
    'Filter',
    # 'CombineGlobally',
    # 'CombinePerKey',
    # 'CombineValues',
    'GroupByKey',
    # 'Partition',
    # 'Windowing',
    'WindowInto',
    # 'Flatten',
    # 'Create',
    # 'Impulse',
    # 'RestrictionProvider'
]


class ParDo(PTransform):

    def stage(self, input_elem):
        pass

    def get_sink(self):
        return 'streamappender[?]'


class WindowInto(PTransform):
    def __init__(self, kind):
        if isinstance(kind, FixedWindows):
            self.kind = kind
        else:
            raise TypeError('Unsupported window kind', kind)

    def get_sink(self):
        return self.kind.get_sink_type()

    def stage(self, input_elem):
        return self.kind.generate(input_elem)


class Map(ParDo):
    def __init__(self, fn):
        self.fn = fn

    def stage(self, input_elem):
        output_elem = self.fn(input_elem)
        output_elem = python_expr_to_weld_expr(output_elem)
        return output_elem, "|sb,si,se| merge(sb,\n{}\n)".format(output_elem.generate())


class Filter(ParDo):
    def __init__(self, fn):
        self.fn = fn

    def stage(self, input_elem):
        output_elem = input_elem
        return output_elem, "|sb,si,se| if(\n{}\n, merge(sb, se), sb)".format(output_elem.generate())


class Flatten(ParDo):
    def stage(self, input_elem):
        return input_elem, "|sb,si,se| for(\n{}\n, sb, |sb2,si2,se2| merge(sb2,se2))".format(input_elem.generate())


class FlatMap(ParDo):
    def __init__(self, fn):
        self.fn = fn

    def stage(self, input_elem):
        output_elem = self.fn(input_elem)
        return output_elem, "|sb,si,se| for(\n{}\n, sb, |sb2,si2,sx2| merge(sb2,sx2))".format(output_elem.generate())


class GroupByKey(PTransform):

    @staticmethod
    def get_iter(source):
        return "keyby({})".format(source)

    def get_sink(self):
        return "streamappender"

    def stage(self, input_elem):
        from baloo.weld import WeldVec, WeldStruct
        key_type = input_elem.weld_type.field_types[0]
        val_type = input_elem.weld_type.field_types[1]
        from baloo.weld import lazify
        output_elem = lazify(WeldStruct([key_type, WeldVec(val_type)]))
        groupby = "tovec(result(for(se, groupmerger, |b,i,e| merge(b,e))))"
        return output_elem, "|sb,si,se| for({}, sb, |b,i,e| merge(b,e))".format(groupby)


class CombineFn(PTransform):
    def __init__(self, fn):
        self.fn = fn

    def get_sink(self):
        return "streamappender"

    def stage(self, input_elems):
        output_elem = self.fn(input_elems)
        return output_elem, "|sb,si,se| merge(sb,\n{}\n)".format(output_elem.generate())

    def without_defaults(self):
        pass
