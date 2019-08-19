from arc_beam.transforms.ptransform import PTransform
from baloo.core.utils import LazyArrayResult
from arc_beam.transforms.window import FixedWindows, TimestampedValue
from baloo.weld import python_expr_to_weld_expr

__all__ = [
    # 'DoFn',
    # 'PartitionFn',
    'ParDo',
    'FlatMap',
    'Map',
    'Filter',
    'CombineGlobally',
    'CombinePerKey',
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

    def stage(self, input_elem, metadata):
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

    def stage(self, input_elem, metadata):
        return self.kind.generate(input_elem)


class Map(ParDo):
    def __init__(self, fn):
        self.fn = fn

    def stage(self, input_elem, metadata):
        output_elem = self.fn(input_elem)
        if isinstance(output_elem, TimestampedValue):
            metadata['ts_extractor'] = output_elem.ts_extractor
            output_elem = output_elem.value
        output_elem = python_expr_to_weld_expr(output_elem)
        return output_elem, "|sb,si,se| merge(sb,\n{}\n)".format(output_elem.generate())


class Filter(ParDo):
    def __init__(self, fn):
        self.fn = fn

    def stage(self, input_elem, metadata):
        output_elem = input_elem
        condition = self.fn(input_elem)
        return output_elem, "|sb,si,se| if(\n{}\n, merge(sb, se), sb)".format(condition.generate())


class Flatten(ParDo):
    def stage(self, input_elem, metadata):
        return input_elem, "|sb,si,se| for(\n{}\n, sb, |sb2,si2,se2| merge(sb2,se2))".format(input_elem.generate())


class FlatMap(ParDo):
    def __init__(self, fn):
        self.fn = fn

    def stage(self, input_elem, metadata):
        output_elem = self.fn(input_elem)
        return output_elem, "|sb,si,se| for(\n{}\n, sb, |sb2,si2,sx2| merge(sb2,sx2))".format(output_elem.generate())


class GroupByKey(PTransform):

    @staticmethod
    def get_iter(source):
        return "keyby({})".format(source)

    def get_sink(self):
        return "streamappender"

    def stage(self, input_elem, metadata):
        from baloo.weld import WeldVec, WeldStruct, lazify
        key_type, val_type = input_elem.weld_type.field_types
        output_elem = lazify(WeldStruct([key_type, WeldVec(val_type)]))
        groupby = "tovec(result(for(se, groupmerger, |b,i,e| merge(b,e))))"
        return output_elem, "|sb,si,se| for(\n{}\n, sb, |sb2,si2,se2| merge(sb2,se2))".format(groupby)


# Takes a stream of key-value pairs and returns a stream of
class CombinePerKey(PTransform):

    def __init__(self, fn):
        self.fn = fn

    def get_sink(self):
        return "streamappender"

    # Create Vector[Key, Vector[Value]]
    # Map each Vector[Value] to Vector[NewValue]
    # Then output Vector[Key, Vector[NewValue]]
    def stage(self, input_elem, metadata):
        from baloo.weld import WeldVec, WeldStruct, lazify
        key_type, val_type = input_elem.weld_type.field_types
        combine = self.fn(lazify(WeldVec(val_type), weld_code="ge.$1"))
        output_elem = lazify(WeldVec(WeldStruct((key_type, combine.weld_type))))
        return output_elem, (
            "|sb,si,se|\n"
            "let groups = tovec(result(for(se, groupmerger, |b,i,e| merge(b,e))));\n"
            "let keyvals = result(for(groups, appender, |gb,gi,ge| merge(gb,{{ge.$0,\n{}\n}})));\n"
            "merge(sb, keyvals)\n"
        ).format(combine.generate())


# for(stream,
#     windower[unit,groupmerger[?],?,dict[?,?]]
#       |agg| for(result(agg),
#                 appender,
#                 |b,i,e| for(merge(b,))
#     ),
#     |b,i,e| merge(b,e)
# )

class CombineGlobally(PTransform):
    def __init__(self, fn):
        self.fn = fn

    def get_sink(self):
        return "streamappender"

    def stage(self, input_elem, metadata):
        output_elem = self.fn(input_elem)
        return output_elem, "|sb,si,se| merge(sb,\n{}\n)".format(output_elem.generate())

    def without_defaults(self):
        pass
