from baloo.weld import WeldObject, LazyResult, LazyArrayResult, numpy_to_weld_type, LazyScalarResult, LazyStructResult, \
    LazyStructOfVecResult, WeldVec, WeldInt, WeldFloat, WeldStruct, WeldBit, WeldChar, WeldLong, WeldDouble, lazify, \
    python_type_to_weld_type
from baloo.weld.convertors.encoders import NumPyEncoder, NumPyDecoder
import json

__all__ = [
    'Pipeline',
]

encoder = NumPyEncoder()
decoder = NumPyDecoder()


class Pipeline(object):
    transforms = []

    def __init__(self, runner=None, options=None, argv=None):
        """Initialize a pipeline object."""
        self.runner = runner
        self.options = options
        self.argv = argv

    def __or__(self, right):  # pipeline | self
        self.transforms.append(right)
        return self

    def __str__(self):
        return "\nPipeline: " + (" | ".join(map(lambda x: x.__str__(), self.transforms)))

    def run(self):
        metadata = json.dumps(self.generate())
        print(metadata)

    def generate(self):
        from arc_beam.transforms.combiners import ToList
        source, sink = self.transforms.pop(0), self.transforms.pop()
        metadata = {
            'nodes': [
                source.encode("source_0"),
                sink.encode("sink_0")
            ],
        }
        self.transforms = list(filter(lambda x: not isinstance(x, ToList), self.transforms))

        weld_type = python_type_to_weld_type(source.output_type)
        elem = lazify(weld_type)

        weld_code = "|source_0: stream[{}], sink_0: streamappender[?]|\n".format(
            weld_type,
            len(self.transforms) - 1
        )

        operator_id = 0
        keyby = False
        for (index, transform) in enumerate(self.transforms):
            from arc_beam import GroupByKey, WindowInto
            if isinstance(transform, GroupByKey) and isinstance(self.transforms[index + 1], WindowInto):
                keyby = True
            else:
                elem.weld_expr = WeldObject(None, None)
                elem.weld_expr.weld_code = 'se'
                elem, body = transform.stage(elem, metadata)
                if transform.label is not None:
                    weld_code += "# {}\n".format(transform.label)
                if index == 0:
                    weld_code += "let operator_{} = result(for(source_0, {}, {}));\n".format(
                        operator_id + 1,
                        transform.get_sink(),
                        body
                    )
                elif index < len(self.transforms) - 1:
                    weld_code += "let operator_{} = result(for({}, {}, {}));\n".format(
                        operator_id + 1,
                        ("keyby(operator_{}, |e| [0])" if keyby else "operator_{}").format(operator_id),
                        transform.get_sink(),
                        body
                    )
                    keyby = False
                else:
                    weld_code += "for(operator_{}, sink_0, {})\n".format(
                        operator_id,
                        body
                    )
                operator_id += 1
        print(weld_code)
        metadata['code'] = weld_code
        return metadata
