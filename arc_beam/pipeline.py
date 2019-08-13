import numpy
import numpy as np
import typing

from baloo.weld import WeldObject, LazyResult, LazyArrayResult, numpy_to_weld_type, LazyScalarResult, LazyStructResult, \
    LazyStructOfVecResult, WeldVec, WeldInt, WeldFloat, WeldStruct, WeldBit, WeldChar, WeldLong, WeldDouble
from baloo.weld.convertors.encoders import NumPyEncoder, NumPyDecoder

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

    # |source_0: stream[i32], sink_2: streamappender[_]|
    #   let sink_0: streamappender[_] = streamappender[_];
    #   let sink_1: streamappender[_] = streamappender[_];
    #   let source_1: stream[_] = result(for(source_0, sink_0, |b,i,e| merge(b, e+5)));
    #   let source_2: stream[_] = result(for(source_1, sink_1, |b,i,e| if(e > 0, merge(b, e), b))));
    #   let source_3: stream[_] = result(for(source_2, sink_2, |b,i,e| merge(b, e+5)));
    #   source_3

    def run(self):
        weld_code = self.generate()
        print(weld_code)

    def generate(self):
        source = self.transforms.pop(0)
        sink = self.transforms.pop()
        num_operators = len(self.transforms)
        weld_type = python_to_weld_type(source.output_type)
        elem = lazify(weld_type)

        weld_code = "\n# ---- External sinks/sources ----\n"
        weld_code += "|source_0: stream[{}], sink_{}: streamappender[?]|\n".format(
            weld_type,
            num_operators - 1
        )

        weld_code += "\n# ---- Internal sinks ----\n"
        for (index, transform) in enumerate(self.transforms[:-1]):
            weld_code += "let sink_{} = {};\n".format(index, transform.get_sink_type())

        weld_code += "\n# ---- Internal sources ----\n"
        for (index, transform) in enumerate(self.transforms):
            elem.weld_expr = WeldObject(None, None)
            elem.weld_expr.weld_code = 'e'
            elem, body = transform.stage(elem)
            if index < len(self.transforms) - 1:
                weld_code += "let source_{} = result(for(source_{}, sink_{}, {}));\n".format(
                    index + 1,
                    index,
                    index,
                    body
                )
            else:
                weld_code += "for(source_{}, sink_{}, {})\n".format(
                    index,
                    index,
                    body
                )
        return weld_code


# Creates a lazy value out of a Weld type
def lazify(weld_type):
    weld_expr = WeldObject(None, None)
    if isinstance(weld_type, (WeldLong, WeldDouble, WeldBit)):
        return LazyScalarResult(weld_expr, weld_type)
    elif isinstance(weld_type, WeldVec):
        return LazyArrayResult(weld_expr, weld_type.elemType)
    elif isinstance(weld_type, WeldStruct):
        if all(map(lambda ty: isinstance(ty, WeldVec), weld_type.field_types)):  # Tuple of arrays
            weld_types = [ty.elemType for ty in weld_type.field_types]
            return LazyStructOfVecResult(weld_expr, weld_types)
        elif all(map(lambda ty: isinstance(ty, (WeldVec, WeldInt, WeldFloat, WeldBit)),
                     weld_type.field_types)):  # Mixed tuple
            weld_types = [ty.elemType for ty in weld_type.field_types]
            return LazyStructResult(weld_expr, weld_types)
        else:
            raise TypeError('Unsupported type in struct {}', weld_type)
    else:
        raise TypeError('Unsupported type {}', weld_type)


# Translates a Python type to a Weld type, e.g. Tuple[int] to WeldStruct(WeldInt())
def python_to_weld_type(python_type):
    if python_type is int:
        return WeldLong()
    elif python_type is float:
        return WeldDouble()
    elif python_type is bool:
        return WeldBit()
    elif python_type is str:
        return WeldVec(WeldChar)
    elif hasattr(python_type, '__origin__'):
        origin = python_type.__origin__
        if origin is list:
            elem_type = python_to_weld_type(python_type.__args__[0])
            return WeldVec(elem_type)
        elif origin is tuple:
            field_types = [python_to_weld_type(type(arg)) for arg in python_type.__args__]
            return WeldStruct(field_types)
    else:
        raise TypeError('Unsupported type {}'.format(python_type))


def weld_to_numpy_type(weld_type):
    if isinstance(weld_type, WeldLong):
        return numpy.dtype(numpy.long)
    elif isinstance(weld_type, WeldDouble):
        return numpy.dtype(numpy.double)
    elif isinstance(weld_type, WeldBit):
        return numpy.dtype(numpy.bool)
    else:
        raise TypeError('Cannot convert {} to numpy'.format(weld_type))
