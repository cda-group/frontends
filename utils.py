from weld.types import *


def python_to_weld(python_type):
    if isinstance(python_type, tuple):
        WeldStruct(map(python_to_weld, python_type))
    elif isinstance(python_type, int):
        WeldInt()
    elif isinstance(python_type, long):
        WeldLong()
    elif isinstance(python_type, float):
        WeldFloat()
    else:
        raise TypeError
