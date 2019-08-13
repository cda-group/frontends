from weld.encoders import NumpyArrayEncoder, NumpyArrayDecoder
from weld.weldobject import WeldObject

from weld_pandas import WeldBase

from weld.types import WeldInt


class Scalar(WeldBase):

    def __init__(self, value, weldty):
        self.value = value
        self.weldty = weldty
        self.weldobj = WeldObject(NumpyArrayEncoder(), NumpyArrayDecoder())
        name = self.weldobj.update(value, WeldInt)

    # Algebraic operators

    def __add__(self, other):  # self + other
        return self.updated("{!s} + {!s}".format(self, other))

    def __mul__(self, other):  # self * other
        return self.updated("{!s} * {!s}".format(self, other))

    def __sub__(self, other):  # self - other
        return self.updated("{!s} - {!s}".format(self, other))

    def __div__(self, other):  # self / other
        return self.updated("{!s} / {!s}".format(self, other))

    def __pow__(self, other, modulo=None):  # self ** other
        return self.updated("pow({!s}, {!s})".format(self, other))

    def __eq__(self, other):  # self == other
        return self.updated("{!s} == {!s}".format(self, other))

    def __ne__(self, other):  # self != other
        return self.updated("{!s} != {!s}".format(self, other))

    def __lt__(self, other):  # self < other
        return self.updated("{!s} < {!s}".format(self, other))

    def __le__(self, other):  # self <= other
        return self.updated("{!s} <= {!s}".format(self, other))

    def __ge__(self, other):  # self >= other
        return self.updated("{!s} >= {!s}".format(self, other))

    def __gt__(self, other):  # self > other
        return self.updated("{!s} > {!s}".format(self, other))
