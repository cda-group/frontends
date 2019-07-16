from weld_pandas.weldobject import *

from weld_pandas.pandas import WeldBase


class Scalar(WeldBase):

    def __init__(self):
        self.weldobj = WeldObject(None, None)

    # Algebraic operators

    def __add__(self, other):  # self + other
        if isinstance(other, Scalar):
            self.set("{!s} + {!s}".format(self, other))
        else:
            raise TypeError

    def __mul__(self, other):  # self * other
        if isinstance(other, Scalar):
            self.set("{!s} * {!s}".format(self, other))
        else:
            raise TypeError

    def __sub__(self, other):  # self - other
        if isinstance(other, Scalar):
            self.set("{!s} - {!s}".format(self, other))
        else:
            raise TypeError

    def __div__(self, other):  # self / other
        if isinstance(other, Scalar):
            self.set("{!s} / {!s}".format(self, other))
        else:
            raise TypeError

    def __pow__(self, other, modulo=None):  # self ** other
        if isinstance(other, Scalar):
            self.set("pow({!s}, {!s})".format(self, other))
        else:
            raise TypeError

    def __eq__(self, other):  # self == other
        if isinstance(other, Scalar):
            self.set("{!s} == {!s}".format(self, other))
        else:
            raise TypeError

    def __ne__(self, other):  # self != other
        if isinstance(other, Scalar):
            self.set("{!s} != {!s}".format(self, other))
        else:
            raise TypeError

    def __lt__(self, other):  # self < other
        if isinstance(other, Scalar):
            self.set("{!s} < {!s}".format(self, other))
        else:
            raise TypeError

    def __le__(self, other):  # self <= other
        if isinstance(other, Scalar):
            self.set("{!s} <= {!s}".format(self, other))
        else:
            raise TypeError

    def __ge__(self, other):  # self >= other
        if isinstance(other, Scalar):
            self.set("{!s} >= {!s}".format(self, other))
        else:
            raise TypeError

    def __gt__(self, other):  # self > other
        if isinstance(other, Scalar):
            self.set("{!s} > {!s}".format(self, other))
        else:
            raise TypeError

