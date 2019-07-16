from weld.weldobject import *
from weld_pandas.utils import WeldBase


# 1D Array
class Series(WeldBase):
    def __init__(self):
        self.weldobj = WeldObject(None, None)

    def __str__(self):
        return self.weldobj.weld_code

    def dot(self, other):
        if isinstance(other, WeldVector):
            self.set("result(for(zip({!s},{!s}), merger[_,+], |b,i,e| merge(b, e.$0+e.$1)))".format(self, other))

    # Algebraic operators

    def __add__(self, other):  # self + other
        if isinstance(other, WeldScalar):
            self.set("map({!s}, |e| e + {!s})".format(self, other))
        elif isinstance(other, WeldVector):
            self.set("result(for(zip({!s},{!s}), appender, |b,i,e| merge(b, e.$0 + e.$1)))".format(self, other))

    def __mul__(self, other):  # self * other
        if isinstance(other, WeldScalar):
            self.set("map({!s}, |e| e * {!s})".format(self, other))
        elif isinstance(other, WeldVector):
            self.set("result(for(zip({!s},{!s}), appender, |b,i,e| merge(b, e.$0 * e.$1)))".format(self, other))

    def __sub__(self, other):  # self - other
        if isinstance(other, WeldScalar):
            self.set("map({!s}, |e| e - {!s})".format(self, other))
        elif isinstance(other, WeldVector):
            self.set("result(for(zip({!s},{!s}), appender, |b,i,e| merge(b, e.$0 - e.$1)))".format(self, other))

    def __div__(self, other):  # self / other
        if isinstance(other, WeldScalar):
            self.set("map({!s}, |e| e / {!s})".format(self, other))
        elif isinstance(other, WeldVector):
            self.set("result(for(zip({!s},{!s}), appender, |b,i,e| merge(b, e.$0 / e.$1)))".format(self, other))

    def __pow__(self, other, modulo=None):  # self ** other
        if isinstance(other, WeldScalar):
            raise TypeError
        elif isinstance(other, WeldVector):
            self.set("cudf[matmul,vec[_]]({!s},{!s})".format(self, other))

    def __eq__(self, other):  # self == other
        if isinstance(other, WeldScalar):
            raise TypeError
        elif isinstance(other, WeldVector):
            self.set("{!s} == {!s}".format(self, other))

    def __ne__(self, other):  # self != other
        if isinstance(other, WeldScalar):
            raise TypeError
        elif isinstance(other, WeldVector):
            self.set("{!s} != {!s}".format(self, other))

    def __lt__(self, other):  # self < other
        if isinstance(other, WeldScalar):
            raise TypeError
        elif isinstance(other, WeldVector):
            self.set("{!s} < {!s}".format(self, other))

    def __le__(self, other):  # self <= other
        if isinstance(other, WeldScalar):
            raise TypeError
        elif isinstance(other, WeldVector):
            self.set("{!s} <= {!s}".format(self, other))

    def __ge__(self, other):  # self >= other
        if isinstance(other, WeldScalar):
            raise TypeError
        elif isinstance(other, WeldVector):
            self.set("{!s} >= {!s}".format(self, other))

    def __gt__(self, other):  # self > other
        if isinstance(other, WeldScalar):
            raise TypeError
        elif isinstance(other, WeldVector):
            self.set("{!s} > {!s}".format(self, other))

    # Other

    def __cmp__(self, other):  # cmp(self, other)
        return "compare({!s}, {!s})"

    def __contains__(self, item):  # item in self
        return "keyexists({!s}, {!s})".format(self, item)

    def __len__(self):  # len(self)
        return "len({!s})".format(self)

    def __iter__(self):  # iter(self)
        raise NotImplementedError

    def __reversed__(self):  # reversed(self)
        raise NotImplementedError

    def __getitem__(self, item):  # self[item]
        return "lookup({!s}, {!s})".format(self, item)

    def __setitem__(self, key, value):  # self[key] = value
        raise NotImplementedError

    def __delitem__(self, key):  # del self[key]
        raise NotImplementedError

    def __setslice__(self, i, j, sequence):  # self[i:j] = sequence
        raise NotImplementedError

    def __delslice__(self, i, j):  # del self[i:j]
        raise NotImplementedError

    def __hash__(self):  # hash(self)
        raise NotImplementedError

    def __get__(self, instance, owner):  # instance.self
        raise NotImplementedError

    def __set__(self, instance, value):  # instance.self = value
        raise NotImplementedError

    def __delete__(self, instance):  # del instance.self
        raise NotImplementedError


