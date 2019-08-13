class ArcType(object):

    def __str__(self):
        return "?"

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return hash(other) == hash(self)

    def __ne__(self, other):
        return hash(other) != hash(self)

    @staticmethod
    def from_dtype(dtype):
        if dtype == 'int16':
            return ArcI16()
        elif dtype == 'int32':
            return ArcI32()
        elif dtype == 'int64':
            return ArcI64()
        elif dtype == 'float32':
            return ArcF32()
        elif dtype == 'float64':
            return ArcF64()
        else:
            raise ValueError("unsupported dtype {}".format(dtype))


class ArcI8(ArcType):
    def __str__(self):
        return "i8"


class ArcBool(ArcType):
    def __str__(self):
        return "bool"


class ArcI16(ArcType):
    def __str__(self):
        return 'i16'


class ArcI32(ArcType):
    def __str__(self):
        return "i32"


class ArcI64(ArcType):
    def __str__(self):
        return "i64"


class ArcF32(ArcType):
    def __str__(self):
        return "f32"


class ArcF64(ArcType):
    def __str__(self):
        return "f64"


class ArcVec(ArcType):
    def __init__(self, elem_type):
        self.elem_type = elem_type

    def __str__(self):
        return "vec[%s]" % str(self.elem_type)


class ArcStruct(ArcType):
    def __init__(self, field_types):
        assert False not in [isinstance(e, ArcType) for e in field_types]
        self.field_types = field_types

    def __str__(self):
        return "{" + ",".join([str(f) for f in self.field_types]) + "}"
