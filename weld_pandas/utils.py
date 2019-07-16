__all__ = [
    'WeldBase'
]


class WeldBase(object):
    weldobj = None

    def __str__(self):
        return self.weldobj.weld_code

    def set(self, code):
        self.weldobj.weld_code = code


