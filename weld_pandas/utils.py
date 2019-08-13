__all__ = [
    'WeldBase'
]


class WeldBase(object):
    weldobj = None

    def __str__(self):
        return self.weldobj.weld_code

    def updated(self, code):
        self.weldobj.weld_code = code
        return self


