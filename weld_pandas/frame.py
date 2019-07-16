from weld.weldobject import *
from weld_pandas.series import Series
from weld_pandas.utils import WeldBase


# Just a 2D Array for now
class DataFrame(WeldBase):
    def __init__(self):
        self.weldobj = WeldObject(None, None)

    def __str__(self):
        return self.weldobj.weld_code

    def dot(self, other):
        if isinstance(other, Series):
            self.set("map("
                     "  {!s},"
                     "  |row|"
                     "    result("
                     "      for("
                     "        zip(row,{!s}),"
                     "        merger[_,+],"
                     "        |b,i,e| merge(b, e.$0+e.$1)"
                     "      )"
                     "    )"
                     ")".format(self, other))
