# class CombineMeanFn(object):
#
from arc_beam import PTransform


class ToList(PTransform):
    def stage(self, input_elem, metadata):
        return "|sb,si,se| merge(sb, se)"
