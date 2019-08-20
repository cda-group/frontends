# class CombineMeanFn(object):
#
from arc_beam import PTransform


class ToList(PTransform):
    def stage(self, input_elem, metadata):
        raise RuntimeError("Not supported")
        # return metadata, input_elem, "|sb,si,se| merge(sb, se)"
