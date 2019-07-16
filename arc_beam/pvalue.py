__all__ = [
    'PCollection',
    # 'TaggedOutput',
    # 'AsSingleton',
    # 'AsIter',
    # 'AsList',
    # 'AsDict',
    # 'EmptySideInput',
    'PBegin',
]


class PValue(object):
    """Base class for PCollection.
    Dataflow users should not construct PValue objects directly in their
    pipelines.
    A PValue has the following main characteristics:
      (1) Belongs to a pipeline. Added during object initialization.
      (2) Has a transform that can compute the value if executed.
      (3) Has a value which is meaningful if the transform was executed.
    """

    def __init__(self, pipeline, tag=None, element_type=None, windowing=None):
        self.pipeline = pipeline
        self.tag = tag
        self.element_type = element_type
        self.producer = None
        if windowing:
            self._windowing = windowing

    def __str__(self):
        return self._str_internal()

    def __repr__(self):
        return '<%s at %s>' % (self._str_internal(), hex(id(self)))

    def _str_internal(self):
        return "%s[%s.%s]" % (self.__class__.__name__,
                              self.producer.full_label if self.producer else None,
                              self.tag)

    def apply(self, *args, **kwargs):
        """Applies a transform or callable to a PValue.
        Args:
          *args: positional arguments.
          **kwargs: keyword arguments.
        The method will insert the pvalue as the next argument following an
        optional first label and a transform/callable object. It will call the
        pipeline.apply() method with this modified argument list.
        """
        arglist = list(args)
        arglist.insert(1, self)
        return self.pipeline.apply(*arglist, **kwargs)

    def __or__(self, ptransform):
        return self.pipeline.apply(ptransform, self)


class PCollection(PValue):
    """A multiple values (potentially huge) container.
    Dataflow users should not construct PCollection objects directly in their
    pipelines.
    """

    def __eq__(self, other):
        if isinstance(other, PCollection):
            return self.tag == other.tag and self.producer == other.producer

    def __ne__(self, other):
        # TODO(BEAM-5949): Needed for Python 2 compatibility.
        return not self == other

    def __hash__(self):
        return hash((self.tag, self.producer))

    # def windowing(self):
    #     if not hasattr(self, '_windowing'):
    #         self._windowing = self.producer.transform.get_windowing(
    #             self.producer.inputs)
    #     return self._windowing
    #
    # def to_runner_api(self, context):
    #     return beam_runner_api_pb2.PCollection(
    #         unique_name=self._unique_name(),
    #         coder_id=context.coder_id_from_element_type(self.element_type),
    #         is_bounded=beam_runner_api_pb2.IsBounded.BOUNDED,
    #         windowing_strategy_id=context.windowing_strategies.get_id(
    #             self.windowing))
    #
    # def _unique_name(self):
    #     if self.producer:
    #         return '%d%s.%s' % (
    #             len(self.producer.full_label), self.producer.full_label, self.tag)
    #     else:
    #         return 'PCollection%s' % id(self)
    #
    # @staticmethod
    # def from_runner_api(proto, context):
    # Producer and tag will be filled in later, the key point is that the
    # same object is returned for the same pcollection id.
    # return PCollection(
    #     None,
    #     element_type=context.element_type_from_coder_id(proto.coder_id),
    #     windowing=context.windowing_strategies.get_by_id(
    #         proto.windowing_strategy_id))


class PBegin(PValue):
    """A pipeline begin marker used as input to create/read transforms.
    The class is used internally to represent inputs to Create and Read
    transforms. This allows us to have transforms that uniformly take PValue(s)
    as inputs.
    """
    pass


class PDone(PValue):
    """PDone is the output of a transform that has a trivial result such as Write.
    """
    pass
