from arc_beam.transforms.ptransform import PTransform

__all__ = [
    # 'DoFn',
    # 'CombineFn',
    # 'PartitionFn',
    'ParDo',
    'FlatMap',
    'Map',
    'Filter',
    # 'CombineGlobally',
    # 'CombinePerKey',
    # 'CombineValues',
    # 'GroupByKey',
    # 'Partition',
    # 'Windowing',
    'WindowInto',
    # 'Flatten',
    # 'Create',
    # 'Impulse',
    # 'RestrictionProvider'
]


class ParDo(PTransform):
    """A :class:`ParDo` transform.
      Processes an input :class:`~apache_beam.pvalue.PCollection` by applying a
      :class:`DoFn` to each element and returning the accumulated results into an
      output :class:`~apache_beam.pvalue.PCollection`.
      """

    def __init__(self, fn, *args, **kwargs):
        super(ParDo, self).__init__()

    def expand(self, input):
        pass


class DoFn(object):
    """A function object used by a transform with custom processing.
    The ParDo transform is such a transform. The ParDo.apply
    method will take an object of type DoFn and apply it to all elements of a
    PCollection object.
    In order to have concrete DoFn objects one has to subclass from DoFn and
    define the desired behavior (start_bundle/finish_bundle and process) or wrap a
    callable object using the CallableWrapperDoFn class.
    """

    def __init__(self):
        pass

    def process(self, element, *args, **kwargs):
        raise NotImplementedError

    def setup(self):
        """Called to prepare an instance for processing bundles of elements."""
        pass

    def start_bundle(self):
        """Called before a bundle of elements is processed on a worker."""
        pass

    def finish_bundle(self):
        """Called after a bundle of elements is processed on a worker."""
        pass

    def teardown(self):
        """Called to use to clean up this instance before it is discarded."""
        pass


class CallableWrapperDoFn(DoFn):
    """A DoFn (function) object wrapping a callable object.
    The purpose of this class is to conveniently wrap simple functions and use
    them in transforms.
    """

    def __init__(self, fn):
        self.fn = fn
        super(CallableWrapperDoFn, self).__init__()

    def process(self, element, *args, **kwargs):
        self.fn(element, *args, **kwargs)

    def __repr__(self):
        return 'CallableWrapperDoFn(%s)' % self.fn


def Map(fn, *args, **kwargs):
    pardo = ParDo(CallableWrapperDoFn(fn), args, kwargs)
    pardo.label = "Map"
    return pardo


def Filter(fn, *args, **kwargs):
    pardo = ParDo(CallableWrapperDoFn(fn), args, kwargs)
    pardo.label = "Filter"
    return pardo


def FlatMap(fn, *args, **kwargs):
    pardo = ParDo(CallableWrapperDoFn(fn), args, kwargs)
    pardo.label = "FlatMap"


def Flatten(fn, *args, **kwargs):
    pardo = ParDo(CallableWrapperDoFn(fn), args, kwargs)
    pardo.label = "Flatten"


class WindowInto(ParDo):
    """A window transform assigning windows to each element of a PCollection.
    Transforms an input PCollection by applying a windowing function to each
    element.  Each transformed element in the result will be a WindowedValue
    element with the same input value and timestamp, with its new set of windows
    determined by the windowing function.
    """

    class WindowIntoFn(DoFn):
        """A DoFn that applies a WindowInto operation."""

        def process(self, element, *args, **kwargs):
            pass

    def __init__(self, fn, *args, **kwargs):
        super(WindowInto, self).__init__(fn, *args, **kwargs)
