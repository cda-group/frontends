__all__ = [
    'TimestampCombiner',
    'WindowFn',
    'BoundedWindow',
    'IntervalWindow',
    'TimestampedValue',
    'GlobalWindow',
    'NonMergingWindowFn',
    'GlobalWindows',
    'FixedWindows',
    'SlidingWindows',
    'Sessions',
]


class TimestampCombiner(object):
    """Determines how output timestamps of grouping operations are assigned."""


class WindowFn(object):
    """An abstract windowing function defining a basic assign and merge."""

    class AssignContext(object):
        """Context passed to WindowFn.assign()."""

        def __init__(self, timestamp, element=None, window=None):
            # self.timestamp = Timestamp.of(timestamp)
            self.element = element
            self.window = window

    def assign(self, assign_context):
        """Associates windows to an element.
        Arguments:
          assign_context: Instance of AssignContext.
        Returns:
          An iterable of BoundedWindow.
        """
        raise NotImplementedError

    class MergeContext(object):
        """Context passed to WindowFn.merge() to perform merging, if any."""

        def __init__(self, windows):
            self.windows = list(windows)

        def merge(self, to_be_merged, merge_result):
            raise NotImplementedError

    def merge(self, merge_context):
        """Returns a window that is the result of merging a set of windows."""
        raise NotImplementedError

    def is_merging(self):
        """Returns whether this WindowFn merges windows."""
        return True


class BoundedWindow(object):
    """A window for timestamps in range (-infinity, end).
    Attributes:
      end: End of window.
    """

    def __init__(self, end):
        pass
    # self.end = Timestamp.of(end)


class IntervalWindow(BoundedWindow):
    """A window for timestamps in range [start, end).
    Attributes:
      start: Start of window as seconds since Unix epoch.
      end: End of window as seconds since Unix epoch.
    """


class TimestampedValue(object):
    """A timestamped value having a value and a timestamp.
    Attributes:
      value: The underlying value.
      timestamp: Timestamp associated with the value as seconds since Unix epoch.
    """

    def __init__(self, value, timestamp):
        from baloo.weld import LazyScalarResult
        if isinstance(timestamp, LazyScalarResult):
            self.ts_extractor = int(timestamp.weld_expr.weld_code.split('$')[1])
        else:
            raise TypeError("Invalid expression, could not extract timestamp")
        self.value = value


class GlobalWindow(BoundedWindow):
    """The default window into which all data is placed (via GlobalWindows)."""


class NonMergingWindowFn(WindowFn):

    def assign(self, assign_context):
        pass

    def is_merging(self):
        return False

    def merge(self, merge_context):
        pass  # No merging.


class GlobalWindows(NonMergingWindowFn):
    """A windowing function that assigns everything to one global window."""

    def assign(self, assign_context):
        pass


# Tumbling
class FixedWindows(object):
    """A windowing function that assigns each element to one time interval.
    The attributes size and offset determine in what time interval a timestamp
    will be slotted. The time intervals have the following formula:
    [N * size + offset, (N + 1) * size + offset)
    Attributes:
      size: Size of the window as seconds.
      offset: Offset of this window as seconds. Windows start at
        t=N * size + offset where t=0 is the UNIX epoch. The offset must be a
        value in range [0, size). If it is not it will be normalized to this
        range.
    """

    def get_sink_type(self):
        ty = 'windower[unit,appender[?],?,vec[?]]'
        assign = '|ts,windows,state| {{ [ts/{}L], () }}'.format(self.size)
        trigger = '|wm,windows,state| { result(for(windows, appender, |b,i,e| if(i < wm, merge(b, i), b))), () }'
        lower = '|agg| result(agg)'
        return "{}(\n  {},\n  \t{},\n  \t{}\n)".format(ty, assign, trigger, lower)

    def generate(self, input_elem):
        from baloo.weld import LazyArrayResult
        output_elem = LazyArrayResult(input_elem.weld_expr, input_elem.weld_type)
        return output_elem, '|sb,si,se| merge(sb, se)'

    def __init__(self, size, offset=0):
        self.size = size * 1000
        self.offset = offset * 1000


# Sliding
class SlidingWindows(NonMergingWindowFn):
    """A windowing function that assigns each element to a set of sliding windows.
    The attributes size and offset determine in what time interval a timestamp
    will be slotted. The time intervals have the following formula:
    [N * period + offset, N * period + offset + size)
    Attributes:
      size: Size of the window as seconds.
      period: Period of the windows as seconds.
      offset: Offset of this window as seconds since Unix epoch. Windows start at
        t=N * period + offset where t=0 is the epoch. The offset must be a value
        in range [0, period). If it is not it will be normalized to this range.
    """

    def assign(self, assign_context):
        pass

    def __init__(self, size, period, offset=0):
        pass


# Session
class Sessions(WindowFn):
    """A windowing function that groups elements into sessions.
    A session is defined as a series of consecutive events
    separated by a specified gap size.
    Attributes:
      gap_size: Size of the gap between windows as floating-point seconds.
    """

    def merge(self, merge_context):
        pass

    def assign(self, assign_context):
        pass
