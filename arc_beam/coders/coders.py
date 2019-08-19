import typing


class Coder(object):
    """Base class for coders."""

    def encode(self, value):
        """Encodes the given object into a byte string."""
        raise NotImplementedError('Encode not implemented: %s.' % self)

    def decode(self, encoded):
        """Decodes the given byte string into the corresponding object."""
        raise NotImplementedError('Decode not implemented: %s.' % self)

    def is_deterministic(self):
        """Whether this coder is guaranteed to encode values deterministically.
        A deterministic coder is required for key coders in GroupByKey operations
        to produce consistent results.
        For example, note that the default coder, the PickleCoder, is not
        deterministic: the ordering of picked entries in maps may vary across
        executions since there is no defined order, and such a coder is not in
        general suitable for usage as a key coder in GroupByKey operations, since
        each instance of the same key may be encoded differently.
        Returns:
          Whether coder is deterministic.
        """
        return False


class CSVCoder(Coder):
    """A coder used for reading and writing strings as CSV."""

    def format(self):
        return 'CSV'


class StrUtf8Coder(Coder):
    """A coder used for reading and writing strings as UTF-8."""

    def format(self):
        return 'UTF8'


class JSONCoder(Coder):
    """A coder used for reading and writing strings as UTF-8."""

    def format(self):
        return 'JSON'
