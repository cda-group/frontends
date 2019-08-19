__all__ = [
    'PTransform',
]


class PTransform(object):
    input_type = "?"
    output_type = "?"
    label = None

    def __rrshift__(self, label):  # name >> self
        self.label = label
        return self

    def __str__(self):
        return type(self).__name__

    def stage(self, input_elem, metadata):
        raise NotImplementedError

    def with_input_types(self, input_type):
        self.input_type = input_type
        return self

    def with_output_types(self, output_type):
        self.output_type = output_type
        return self

    def get_sink(self):
        raise NotImplementedError

    @staticmethod
    def get_iter(source):
        return source
