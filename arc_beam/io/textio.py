from arc_beam.transforms.ptransform import PTransform

__all__ = [
    'ReadFromText',
    'WriteToText'
]


class ReadFromText(PTransform):
    def __init__(self, path, df_schema):
        self.path = path
        self.df_schema = df_schema
        super(ReadFromText, self).__init__()

    def expand(self, inputs):
        pass


class WriteToText(PTransform):
    def __init__(self, path):
        self.path = path
        super(WriteToText, self).__init__()

    def expand(self, inputs):
        pass
