import arc_beam as beam
import arc_beam.transforms.window as window
# import weld_pandas as pd

import grizzly.grizzly as pd


def matmul(numbers):
    df = pd.DataFrame(numbers, numbers)
    s = pd.Series(numbers)
    return df.dot(s)


class TestSuite(object):
    def test1(self):
        p = beam.Pipeline()

        (p | (beam.io.ReadFromText(80, pd.DataFrameWeld({})) | beam.Filter(lambda x: x > 0)))

        # | beam.WindowInto(window.FixedWindows(60))
        # | beam.Map(matmul)
        # | beam.io.WriteToText(80))

        p.run()
