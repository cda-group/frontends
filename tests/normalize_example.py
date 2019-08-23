import arc_beam as beam
import arc_beam.transforms.window as window
import baloo as pandas
from arc_beam.coders.coders import CSVCoder


def normalize(data):
    series = pandas.Series(data)
    avg = series.sum() / series.count()
    return series / avg


class TestSuite(object):
    def test1(self):
        p = beam.Pipeline()

        (p
         | beam.io.ReadFromText(path='input.txt', coder=CSVCoder()).with_output_types(int)
         | beam.WindowInto(window.FixedWindows(size=5))
         | beam.CombineGlobally(normalize)
         | beam.io.WriteToText(path='output.txt', coder=CSVCoder()))

        p.run()
