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
         | beam.io.ReadFromText('input.txt', coder=CSVCoder()).with_output_types(int)
         | beam.WindowInto(window.FixedWindows(60))
         | beam.CombineGlobally(normalize)
         | beam.io.WriteToText('output.txt', coder=CSVCoder()))

        p.run()
