import arc_beam as beam
import arc_beam.transforms.window as window
import baloo as pandas


def normalize(data):
    df = pandas.DataFrame({
        'col1': pandas.Series(data)
    })
    avg = df['col1'].sum() / df['col1'].count()
    df = df[(df['col1'] > avg) & (df['col1'] < avg + 100)]
    return df['col1'] - (df['col1'].count() / df['col1'].sum() + 5)


class TestSuite(object):
    def test1(self):
        p = beam.Pipeline()

        (p
         | beam.io.ReadFromText('input.txt').with_output_types(int)
         | beam.Filter(lambda x: x < 3)
         | beam.WindowInto(window.FixedWindows(60))
         | beam.Map(normalize)
         | beam.Map(lambda x: x + 3)
         | beam.Map(lambda x: x.sum())
         | beam.io.WriteToText('output.txt'))

        p.run()
