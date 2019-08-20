from typing import Tuple

import arc_beam as beam
import arc_beam.transforms.window as window
import arc_beam.transforms.combiners as combiners
import baloo as pandas


class TestSuite2(object):
    def test2(self):
        p = beam.Pipeline()
        n = 4  # 4x4=16 grids
        height, width = 100, 100  # height/width of touchpad
        grid_width, grid_height = int(width / n), int(height / n)  # height/width of grids

        # Elements are tuples of of (timestamp, x, y, pressure)
        (p
         | beam.io.ReadFromSocket('127.0.0.1:8000', beam.coders.CSVCoder()).with_output_types(
                    Tuple[int, float, float, float])

         | 'preprocess' >> beam.Filter(lambda elem: (elem[2] < 0.0) & (elem[2] > 1.0))
         | 'extract timestamp' >> beam.Map(lambda elem: window.TimestampedValue(elem[1:4], elem[0]))
         | 'extract key' >> beam.Map(lambda elem: (((elem[0] / grid_height).toInt(), (elem[1] / grid_width).toInt()), elem[2]))
         | 'create tumbling window' >> beam.WindowInto(window.FixedWindows(60))
         | 'sum up pressures' >> beam.CombinePerKey(lambda values: (pandas.Series(values) + 0.1).sum())
         # | 'collect window as list' >> combiners.ToList()

         | beam.io.WriteToSocket('127.0.0.1:9000', beam.coders.CSVCoder()))

        p.run()
