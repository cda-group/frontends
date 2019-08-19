from typing import Tuple

import arc_beam as beam
import arc_beam.transforms.window as window
import baloo as pandas


class TestSuite2(object):
    def test2(self):
        p = beam.Pipeline()
        n = 4  # 4x4=16 grids
        height, width = 100, 100  # height/width of touchpad
        grid_width, grid_height = int(width / n), int(height / n)  # height/width of grids

        # Elements are tuples of of (timestamp, x-coordinate, y-coordinate, pressure)
        (p
         | beam.io.ReadFromSocket('localhost', 8000, beam.coders.CSVCoder())
                  .with_output_types(Tuple[int, float, float, float])

         | beam.Map(lambda data: window.TimestampedValue(data[0], data[1:4]))
         | beam.Filter(lambda data: (data[2] < 0.0) & (data[2] > 1.0))
         | beam.Map(lambda data: ((data[0] / grid_height, data[1] / grid_width), data[2]))
         | beam.WindowInto(window.FixedWindows(60))
         | beam.GroupByKey()
         | beam.Map(lambda data: (data[0], pandas.Series(data[1]).sum()))

         | beam.io.WriteToSocket('localhost', 9000, beam.coders.CSVCoder()))

        p.run()
