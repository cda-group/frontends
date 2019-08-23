from typing import Tuple

import arc_beam as beam
import arc_beam.transforms.window as window
import arc_beam.transforms.combiners as combiners
import baloo as pandas


class TestSuite2(object):
    def test2(self):
        p = beam.Pipeline()
        width, height = 500, 300  # 500x300 points on touchpad
        num_x_grids, num_y_grids = 5, 3  # 5x3 grids
        grid_width, grid_height = int(width / num_x_grids), int(height / num_y_grids)
        epsilon = 0.1
        ts, x, y, z = int, float, float, float

        (p
         | beam.io.ReadFromSocket('127.0.0.1:8000', beam.coders.CSVCoder())
                  .with_output_types(Tuple[ts, x, y, z])

         | 'preprocess'
            >> beam.Filter(lambda e: (e[3] >= 0.0) & (e[3] < 1.0))

         | 'extract timestamp'
            >> beam.Map(lambda e: window.TimestampedValue(e[1:4], e[0]))

         | 'extract key'
            >> beam.Map(lambda e: (((e[0] / grid_width).asInt(), (e[1] / grid_height).asInt()), e[2]))

         | 'add to pressure'
            >> beam.Map(lambda e: (e[0], e[1] + epsilon))

         | 'create tumbling window'
            >> beam.WindowInto(window.FixedWindows(size=6))

         | 'sum up pressures'
            >> beam.CombinePerKey(lambda e: pandas.Series(e).sum())

         | 'collect window as list'
            >> combiners.ToList()

         | beam.io.WriteToSocket('127.0.0.1:9000', beam.coders.CSVCoder()))

        p.run()
