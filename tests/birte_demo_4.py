from typing import Tuple

import arc_beam as beam
import arc_beam.transforms.window as window
import arc_beam.transforms.combiners as combiners
import baloo as pandas


class TestSuite2(object):
    def test2(self):
        p = beam.Pipeline()
        height, width = 500, 300  # 500x300 points on touchpad
        num_x_grids, num_y_grids = 5, 3  # 5x3 grids
        grid_width, grid_height = int(width / num_x_grids), int(height / num_y_grids)  # height/width of grids

        (p
         | beam.io.ReadFromSocket('127.0.0.1:8000', beam.coders.CSVCoder()).with_output_types(
                    #      ts,     x,     y,     z
                    Tuple[int, float, float, float])

         | 'preprocess' >> beam.Filter(lambda e: (e[3] > 0.0) & (e[3] < 1.0))
         | 'extract timestamp' >> beam.Map(lambda e: window.TimestampedValue(e[1:4], e[0]))
         | 'extract key' >> beam.Map(
                    lambda e: (((e[0] / grid_height).asInt() - 1, (e[1] / grid_width).asInt() - 1), e[2]))
         | 'create tumbling window' >> beam.WindowInto(window.FixedWindows(60))
         | 'group by grid cell' >> beam.GroupByKey()
         | 'sum up pressures' >> beam.Map(lambda e: (e[0], (pandas.Series(e[1]) + 0.1).sum()))
         | 'collect window as list' >> combiners.ToList()
         # | 'normalize grid' >> beam.Map(normalize)

         | beam.io.WriteToSocket('127.0.0.1:9000', beam.coders.CSVCoder()))

        p.run()
        # | 'sum up pressures' >> beam.CombinePerKey(lambda elem: (pandas.Series(elem[1]) + 0.1).sum())
