from typing import Tuple

import arc_beam as beam
import arc_beam.transforms.window as window
import arc_beam.transforms.combiners as combiners
import baloo as pandas


def normalize(keyvals):
    min_val = keyvals.min()
    max_val = keyvals.max()
    normalized_values = (values - min_val) / (max_val - min_val)
    return key, normalized_values


class TestSuite2(object):
    def test2(self):
        p = beam.Pipeline()
        n = 4  # 4x4=16 grids
        height, width = 100, 100  # height/width of touchpad
        grid_width, grid_height = int(width / n), int(height / n)  # height/width of grids

        (p
         | beam.io.ReadFromSocket('127.0.0.1:8000', beam.coders.CSVCoder()).with_output_types(
                    #      ts,     x,     y,     z
                    Tuple[int, float, float, float])

         | 'preprocess' >> beam.Filter(lambda e: (e[2] < 0.0) & (e[2] > 1.0))
         | 'extract timestamp' >> beam.Map(lambda e: window.TimestampedValue(e[1:4], e[0]))
         | 'extract key' >> beam.Map(lambda e: (((e[0] / grid_height).asInt(), (e[1] / grid_width).asInt()), e[2]))
         | 'create tumbling window' >> beam.WindowInto(window.FixedWindows(60))
         | 'group by grid cell' >> beam.GroupByKey()
         | 'sum up pressures' >> beam.Map(lambda e: (e[0], (pandas.Series(e[1]) + 0.1).sum()))
         | 'collect window as list' >> combiners.ToList()
         # | 'normalize grid' >> beam.Map(normalize)

         | beam.io.WriteToSocket('127.0.0.1:9000', beam.coders.CSVCoder()))

        p.run()
        # | 'sum up pressures' >> beam.CombinePerKey(lambda elem: (pandas.Series(elem[1]) + 0.1).sum())
