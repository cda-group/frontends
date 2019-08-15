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

        # TODO: Probably remove this example
        # Elements are tuples of of (x-coordinate, y-coordinate, pressure)
        (p
         | beam.io.ReadFromText('input.txt').with_output_types(Tuple[float, float, float])
         | beam.Map(lambda data: ((data[0] / grid_height, data[1] / grid_width), data[2]))
         | beam.GroupByKey()
         | beam.WindowInto(window.FixedWindows(60))
         | beam.CombineFn(lambda data: pandas.Series(data).sum())
         | beam.io.WriteToText('output.txt'))

        p.run()
