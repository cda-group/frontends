from typing import Tuple

import arc_beam as beam
import arc_beam.transforms.window as window
import arc_beam.transforms.combiners as combiners
import baloo as pandas


class TestSuite2(object):
    def test2(self):
        p = beam.Pipeline()

        grid_width, grid_height = ...
        epsilon = ...
        touchpad, display = ...
        ts, x, y, z = int, float, float, float
        max_pressure = ...
        window_length = ...

        (p
         | beam.io.ReadFromSocket(addr=touchpad, coder=beam.coders.CSVCoder())
                  .with_output_types(Tuple[ts, x, y, z])

         | 'preprocess'
            >> beam.Filter(lambda e: (e[1] >= 0) & (e[1] <= width))
         |     beam.Filter(lambda e: (e[2] >= 0) & (e[2] <= height))
         |     beam.Filter(lambda e: (e[3] >= 0) & (e[3] <= max_pressure))

         | 'extract timestamp'
            >> beam.Map(lambda e: window.TimestampedValue(value=e[1:4], timestamp=e[0]))

         | 'extract key'
            >> beam.Map(lambda e: ((e[0] / grid_width, e[1] / grid_height), e[2]))

         | 'add to pressure'
            >> beam.Map(lambda e: (e[0], e[1] + epsilon))

         | 'create tumbling window'
            >> beam.WindowInto(window.FixedWindows(size=window_length))

         | 'sum up pressures'
            >> beam.CombinePerKey(lambda e: pandas.Series(e).sum())

         | 'collect window as list'
            >> combiners.ToList()

         | beam.io.WriteToSocket(addr=display, coder=beam.coders.CSVCoder()))

        p.run()
