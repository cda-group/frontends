__all__ = [
    'Pipeline',
]


class Pipeline(object):
    transform_stack = []

    def __init__(self, runner=None, options=None, argv=None):
        """Initialize a pipeline object."""
        self.runner = runner
        self.options = options
        self.argv = argv

    def apply(self, transform, pvalueish=None, label=None):
        """Applies a custom transform using the pvalueish specified."""
        # TODO: Not sure what to do here
        self.transform_stack.append(transform)

    def run(self):
        """Runs the pipeline. Returns whatever our runner returns after running."""
        transform = self.transform_stack.pop()


class PipelineRunner:
    def __init__(self):
        self
