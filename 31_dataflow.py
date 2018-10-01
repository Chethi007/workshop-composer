from __future__ import absolute_import

import argparse
import json

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class JsonCoder(object):
    """A JSON coder interpreting each line as a JSON string."""

    def encode(self, x):
        return json.dumps(x)

    def decode(self, x):
        return json.loads(x)


def compute(record):
    yield '{} {} {} {} {}'.format(record['order']['code'],
                                  len(record['order']['myRelatedOrders']),
                                  len(record['order']['statuses']),
                                  len(record['order']['partners']),
                                  len(record['order']['comments']))


def run(argv=None):
    """Main entry point; defines and runs the pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=pipeline_options) as p:
        (p  # pylint: disable=expression-not-assigned
         | 'read' >> ReadFromText(known_args.input, coder=JsonCoder())
         | 'compute' >> beam.FlatMap(compute)
         | 'write' >> WriteToText(known_args.output, shard_name_template=''))


if __name__ == '__main__':
    run()
