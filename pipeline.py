"""A transaction extraction workflow."""

from extraction_composite_transformation import ExtractionCompositeTransformation
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.filesystem import CompressionTypes

import apache_beam as beam
import argparse
import logging

blue = "\x1b[36;21m"
reset = "\x1b[0m"
format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"

formatter = logging.Formatter(blue + format + reset)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)



def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the transaction extraction pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        dest="input",
        default="gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv",
        help="Input transaction CSV file to be processed.",
    )
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file containing extracted transactions.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=pipeline_options) as p:

        # Read the text file[pattern] into a PCollection.
        lines = p | 'Read' >> ReadFromText(known_args.input,  skip_header_lines=1)
        logger.info("Successfully read the input file")
        data = ( lines | "Transformation Pipeline" >> ExtractionCompositeTransformation())

        logger.info("Successfully completed transformation")

        # Write the output using a "Write" transform that has side effects.
        # pylint: disable=expression-not-assigned
        lines | 'Write' >> WriteToText(known_args.output,
                                       file_name_suffix=".jsonl.gz",
                                       compression_type=CompressionTypes.GZIP,
                                       header="date, total_amount"
                                       )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
