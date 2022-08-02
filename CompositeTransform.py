import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from core import ExtractFields, AmountFilter, DateFilter, FormatFields

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class CompositeTransform(beam.PTransform):
    """Composit Transform process consists multiple processes."""

    def expand(self, pcoll):
        res = (
            pcoll
            | 'extraction process' >> beam.ParDo(ExtractFields())
            | 'filter the records with amount' >> beam.ParDo(AmountFilter())
            | 'filter the records with date' >> beam.ParDo(DateFilter())
            | 'sum by key' >> beam.CombinePerKey(sum)
            | 'format the data' >> beam.ParDo(FormatFields())
        )
        return res


def main(input_file: str, out_file_path: str):
    """Main method consists of the business logic. """
    logger.info('process has started')
    # create pipeline
    trans_proc = beam.Pipeline(options=PipelineOptions())

    # read input
    trans_data = (trans_proc
                  | 'Read Transactions csv' >> beam.io.ReadFromText(input_file, skip_header_lines=1)
                  )
    # apply composit transformation
    formatted_data = (trans_data | 'prepare out put' >> CompositeTransform())

    # write to file
    (formatted_data
        | 'writing to a file' >> beam.io.WriteToText(f'{out_file_path}results.jsonl',
                                                     file_name_suffix='.gz',
                                                     compression_type=beam.io.filesystem.CompressionTypes.GZIP,
                                                     header='date, total_amount')
     )
    res = trans_proc.run()
    res.wait_until_finish()
    logger.info('process has completed')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file',
                        help='input csv file to process',
                        required=True)
    parser.add_argument('--out_file_path',
                        help='path to output file',
                        default='./output/')
    args = parser.parse_args()
    main(args.input_file, args.out_file_path)
