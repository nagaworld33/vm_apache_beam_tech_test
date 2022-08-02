import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from core import ExtractFields, AmountFilter, DateFilter, FormatFields

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def main(input_file: str, out_file_path: str):
    """Main method consists of the business logic. """
    logger.info('Process has started')

    # create pipeline
    trans_proc = beam.Pipeline(options=PipelineOptions())

    # read input
    trans_data = (trans_proc
                  | 'Read Transactions csv' >> beam.io.ReadFromText(input_file, skip_header_lines=1)
                  )

    # extract only required fields
    extracted_data = (trans_data | 'extraction process' >>
                      beam.ParDo(ExtractFields()))

    # Find all transactions have a `transaction_amount` greater than `20`
    amt_filtered_data = (extracted_data
                         | 'filter the records with amount' >> beam.ParDo(AmountFilter())
                         )
    # Exclude all transactions made before the year `2010`
    dt_filtered_data = (amt_filtered_data
                        | 'filter the records with date' >> beam.ParDo(DateFilter())
                        )

    # summed data with combine with key
    summed_data = (dt_filtered_data
                   | 'sum by key' >> beam.CombinePerKey(sum)
                   )
    formatted_data = (summed_data
                      | 'format the data' >> beam.ParDo(FormatFields())
                      )

    # write to file
    (formatted_data
        | 'writing to a file' >> beam.io.WriteToText(f'{out_file_path}results.jsonl',
                                                     file_name_suffix='.gz',
                                                     compression_type=beam.io.filesystem.CompressionTypes.GZIP,
                                                     header='date, total_amount')
     )
    res = trans_proc.run()
    res.wait_until_finish()
    logger.info('Process has completed')


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
