"""This module defines classes required for the ETL logic."""
from datetime import datetime

import apache_beam as beam


class ExtractFields(beam.DoFn):
    """Returns list of items which are passed date & amount values. """

    def process(self, element):
        if element:
            fields = element.split(',')
            date = self._parse_date(fields[0])
            amount = float(fields[3])
            return [(date, amount)]

    def _parse_date(self, input_value):
        if input_value:
            return input_value[0:10]


class AmountFilter(beam.DoFn):
    """Filters the records with amount > 20 """

    def process(self, record):
        if record:
            if record[1] > 20:
                return [record]


class DateFilter(beam.DoFn):
    """Filters the records with Date value > 2010 """

    def process(self, record):
        if record:
            if self._get_year(record[0]) > 2010:
                return [record]

    def _get_year(self, date_str):
        if date_str:
            date = datetime.strptime(date_str, '%Y-%m-%d')
            return date.year


class FormatFields(beam.DoFn):
    """Prepares records in csv format """

    def process(self, key):
        res = f'{key[0]}, {key[1]}'
        return [res]
