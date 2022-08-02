import unittest

import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline


from CompositeTransform import CompositeTransform


class TestCompositeTransfrom(unittest.TestCase):

    def test_amount_filter(self):
        """Test case for amount filer."""
        ROWS = [
            '2017-01-01 04:22:23 UTC,vm100000232023202302321,vmw100000232023202302321,16']
        p = TestPipeline()
        input = (p | beam.Create(ROWS))
        output = (input | CompositeTransform())
        assert_that(
            output,
            equal_to([]))

    def test_year_filter(self):
        """Test case for year filer."""
        ROWS = [
            '2009-01-01 04:22:23 UTC,vm100000232023202302321,vmw100000232023202302321,11']
        p = TestPipeline()
        input = (p | beam.Create(ROWS))
        output = (input | CompositeTransform())
        assert_that(
            output,
            equal_to([]))

    def test_composite_process(self):
        """Test case for all cases in composite process """
        ROWS = ['2009-01-09 02:54:25 UTC,vm100000232023202302321,vmw100000232023202302321,87.99',
                '2017-01-01 04:22:23 UTC,vm200000232023202302321,vmw200000232023202302321,9.99',
                '2017-03-18 14:09:16 UTC,vm300000232023202302321,vmw300000232023202302321,37.22',
                '2017-03-18 14:10:44 UTC,vm400000232023202302321,vmw400000232023202302321,1.08030',
                '2018-02-27 16:04:11 UTC,vm500000232023202302321,vmw500000232023202302321,100.12']
        p = TestPipeline()
        input = (p | beam.Create(ROWS))
        output = (input | CompositeTransform())
        assert_that(
            output,
            equal_to(['2017-03-18, 37.22', '2018-02-27, 100.12']))

if __name__ == '__main__':
    unittest.main()
