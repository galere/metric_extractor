from extraction_composite_transformation import ExtractionCompositeTransformation
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.test_pipeline import TestPipeline

import apache_beam as beam
import pytest


@pytest.fixture
def setup():
    pass


def test_pass_case():

    INPUT = [
        "2017-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,19.95",
        "2017-08-31 17:00:09 UTC,wallet00001e494c12b3083634,wallet00005f83196ec58e4ffe,13700000023.08",
        "2018-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,129.12",
        "2018-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,139.12",
    ]

    OUTPUT = [
        "2017-03-18, 2102.22",
        "2017-08-31, 13700000023.08",
        "2018-02-27, 268.24",
    ]
    with TestPipeline() as tp:
        input = tp | beam.Create(INPUT)
        output = input | ExtractionCompositeTransformation()

        assert_that(output, equal_to(OUTPUT))
