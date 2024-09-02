import pytest

from tests.default_spark_builder import DefaultSparkFactory
from tests.default_spark_builder import default_spark_session

@pytest.fixture(scope='session', autouse=True)
def setup_session():
    # Code to run before all tests
    print("\nCreating spark session")
    spark = default_spark_session


def test_things():
    s1 = DefaultSparkFactory()
    s2 = DefaultSparkFactory()

    assert s1 is s2
    assert id(s1) == id(s2)
    assert s1.spark == default_spark_session


