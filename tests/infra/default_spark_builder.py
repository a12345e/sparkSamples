from pyspark.sql import SparkSession
import pytest

@pytest.fixture(scope='session')
def spark():
    # Create a Spark session
    print('create spark session ...')
    session = SparkSession.builder \
        .appName("PySpark Test Session") \
        .master("local[*]") \
        .getOrCreate()
    print('created spark session')
    yield session
    print('stop spark session ...')
    session.stop()
    print('stop spark session stopped')


