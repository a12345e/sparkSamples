from pyspark.sql import SparkSession


def create_spark_session():
    # Create a Spark session
    spark = SparkSession.builder \
        .appName("PySpark Test Session") \
        .master("local[*]") \
        .getOrCreate()
    return spark

class DefaultSparkFactory:
    _instances = {}

    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__new__(cls)
            cls._instances[cls] = instance
            cls._spark = create_spark_session()
        return cls._instances[cls]

    @property
    def spark(self):
        return self._spark

