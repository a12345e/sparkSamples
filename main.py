

from pyspark.sql import SparkSession

def run():
    spark = SparkSession.builder.appName("PySparkExample").getOrCreate()

# Sample DataFrame creation
    data = [("James", "Smith", "USA", "CA"),
            ("Michael", "Rose", "USA", "NY"),
            ("Robert", "Williams", "USA", "CA"),
            ("Maria", "Jones", "USA", "FL")]

    columns = ["firstname", "lastname", "country", "state"]

    df = spark.createDataFrame(data, columns)
    print(type(df))
    rdd = spark.sparkContext.parallelize([1,2,3,4,5,6])
    print(rdd.count())


# Show DataFrame content
    df.where(df.lastname=='Smith').show()

# Stop the SparkSession
    spark.stop()

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    run()
