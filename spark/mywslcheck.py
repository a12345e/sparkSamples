import random
from pathlib import Path
from string import ascii_letters
from typing import Dict, Any, List
from pyspark import SparkFiles, SparkConf, SparkContext
from pyspark.sql.types import *
import pyspark
from pyspark.sql import SparkSession, Observation, Window
from pyspark.sql.functions import explode, explode_outer, posexplode_outer, sum
import requests
from pprint import pprint


def create_random_username(num_characters: int, username_base: str) -> str:
    random_string = ''.join(random.choices(ascii_letters, k=num_characters))
    return f"{username_base}_{random_string}"


def func5(spark: SparkSession):
    # Prepare data
    data = [("James", "Sales", 3000), \
            ("Michael", "Sales", 4600), \
            ("Robert", "Sales", 4600), \
            ("James", "Sales", 3000)
            ]
    columns = ["employee_name", "department", "salary"]

    # Create DataFrame
    df = spark.createDataFrame(data=data, schema=columns)
    df.printSchema()
    df.show(truncate=False)

    # Using distinct()
    distinctDF = df.distinct()
    distinctDF.show(truncate=False)

    # Using dropDuplicates()
    dropDisDF = df.dropDuplicates(["department", "salary"])
    dropDisDF.show(truncate=False)

    # Using dropDuplicates() on single column
    dropDisDF = df.dropDuplicates(["salary"]).select("salary")
    dropDisDF.show(truncate=False)
    print(dropDisDF.collect())


def get_countries() -> list():
    countries_site = f"https://restcountries.com/v3.1/all?fields=name"
    response = requests.get(countries_site)
    json_response = response.json()

    countries_list = []
    for i in json_response:
        countries_list.append(i['name']['common'])

    return countries_list


def func4(spark: SparkSession):
    pyspark.sql.Catalog.createTable(tableName='kook', path='/tmp/alon_table.parquet')
    spark.sql("select * from cook")


def generate_random_dataframe(spark: SparkSession, schema: StructType, size: int,
                              partitions_value_sets: Dict[str, List[Any]], partitions: int):
    data = []
    for i in range(0,size):
        row = []
        for field in schema:
             if field.name in partitions_value_sets:
                 row.append(random.choice(partitions_value_sets[field.name]))
             else:
                if field.dataType == IntegerType():
                    row.append(random.randint(0,100))
                elif field.dataType == StringType:
                    row.append('some_'+str(random.randint(0, 100)))
                else:
                    row.append(None)
        data.append(tuple(row))
    df = spark.createDataFrame(data=data, schema=schema)
    return df


def infinite_sequence():
    num = 0
    while True:
        yield num
        num += 1


def dataFrameBuilder(spark: SparkSession):
    y = infinite_sequence()
    for x in infinite_sequence():
        print(x)
        if x > 5:
            return


def func2(spark: SparkSession):
    arrayData = [
        ('James', ['Java', 'Scala'], {'hair': 'black', 'eye': 'brown'}),
        ('Michael', ['Spark', 'Java', None], {'hair': 'brown', 'eye': None}),
        ('Robert', ['CSharp', ''], {'hair': 'red', 'eye': ''}),
        ('Washington', None, None),
        ('Jefferson', ['1', '2'], {})]
    df = spark.createDataFrame(data=arrayData, schema=['name', 'knownLanguages', 'properties'])
    df.printSchema()
    df.show()
    df2 = df.select(df.name, explode(df.knownLanguages))
    df2.printSchema()
    df2.show()
    df.select(df.name, explode_outer(df.knownLanguages)).show()
    df.select(df.name, explode_outer(df.properties)).show()
    df.select(df.name, posexplode_outer(df.knownLanguages)).show()
    df.select(df.name, posexplode_outer(df.properties)).show()


def func1(spark: SparkSession) -> None:
    data = [(1, 2), (3, 4), (5, 6)]
    df = spark.createDataFrame(data, ["col1", "col2"])

    # Define a function to add two columns
    def add_columns(row):
        return (row[0], row[1], row[0] + row[1])

    new_rdd = df.rdd.map(add_columns)

    # Convert the RDD back to a DataFrame
    new_df = new_rdd.toDF(["col1", "col2", "sum"])

    # Display the results
    new_df.show()


def func6(spark: SparkSession):
    usernames_list = ['Alice', 'Bob', 'Charlie', 'David', 'Eva', 'Frank', 'Grace', 'Henry', 'Ivy', 'Jack'][:1]
    countries_list = get_countries()[:1]
    partitions_value_sets = {
        'country' : countries_list,
        'username': usernames_list
    }
    schema = StructType([
        StructField("country", StringType(), True),
        StructField("username", StringType(), True),
        StructField("id", IntegerType(), True)])
    df = generate_random_dataframe(spark, schema, 100, partitions_value_sets, 10)
    df  = df.repartition(10, 'country','username')
    # df = spark.createDataFrame(
    #     [(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])

    def func(itr):
        for person in itr:
            print(person.country,' ', person.username, ' ', person.id)

    df.foreachPartition(func)
    print(df.rdd.getNumPartitions())
    df.repartition(3).write.mode("overwrite").partitionBy('country','username').parquet('/tmp/table_alon.parquet')
    dr = spark.read.parquet('/tmp/table_alon.parquet')
    print(dr.rdd.getNumPartitions())
    dr.groupby('country','username').count().orderBy('country', 'username').show(200,truncate=False)

def fill(spark: SparkSession):
    data = [(1, None, "A"), (2, 3, None), (None, 4, "C")]
    columns = ["col1", "col2", "col3"]
    df = spark.createDataFrame(data, columns)

    # Drop rows with any null values
    df_drop = df.na.drop()

    # Fill null values with specified values
    df_fill = df.na.fill({"col1": 0, "col2": 1, "col3": "unknown"})

    # Replace specified values
    df_replace = df.na.replace(1, 100)

    # Show the results
    df_drop.show()
    df_fill.show()
    df_replace.show()

def waterMark(spark: SparkSession):
    from pyspark.sql import Row
    from pyspark.sql.functions import timestamp_seconds
    df = spark.readStream.format("rate").load().selectExpr(
        "value % 5 AS value", "timestamp")
    df.select("value", df.timestamp.alias("time")).withWatermark("time", '10 minutes').show()

def window(spark : SparkSession):
    df = spark.createDataFrame(
        [(1, "a"), (1, "a"), (2, "a"), (1, "b"), (2, "b"), (3, "b")], ["id", "category"])
    window = Window.partitionBy("category").orderBy("id").rangeBetween(Window.currentRow, 1)
    df.withColumn("sum", sum("id").over(window)).sort("category", "id").show()

def getFileSystem(spark: SparkSession):
    sc = spark.sparkContext
    URI = sc._gateway.jvm.java.net.URI
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
    # Prepare a FileSystem manager
    fs = FileSystem.get(URI("hdfs://localhost:9000"),Configuration())
    return fs

def addFile(spark: SparkSession, name: str):
    spark.sparkContext.addFile("/tmp/alon_add_file")

def delete_path(spark, path):
    sc = spark.sparkContext
    fs = getFileSystem(spark)
    fs.delete(sc._jvm.org.apache.hadoop.fs.Path(path), True)

def write_binary_file_to_hdfs(spark: SparkSession, path: str, content:bytearray):

    # Convert the binary content to a DataFrame
    df = spark.createDataFrame([(content,)], ["binaryData"])

    df.write \
        .mode("overwrite") \
        .save(path)

def write_binary_file_to_hdfs_as_sequence(spark: SparkSession, path: str, content:bytearray):

    data = [('key1',content)]
    rdd  = spark.sparkContext.parallelize(data)
    rdd.saveAsSequenceFile(path)


def write_text_file(spark: SparkSession, content: str, path: str):
    rdd = spark.sparkContext.parallelize([content])
    rdd.repartition(1).saveAsTextFile(path)

def createSparkSession():
    spark = SparkSession.builder.remote("spark://172.17.152.175:7077").getOrCreate()
    return spark
def run():
    spark = SparkSession.builder \
        .appName("RemotePySpark").master("local[*]").getOrCreate()

    # Example data processing
    data = [("John", 30), ("Doe", 25)]
    df = spark.createDataFrame(data, ["Name", "Age"])
    print(df.schema)
    df.show()



#     spark = createSparkSession()
#     path = "hdfs://localhost:9000/tmp/a12345e/output.txt"
#     delete_path(spark, path)
#     delete_path(spark,"hdfs://localhost:9000/tmp/a12345e/binary")
#     write_text_file(spark,"helo world","hdfs://localhost:9000/tmp/a12345e/output.txt")
# #    window(spark)
#     write_binary_file_to_hdfs_as_sequence(spark,"hdfs://localhost:9000/tmp/a12345e/binary",bytearray([1, 2, 3, 4, 5]))


    #window(spark)


# Press the green button in the gutter to run the script.
run()
