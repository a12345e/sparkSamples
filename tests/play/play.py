from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from tests.infra.dataframes_helpers import print_dataframe_schema_and_rows

# Initialize Spark session
spark = SparkSession.builder.appName("array_of_tuples").getOrCreate()

# Sample data with key and value columns
data = [
    ("1", 1, 11),
    ("2", 2,12),
    ("3", 3, 13),
    ("4", 4, 14)
]

schema = ["id", "key1", "value1"]

df = spark.createDataFrame(data, schema)

# Step 1: Create an array of 2-tuples (key, value)
array_of_tuples = F.array(
    F.struct(F.lit(1).alias('kaka'), F.col("value1"))
)

df_with_tuples = df.withColumn("array_of_tuples", array_of_tuples)

# Step 2: Optionally, add more tuples to the array
# If you want to append more tuples, you can use concat like this:
# df_with_tuples = df_with_tuples.withColumn("array_of_tuples",
#                                            F.concat(F.col("array_of_tuples"),
#                                            F.array(F.struct(F.lit("new_key"), F.lit("new_value")))))

# Step 3: Explode the array of tuples into individual rows
df_exploded = df_with_tuples.withColumn("exploded_tuple", F.explode("array_of_tuples"))

print_dataframe_schema_and_rows(df_exploded)
# Step 4: Extract 'key' and 'value' from the exploded tuples
df_final = df_exploded.withColumn("key", F.col("exploded_tuple.kaka")) \
                      .withColumn("value", F.col("exploded_tuple.value1"))

print_dataframe_schema_and_rows(df_final)
