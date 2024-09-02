from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, struct
from pyspark.sql.functions import min

# Initialize SparkSession
spark = SparkSession.builder.appName("Find Minimum Tuple Example").getOrCreate()

# Sample Data
data = [
    ("store1", {"apple": 10, "banana": 20}),
    ("store2", {"apple": 15, "orange": 25}),
    ("store3", {"banana": 30, "orange": 5})
]

# Create DataFrame with a map column
df = spark.createDataFrame(data, ["store", "sales"])
df.printSchema()
df.show()

df.show(truncate=False)
exploded_df = df.withColumn("sales_exploded", explode(col("sales")).alias("key","value"))

exploded_df.show(truncate=False)

# Convert the exploded array into separate columns
exploded_df = exploded_df.withColumn("key", col("sales_exploded.key"))
exploded_df = exploded_df.withColumn("value", col("sales_exploded.value"))

# Group by the original row and find the minimum value
result_df = exploded_df.groupBy("store").agg(
    min(struct("value", "key")).alias("min_sales")
)

result_df.show(truncate=False)