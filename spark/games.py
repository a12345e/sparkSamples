from itertools import chain

from pyspark.sql import SparkSession
from pyspark.sql.functions import create_map, lit, when, col
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("CreateMapTest").getOrCreate()

data = [("Alice", "HR", 3000), ("Bob", "Engineering", 4000)]
columns = ["Name", "Department", "Salary"]

df = spark.createDataFrame(data, columns)
map = create_map(
    "Department", lit("Department"),
    "Salary", lit("Salary"))

map1 = {
"Department": "Department",
"Salary1" : "Salary"
}
array = [ x for tup in [(z,lit(d)) for z,d in map1.items()] for x in tup]
print(array)

print(map['Department'])
print(map['HR'])
df_with_map = df.withColumn("Name",create_map(array))

df_with_map.show(truncate=False)