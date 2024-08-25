from pyspark.sql.functions import sum, when, col, lit, min, max, count, countDistinct, isnan, greatest
from pyspark.sql.functions import row_number, regexp_replace, regexp_extract, substring, concat, replace, split, explode, map_from_arrays, regexp_extract_all, coalesce, position, posexplode, expr
from pyspark.sql.functions import dateadd, format_number
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import pandas as pd
from functools import reduce
from pyspark.sql import functions as F


spark = SparkSession \
    .builder \
    .appName("App Ad").getOrCreate()
#    .config("spark.some.config.option", "some-value") \

spark.conf.set("spark.sql.repl.eagerEval.enabled", True)

file = "data/3.csv"

df = spark.read.format("csv") \
       .option("sep", ";")\
       .option("header", "false") \
       .option("inferSchema", "true") \
       .load(file) 

column_names = []
for i in range(len(df.columns)):
    column_names.append("set" + str(i))
df = df.toDF(*column_names)

print(df.count())
print(len(df.columns))

df.show(truncate=False)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, posexplode, expr, collect_list, struct

df = df.withColumn("all_matches", split(col("set0"), "\\D+"))

# Explode the array of matches with their positions
exploded_df = df.select("set0", posexplode(col("all_matches")).alias("pos", "match"))

# Filter out empty matches (from consecutive non-digit characters)
filtered_df = exploded_df.filter(col("match") != "")

# Calculate the actual position of each match in the original string
result_df = filtered_df.withColumn("actual_pos", expr("instr(set0, match)"))

# Group by original text and collect matches and positions into a list of structs
final_df = result_df.groupBy("set0").agg(collect_list(struct("match", "actual_pos")).alias("match_positions"))

# Show the results
final_df.show(truncate=False)
df.show(truncate=False)
result_df.show