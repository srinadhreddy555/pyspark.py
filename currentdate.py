from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# Create SparkSession
spark = SparkSession.builder \
            .appName('SparkByExamples.com') \
            .getOrCreate()

# Get the current date and time
current_datetime_df = spark.range(1).select(current_timestamp().alias("current_datetime"))

# Show the result
current_datetime_df.show(truncate=False)
