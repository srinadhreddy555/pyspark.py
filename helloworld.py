



from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("HelloWorld") \
    .getOrCreate()

# Create a DataFrame with a single column named "message"
data = [("Hello, World!",)]
df = spark.createDataFrame(data, ["message"])

# Show the DataFrame content
df.show()

# Do more operations with the DataFrame or other Spark components here (if needed).

# Stop the SparkSession at the end of your script or when all Spark tasks are done.
spark.stop()
