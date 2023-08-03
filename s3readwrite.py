from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("ReadAndWriteS3") \
    .getOrCreate()


# Replace 'your/input/file/path.csv' with the actual S3 file path you want to read
input_file_path = "s3://pyspark-sets-bucket-1/csv files/car sales/car_sales_data.csv"

# Read the file from S3 into a DataFrame
df = spark.read.csv(input_file_path, header=True, inferSchema=True)

# Perform filter operations on the DataFrame
filtered_df = df.filter(df["Registration"] == "yes")


# Show the content of the DataFrames
df.show()
filtered_df.show()

# Replace 'your/output/file/path.csv' with the actual S3 file path you want to write to
output_file_path = "s3://pyspark-sets-bucket-1/csv files/car sales/updated/"


# Write the filtered DataFrame back to S3 as a CSV file
filtered_df.write.csv(output_file_path, header=True, mode="overwrite")


# Stop the SparkSession at the end of your script or when all Spark tasks are done.
spark.stop()



