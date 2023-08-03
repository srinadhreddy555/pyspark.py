
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType,IntegerType

spark = SparkSession.builder.appName('withcolumn').getOrCreate()

data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
df.printSchema()

#it will show all the columns
df.show(truncate=False)

#Change DataType using PySpark withColumn

df2 = df.withColumn("salary",col("salary").cast("Integer"))
df2.printSchema()
df2.show(truncate=False)

#Update The Value of an Existing Column
df3 = df.withColumn("salary",col("salary")*100)
df3.printSchema()
df3.show(truncate=False) 

#Create a Column from an Existing
df4 = df.withColumn("CopiedColumn",col("salary")* -1)
df4.printSchema()

#adding a new column
df5 = df.withColumn("Country", lit("USA"))
df5.printSchema()

#Add a New Column using withColumn
df6 = df.withColumn("Country", lit("USA")) \
   .withColumn("anotherColumn",lit("anotherValue"))
df6.printSchema()

#updating the column  name 
df.withColumnRenamed("gender","sex") \
  .show(truncate=False) 
  
#doping a column
df4.drop("CopiedColumn") \
.show(truncate=False) 
