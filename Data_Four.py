from pyspark.sql import SparkSession
from pyspark.sql import functions as f


# Create a SparkSession
spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv("sample_data.csv", header=True, inferSchema=True)

# Group by "age" and calculate the average salary for each age group
result_df = df.groupBy("age").agg(f.avg(df.Salary).alias("AverageSalary"))
result_df.show()