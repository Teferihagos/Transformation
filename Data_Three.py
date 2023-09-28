from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv("data/sample_data.csv", header=True, inferSchema=True)

# Add a new column "SalaryPlus10000"
# Add a new column "SalaryPlus10000"
df_with_new_column = df.withColumn("SalaryPlus10000",df.Salary + 10000)
df_with_new_column.show()

