from pyspark.sql import SparkSession
from pyspark.sql import functions as f

# Create a SparkSession
spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv("data/sample_data.csv", header=True, inferSchema=True)

# Sort the DataFrame by the "age" column in ascending order
sorted_df = df.orderBy("age")

# To sort in descending order, you can use the "desc" function
# sorted_df = df.orderBy("age", ascending=False)

# Show the sorted DataFrame
sorted_df.show()
