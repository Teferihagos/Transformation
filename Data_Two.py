from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("ReadCSV").getOrCreate()

# Read the CSV file into a DataFrame
df = spark.read.csv("data/sample_data.csv", header=True, inferSchema=True)

# Select specific columns
selected_df = df.select("name", "age")
selected_df.show()


# Show the selected DataFrame
filtered_df = df.filter(df["age"] > 30)
filtered_df.show()
