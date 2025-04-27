import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, count  # Import count function

# Set Java Home (replace path if different)
os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/temurin-8.jdk/Contents/Home"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SBI Risk Analysis") \
    .config("spark.driver.memory", "1g") \
    .master("local[1]") \
    .getOrCreate()  # This starts the Spark session

# Load the dataset
df = spark.read.csv("data/processed/output.csv", header=True, inferSchema=True)

# Show the first few rows to verify data loading
df.show(5)

# Step 1: Risk Analysis - Highest NPA Years

# Filter data for high-risk categories and order by Gross NPA Closing in descending order
high_risk_years = df.filter(df["Risk_Category"].isin(["High", "Very High", "Extreme"])) \
    .orderBy(col("Gross_NPA_Closing").desc()) \
    .select("Year", "Gross_NPA_Closing", "Normalized_Likelihood", "Normalized_Impact")

# Show top 5 highest risk years
print("Top 5 Highest Risk Years:")
high_risk_years.show(5)

# Step 2: Year with Maximum NPA
max_npa_year = df.orderBy(col("Gross_NPA_Closing").desc()) \
    .limit(1) \
    .select("Year", "Gross_NPA_Closing")

print("Year with Maximum NPA:")
max_npa_year.show()

# Step 3: Risk Category Distribution and Average NPA
risk_category_distribution = df.groupBy("Risk_Category") \
    .agg(
        avg("Gross_NPA_Closing").alias("avg_npa"),
        avg("Normalized_Likelihood").alias("avg_likelihood"),
        avg("Normalized_Impact").alias("avg_impact"),
        count("*").alias("count")  # Count the number of records in each Risk Category
    ).orderBy("Risk_Category")

print("Risk Category Distribution and Average NPA:")
risk_category_distribution.show()

# Step 4: Correlation between Normalized Likelihood, Normalized Impact, and Gross NPA Closing
correlation_analysis = df.select(
    avg("Normalized_Likelihood").alias("avg_likelihood"),
    avg("Normalized_Impact").alias("avg_impact"),
    avg("Gross_NPA_Closing").alias("avg_npa")
)

print("Risk Correlation Analysis (Avg Likelihood, Avg Impact, Avg NPA):")
correlation_analysis.show()

# Final Analysis Output (as an example)
print("Risk Analysis Summary:")
print(f"Highest Risk Years (based on NPA):")
high_risk_years.show(5)
print(f"Year with Maximum NPA: {max_npa_year.collect()}")
print(f"Risk Category Distribution and Avg NPA: {risk_category_distribution.collect()}")
print(f"Risk Correlation Analysis: {correlation_analysis.collect()}")