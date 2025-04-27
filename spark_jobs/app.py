from flask import Flask, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count
import os

app = Flask(__name__)

# Set Java Home if needed
os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/temurin-8.jdk/Contents/Home"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SBI Risk Analysis") \
    .config("spark.driver.memory", "1g") \
    .master("local[1]") \
    .getOrCreate()

# Load the dataset
df = spark.read.csv("data/processed/output.csv", header=True, inferSchema=True)

@app.route('/')
def home():
    return "Welcome to the SBI Risk Analysis API"

@app.route('/top-high-risk-years')
def top_high_risk_years():
    high_risk_years = df.filter(df["Risk_Category"].isin(["High", "Very High", "Extreme"])) \
        .orderBy(col("Gross_NPA_Closing").desc()) \
        .select("Year", "Gross_NPA_Closing", "Normalized_Likelihood", "Normalized_Impact")
    result = high_risk_years.limit(5).toPandas().to_dict(orient='records')
    return jsonify(result)

@app.route('/max-npa-year')
def max_npa_year():
    max_npa = df.orderBy(col("Gross_NPA_Closing").desc()) \
        .limit(1) \
        .select("Year", "Gross_NPA_Closing")
    result = max_npa.toPandas().to_dict(orient='records')
    return jsonify(result)

@app.route('/risk-distribution')
def risk_distribution():
    distribution = df.groupBy("Risk_Category") \
        .agg(
            avg("Gross_NPA_Closing").alias("avg_npa"),
            avg("Normalized_Likelihood").alias("avg_likelihood"),
            avg("Normalized_Impact").alias("avg_impact"),
            count("*").alias("count")
        ).orderBy("Risk_Category")
    result = distribution.toPandas().to_dict(orient='records')
    return jsonify(result)

@app.route('/correlation-analysis')
def correlation_analysis():
    correlation = df.select(
        avg("Normalized_Likelihood").alias("avg_likelihood"),
        avg("Normalized_Impact").alias("avg_impact"),
        avg("Gross_NPA_Closing").alias("avg_npa")
    )
    result = correlation.toPandas().to_dict(orient='records')
    return jsonify(result)

if __name__ == '__main__':
    app.run(debug=True)  # Run on localhost:5000
