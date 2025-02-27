from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, StringType

# Initialize SparkSession with S3 Configurations
spark = SparkSession.builder \
    .appName("ClaimsETL") \
    .getOrCreate()

# Load raw data from S3 (or local for testing)
claims_df = spark.read.option("header", "true").csv("s3://rakesh-accounting-bucket/accident_claim/claims.csv")
accidents_df = spark.read.option("header", "true").csv("s3://rakesh-accounting-bucket/accident_claim/accidents.csv")
vehicles_df = spark.read.option("header", "true").csv("s3://rakesh-accounting-bucket/accident_claim/vehicles.csv")
claimants_df = spark.read.option("header", "true").csv("s3://rakesh-accounting-bucket/accident_claim/claimants.csv")

# Print schemas for debugging
print("Schemas after reading CSVs:")
claims_df.printSchema()
accidents_df.printSchema()
vehicles_df.printSchema()
claimants_df.printSchema()

# Data Cleaning
claims_df = claims_df.dropDuplicates(["claim_id"]).na.fill({"status": "Pending"})
accidents_df = accidents_df.dropDuplicates(["accident_id"])
vehicles_df = vehicles_df.dropDuplicates(["vehicle_id"])
claimants_df = claimants_df.dropDuplicates(["claimant_id"]).na.fill({"age": 30})

# Convert data types
claims_df = claims_df.withColumn("claim_amount", col("claim_amount").cast(DoubleType())) \
                     .withColumn("processing_days", col("processing_days").cast(IntegerType()))

claimants_df = claimants_df.withColumn("age", col("age").cast(IntegerType()))

# Ensure all necessary columns exist before joins
claims_df = claims_df.withColumn("vehicle_id", col("vehicle_id").cast(StringType())) \
                     .withColumn("claimant_id", col("claimant_id").cast(StringType())) \
                     .withColumn("accident_id", col("accident_id").cast(StringType()))

vehicles_df = vehicles_df.withColumn("vehicle_id", col("vehicle_id").cast(StringType())) \
                         .withColumn("accident_id", col("accident_id").cast(StringType()))

claimants_df = claimants_df.withColumn("claimant_id", col("claimant_id").cast(StringType())) \
                           .withColumn("policy_id", col("policy_id").cast(StringType()))

# Fact Table (Ensure correct joins)
fact_claims = claims_df.join(accidents_df, "accident_id", "inner") \
                       .join(vehicles_df, "accident_id", "inner") \
                       .join(claimants_df, "claimant_id", "inner") \
                       .select(
                           "claim_id", "accident_id", "claimant_id", "vehicle_id", "policy_id",
                           "claim_date", "claim_amount", "status", "processing_days"
                       )

# Dimension Tables
dim_accidents = accidents_df.select("accident_id", "accident_date", "location", "severity")
dim_vehicles = vehicles_df.select("vehicle_id", "make", "model", "year")
dim_claimants = claimants_df.select("claimant_id", "name", "age", "policy_id")

# Debugging before writing
print("Schemas before writing to S3:")
fact_claims.printSchema()
dim_accidents.printSchema()
dim_vehicles.printSchema()
dim_claimants.printSchema()

# Write to S3 in Parquet format
fact_claims.write.mode("overwrite").parquet("s3://rakesh-accounting-bucket/accident_claim/processed/fact_claims/")
dim_accidents.write.mode("overwrite").parquet("s3://rakesh-accounting-bucket/accident_claim/processed/dim_accidents/")
dim_vehicles.write.mode("overwrite").parquet("s3://rakesh-accounting-bucket/accident_claim/processed/dim_vehicles/")
dim_claimants.write.mode("overwrite").parquet("s3://rakesh-accounting-bucket/accident_claim/processed/dim_claimants/")

# Stop Spark Session
spark.stop()


#