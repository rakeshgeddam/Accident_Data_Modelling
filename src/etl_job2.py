from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, when, col, count

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("ClaimsAnalysis") \
    .getOrCreate()

# Load Parquet files from S3
fact_claims = spark.read.parquet("s3://rakesh-accounting-bucket/accident_claim/processed/fact_claims/")
dim_accidents = spark.read.parquet("s3://rakesh-accounting-bucket/accident_claim/processed/dim_accidents/")

# Query 1: Average claim amount by severity
avg_claim_by_severity = fact_claims.join(dim_accidents, "accident_id", "inner") \
    .groupBy("severity") \
    .agg(avg("claim_amount").alias("avg_payout"))
avg_claim_by_severity.write.mode("overwrite").parquet("s3://rakesh-accounting-bucket/accident_claim/results/avg_claim_by_severity/")

# Query 2: Processing time by status
avg_processing_by_status = fact_claims.groupBy("status") \
    .agg(avg("processing_days").alias("avg_days"))
avg_processing_by_status.write.mode("overwrite").parquet("s3://rakesh-accounting-bucket/accident_claim/results/avg_processing_by_status/")

# Query 3: Rejection rate by location
rejection_rate_by_location = fact_claims.join(dim_accidents, "accident_id", "inner") \
    .groupBy("location") \
    .agg(
        (sum(when(col("status") == "Rejected", 1).otherwise(0)) / count("*")).alias("rejection_rate")
    ) \
    .orderBy(col("rejection_rate").desc())
rejection_rate_by_location.write.mode("overwrite").parquet("s3://rakesh-accounting-bucket/accident_claim/results/rejection_rate_by_location/")

# Show results (optional)
avg_claim_by_severity.show()
avg_processing_by_status.show()
rejection_rate_by_location.show(10)

# Stop SparkSession
spark.stop()