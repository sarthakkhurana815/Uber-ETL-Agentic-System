from pyspark.sql.functions import *
from config import silver_path
 
def run_quality_checks(spark):
    df = spark.read.format("delta").load(silver_path)
    row_count = df.count()
    
    required_columns = ["Date", "Time", "Booking ID", "Booking Status", "Customer ID", "Vehicle Type"]
    null_counts = df.select([count(when(col(c).isNull(), 1)).alias(c) for c in required_columns]).first().asDict()
 
    invalid_distance = df.filter((col("Ride Distance").isNotNull()) & (col("Ride Distance") < 0)).count()
    invalid_amount = df.filter((col("Booking Value").isNotNull()) & (col("Booking Value") < 0)).count()
    invalid_ratings = df.filter((col("Driver Ratings").isNotNull()) & (~col("Driver Ratings").between(1, 5))).count()
 
    passed = (
        row_count > 0
        and all(value == 0 for value in null_counts.values())
        and invalid_distance == 0
        and invalid_amount == 0
        and invalid_ratings == 0
    )
 
    return {
        "status": "passed" if passed else "failed",
        "row_count": row_count,
        "null_counts": null_counts,
        "invalid_distance": invalid_distance,
        "invalid_amount": invalid_amount,
        "invalid_ratings": invalid_ratings
    }
 