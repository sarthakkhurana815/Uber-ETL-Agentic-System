from pyspark.sql.functions import *
from config import (source_path, bronze_path, silver_path, quarantine_path, gold_daily_path,
                    gold_location_path, gold_payment_path)
 
def read_source(spark):
    raw_df = spark.read.csv(source_path, header=True, inferSchema=True, nullValue="null")
    for c in raw_df.columns:
        raw_df = raw_df.withColumnRenamed(c, c.replace(" ", "_"))
    return raw_df
 
def build_bronze(spark, raw_df):
    bronze_df = raw_df.withColumn("Booking_ID", regexp_replace(col("Booking_ID"), '"', "")) \
                      .withColumn("Customer_ID", regexp_replace(col("Customer_ID"), '"', "")) \
                      .withColumn("ingestion_timestamp", current_timestamp()) \
                      .withColumn("source_file", lit(source_path))
    
    bronze_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(bronze_path)
    return spark.read.format("delta").load(bronze_path)
 
def build_silver(spark, bronze_df):
    silver_df = bronze_df.withColumn("booking_timestamp", to_timestamp(concat_ws(" ", date_format(col("Date"), "yyyy-MM-dd"), date_format(col("Time"), "HH:mm:ss")), "yyyy-MM-dd HH:mm:ss")) \
                         .withColumn("Booking_Value", col("Booking_Value").cast("double")) \
                         .withColumn("Ride_Distance", col("Ride_Distance").cast("double")) \
                         .withColumn("Driver_Ratings", col("Driver_Ratings").cast("double")) \
                         .withColumn("Customer_Rating", col("Customer_Rating").cast("double"))
 
    silver_df = silver_df.dropDuplicates(["Booking_ID"])
 
    # Separate valid and invalid (quarantine) records
    valid_df = silver_df.filter(
        (col("Booking_ID").isNotNull()) &
        (col("Customer_ID").isNotNull()) &
        (col("Booking_Value").isNull() | (col("Booking_Value") >= 0)) & 
        (col("Ride_Distance").isNull() | (col("Ride_Distance") >= 0)) & 
        (col("Driver_Ratings").isNull() | col("Driver_Ratings").between(1, 5))
    )
    
    invalid_df = silver_df.subtract(valid_df)
 
    invalid_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(quarantine_path)
    valid_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path)
    
    return spark.read.format("delta").load(silver_path)
 
def build_gold(spark, silver_df):
    daily_metrics = [
        count("Booking_ID").alias("total_trips"),
        sum("Booking_Value").alias("total_revenue"),
        avg("Booking_Value").alias("average_trip_amount"),
        avg("Ride_Distance").alias("average_trip_distance")
    ]
 
    daily_summary_df = silver_df.groupBy("Date").agg(*daily_metrics)
    daily_summary_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_daily_path)
 
    location_summary_df = silver_df.groupBy("Pickup_Location").agg(*daily_metrics)
    location_summary_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_location_path)
 
    payment_summary_df = silver_df.groupBy("Payment_Method").agg(*daily_metrics)
    payment_summary_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_payment_path)
 
    return {
        "daily_summary": daily_summary_df,
        "location_summary": location_summary_df,
        "payment_summary": payment_summary_df
    }
 
def run_etl(spark):
    raw_df = read_source(spark)
    bronze_df = build_bronze(spark, raw_df)
    silver_df = build_silver(spark, bronze_df)
    gold_dfs = build_gold(spark, silver_df)
    return {
        "status": "success",
        "bronze_rows": bronze_df.count(),
        "silver_rows": silver_df.count(),
        "daily_summary_rows": gold_dfs["daily_summary"].count(),
        "location_summary_rows": gold_dfs["location_summary"].count(),
        "payment_summary_rows": gold_dfs["payment_summary"].count()
    }
 