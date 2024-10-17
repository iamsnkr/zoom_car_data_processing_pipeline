# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, DoubleType

# # Create Spark Session

# spark = SparkSession.builder \
#     .master("local[2]") \
#     .config("spark.sql.shuffle.partitions", 3) \
#     .appName("process_zoom_car_bookings") \
#     .getOrCreate()

# job level parameters
date_info =  dbutils.widgets.get("date") 
bookings_path = dbutils.widgets.get("path") + f"zoom_car_bookings_{date_info}.json" 
date_time_format = dbutils.widgets.get("date_time_format")

# Schema information
bookings_schema = StructType([
    StructField("booking_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("car_id", StringType(), True),
    StructField("booking_date", StringType(), True),
    StructField("start_time", StringType(), True),
    StructField("end_time", StringType(), True),
    StructField("status", StringType(), True),
    StructField("total_amount", DoubleType(), True)  # Changed to DoubleType
])

# Reading Data
bookings_raw_df = spark.read.format("json") \
    .option("inferSchema", False) \
    .schema(bookings_schema) \
    .load(bookings_path)

# Schema Info
bookings_raw_df.printSchema()

# Sample data
bookings_raw_df.show(5)
"""
# Remove records with null values in critical fields
# (booking_id, customer_id, car_id, booking_date).
# Validate date formats.
# Ensure status is one of the predefined statuses
# (completed, cancelled, pending).
"""
valid_bookings_statuses = ["completed", "cancelled", "pending"]

print(f"booking data count before {bookings_raw_df.count()}")
valid_bookings_df = bookings_raw_df.dropna(subset=("booking_id", "customer_id", "car_id", "booking_date")) \
    .withColumn("valid_start_time", to_timestamp("start_time", date_time_format)) \
    .withColumn("valid_booking_date", to_date("booking_date", "yyyy-MM-dd")) \
    .withColumn("valid_end_time", to_timestamp("end_time", date_time_format)) \
    .filter(col("valid_start_time").isNotNull() &
            col("valid_booking_date").isNotNull() &
            col("valid_end_time").isNotNull()) \
    .drop("valid_start_time", "valid_booking_date", "valid_end_time") \
    .filter(col("status").isin(valid_bookings_statuses))

valid_bookings_df.printSchema()
valid_bookings_df.show(5)
print(f"booking after {valid_bookings_df.count()}")


"""
Bookings Data Transformations:
■ Parse start_time and end_time into separate date and time
columns.
■ Calculate the total duration of each booking.
"""
bookings_transformed_df = valid_bookings_df\
    .withColumn("start_date", split(col("start_time"), 'T')[0])\
    .withColumn("starting_time", split(split(col("start_time"), 'T')[1], 'Z')[0]) \
    .withColumn("end_date", split(col("end_time"), 'T')[0]) \
    .withColumn("ending_time", split(split(col("end_time"), 'T')[1], 'Z')[0]) \
    .withColumn("total_duration",
                (unix_timestamp('end_time', date_time_format) -
                 unix_timestamp('start_time', date_time_format)) / 3600)\
    .drop("start_time", "end_time")

bookings_transformed_df.printSchema()

# print final df sample data
bookings_transformed_df.show(5)

"""
Loading cleaned data into the staging_bookings_delta table.
"""
staging_bookings_delta = dbutils.widgets.get("staging_bookings_delta")
bookings_transformed_df.write.format("delta").mode("overwrite").saveAsTable(staging_bookings_delta)


