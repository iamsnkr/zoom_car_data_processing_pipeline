import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, DoubleType

# Create Spark Session

spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", 3) \
    .appName("process_zoom_car_customers") \
    .getOrCreate()

# Paths of Bookings and Customers
date_info = "20241017"
customers_path = f"/FileStore/Order_Tracking/staging/zoom_car_customers_{date_info}.json"

# Schema information
customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("status", StringType(), True)
])

# Reading Data
customers_raw_df = spark.read.format("json") \
    .option("inferSchema", False) \
    .schema(customers_schema) \
    .load(customers_path)

# Schema Info
customers_raw_df.printSchema()

# Sample data
customers_raw_df.show(5)

# Remove records with null values in critical fields
# (customer_id,name, email).
# Validate email formats.
# Ensure status is one of the predefined statuses (active, inactive)
print(f"customers data count before : {customers_raw_df.count()}")
proper_email_regex = "^[a-zA-Z0-9._]+@[a-z0-9._]+\\.[a-z]{2,}"
valid_customers_statuses = ["active", "inactive"]
valid_customers_df = customers_raw_df.dropna(subset=("customer_id", "name", "email")) \
    .filter(col("status").isin(valid_customers_statuses)) \
    .filter(col("email").rlike(proper_email_regex)) \
    .withColumn("valid_signup_date", to_date(col("signup_date"), "yyyy-MM-dd")) \
    .filter(col("valid_signup_date").isNotNull()).drop("valid_signup_date")

valid_customers_df.printSchema()
valid_customers_df.show(5)
print(f"customers data count after : {valid_customers_df.count()}")


def format_phone_number(phone_number):
    # Remove all non-digit characters except for leading '+'
    digits = re.sub(r'(?<!\+)\D', '', phone_number)

    # Format number based on its length
    if len(digits) == 10:  # Standard US number
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits.startswith('1'):  # US country code
        return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    elif len(digits) > 11 and digits.startswith('1'):  # International format
        return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]} (Ext: {digits[11:]})" if len(
            digits) > 11 else f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    elif len(digits) > 10:  # Handle other formats
        return f"+{digits[:len(digits) - 10]} ({digits[-10:-7]}) {digits[-7:-4]}-{digits[-4:]}" + (
            f" (Ext: {digits[-3:]})" if len(digits) > 10 else "")

    return phone_number  # Return original if it doesn't match


# Register UDF
format_phone_number_udf = udf(format_phone_number, StringType())

"""
Customers Data Transformations:
○ Normalize phone numbers to a standard format.
○ Calculate customer tenure from signup_date.
"""
customers_transformed_df = valid_customers_df \
    .withColumn("phone_number", format_phone_number_udf("phone_number")) \
    .withColumn("signup_date", to_date(col('signup_date'), 'yyyy-MM-dd')) \
    .withColumn("tenure_in_days", date_diff(current_date(), col("signup_date")))


customers_transformed_df.printSchema()

# print final df sample data
customers_transformed_df.show(10)

"""
Loading cleaned data into the staging_customers_delta table.
"""
staging_customers_delta = "hive_metastore.default.staging_customers_delta"
customers_transformed_df.write.format("delta").mode("overwrite").saveAsTable(staging_customers_delta)
