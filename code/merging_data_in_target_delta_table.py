from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from pydeequ.checks import CheckLevel, Check
from pydeequ.verification import VerificationSuite, VerificationResult

spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", 3) \
    .appName("process_zoom_car_customers") \
    .getOrCreate()

# Tables information
staging_bookings_delta = "hive_metastore.default.staging_bookings_delta"
staging_customers_delta = "hive_metastore.default.staging_customers_delta"

# read the staged data
bookings_df = spark.read.format("delta").table(staging_bookings_delta)
customers_df = spark.read.format("delta").table(staging_customers_delta)

# printSchema and sample data
bookings_df.printSchema()
bookings_df.show(5, truncate=False)

customers_df.printSchema()
customers_df.show(5, truncate=False)

"""
Applying Quality Checks after Transformations
"""
bookings_check = Check(spark, CheckLevel.Error, "Quality Check on Bookings dataframe") \
    .hasSize(lambda x: x > 0) \
    .isComplete("booking_id") \
    .isComplete("customer_id") \
    .isComplete("car_id") \
    .isComplete("booking_date") \
    .isComplete("status") \
    .isNonNegative("total_amount") \
    .isComplete("start_date") \
    .isComplete("starting_time") \
    .isComplete("end_date") \
    .isComplete("ending_time") \
    .isNonNegative("total_duration")

customers_check = Check(spark, CheckLevel.Error, "Quality Check on Customers dataframe") \
    .hasSize(lambda x: x > 0) \
    .isComplete("customer_id") \
    .isComplete("name") \
    .isComplete("email") \
    .isComplete("signup_date") \
    .isComplete("phone_number") \
    .isComplete("status") \
    .isNonNegative("tenure_in_days")

# Run the verification suite
bookings_verification = VerificationSuite(spark) \
    .onData(bookings_df) \
    .addCheck(bookings_check) \
    .run()
customers_verification = VerificationSuite(spark) \
    .onData(customers_df) \
    .addCheck(customers_check) \
    .run()

# Display the verification results
bookings_verification.checkResultsAsDataFrame().show(truncate=False)
customers_verification.checkResultsAsDataFrame().show(truncate=False)

# Check if verification passed
if bookings_verification.status != "Success":
    raise ValueError("Data Quality Checks Failed for Booking Data")

if customers_verification.status != "Success":
    raise ValueError("Data Quality Checks Failed for Customer Data")

# Tables information
bookings_delta_table = "hive_metastore.default.bookings_delta"
customers_delta_table = "hive_metastore.default.customers_delta"

bookings_table_exists = spark._jsparkSession.catalog().tableExists(bookings_delta_table)
customers_table_exists = spark._jsparkSession.catalog().tableExists(customers_delta_table)

"""
■ Update: If booking_id exists in the target table,
update the existing records.
■ Insert: If booking_id does not exist, insert new
records.
■ Delete: If the status of a booking is cancelled, delete the record
from the target table
"""
if bookings_table_exists:
    # Load the existing table
    bookings_table = DeltaTable.forName(spark, bookings_delta_table)
    mergeCondition = "source.booking_id = update.booking_id"
    bookings_table.alias("source") \
        .merge(source=bookings_df.alias("update"),
               condition=mergeCondition) \
        .whenMatchedUpdate(
        condition="source.status <> 'cancelled'",
        set={
            "customer_id": "update.customer_id",
            "car_id": "update.car_id",
            "booking_date": "update.booking_date",
            "status": "update.status",
            "total_amount": "update.total_amount",
            "start_date": "update.start_date",
            "starting_time": "update.starting_time",
            "end_date": "update.end_date",
            "ending_time": "update.ending_time",
            "total_duration": "update.total_duration"
        }) \
        .whenNotMatchedInsert(
        values={
            "booking_id": "update.booking_id",
            "customer_id": "update.customer_id",
            "car_id": "update.car_id",
            "booking_date": "update.booking_date",
            "status": "update.status",
            "total_amount": "update.total_amount",
            "start_date": "update.start_date",
            "starting_time": "update.starting_time",
            "end_date": "update.end_date",
            "ending_time": "update.ending_time",
            "total_duration": "update.total_duration"
        }) \
        .whenMatchedDelete(
        # Delete if the booking status is cancelled
        condition="source.status = 'cancelled'") \
        .execute()
else:
    bookings_df.write.format("delta").mode("overwrite").saveAsTable(bookings_delta_table)

"""
■ Update: If customer_id exists in the target table,
update the existing records.
■ Insert: If customer_id does not exist, insert new
records.
■ Delete: If the status of a customer is inactive, delete the record
from the target table
"""
if customers_table_exists:
    # Load the existing table
    customers_table = DeltaTable.forName(spark, customers_delta_table)
    mergeCondition = "source.customer_id = update.customer_id"
    customers_table.alias("source") \
        .merge(source=customers_df.alias("update"),
               condition=mergeCondition) \
        .whenMatchedUpdate(
        condition="source.status <> 'inactive'",
        set={
            "name": "update.name",
            "email": "update.email",
            "signup_date": "update.signup_date",
            "phone_number": "update.phone_number",
            "status": "update.status",
            "tenure_in_days": "update.tenure_in_days"
        }) \
        .whenNotMatchedInsert(
        values={
            "customer_id": "update.customer_id",
            "name": "update.name",
            "email": "update.email",
            "signup_date": "update.signup_date",
            "phone_number": "update.phone_number",
            "status": "update.status",
            "tenure_in_days": "update.tenure_in_days"
        }) \
        .whenMatchedDelete(
        # Delete if the booking status is cancelled
        condition="source.status = 'inactive'") \
        .execute()
else:
    customers_df.write.format("delta").mode("overwrite").saveAsTable(customers_delta_table)

customer_count = spark.sql(f"SELECT COUNT(*) FROM {customers_delta_table}").collect()[0][0]
booking_count = spark.sql(f"SELECT COUNT(*) FROM {bookings_delta_table}").collect()[0][0]

print(f"Customer count: {customer_count}")
print(f"Booking count: {booking_count}")
