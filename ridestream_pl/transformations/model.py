from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

# Define the source stream of changes
@dp.view
def dim_passenger_view():
    df=spark.readStream.table("silver_obt")
    df=df.dropDuplicates(subset=['passenger_id'])
    df=df.select("passenger_id","passenger_name","passenger_email","passenger_phone")
    return df

# Create the target streaming table
dp.create_streaming_table(
    name="dim_passenger"
)

# Apply changes from the source to the target using Auto CDC
dp.create_auto_cdc_flow(
    target="dim_passenger",
    source="dim_passenger_view",
    keys=["passenger_id"],
    sequence_by=col("passenger_id"),
    stored_as_scd_type=1
)

#driver_dimension

@dp.view
def dim_driver_view():
    df=spark.readStream.table("silver_obt")
    df=df.dropDuplicates(subset=['driver_id'])
    df=df.select("driver_id","driver_name","driver_rating","driver_phone","driver_license")
    return df

# Create the target streaming table
dp.create_streaming_table(
    name="dim_driver"
)

# Apply changes from the source to the target using Auto CDC
dp.create_auto_cdc_flow(
    target="dim_driver",
    source="dim_driver_view",
    keys=["driver_id"],
    sequence_by=col("driver_id"),
    stored_as_scd_type=1
)


#vehicle_dimension

@dp.view
def dim_vehicle_view():
    df = spark.readStream.table("silver_obt")
    df = df.dropDuplicates(subset=['vehicle_id'])
    df = df.select(
        "vehicle_id",
        "vehicle_type_id",
        "vehicle_make_id",
        "vehicle_model",
        "vehicle_color",
        "license_plate",
        "vehicle_make",
        "vehicle_type",
        "description"
    )
    return df

# Create the target streaming table
dp.create_streaming_table(
    name="dim_vehicle"
)

# Apply changes from the source to the target using Auto CDC
dp.create_auto_cdc_flow(
    target="dim_vehicle",
    source="dim_vehicle_view",
    keys=["vehicle_id"],
    sequence_by=col("vehicle_id"),
    stored_as_scd_type=1
)


#payment_dimension

@dp.view
def dim_payment_view():
    df = spark.readStream.table("silver_obt")
    df = df.dropDuplicates(subset=['payment_method_id'])
    df = df.select(
        "payment_method_id",
        "payment_method",
        "is_card",
        "requires_auth"
    )

    return df

# Create the target streaming table
dp.create_streaming_table(
    name="dim_payment"
)

# Apply changes from the source to the target using Auto CDC
dp.create_auto_cdc_flow(
    target="dim_payment",
    source="dim_payment_view",
    keys=["payment_method_id"],
    sequence_by=col("payment_method_id"),
    stored_as_scd_type=1
)



@dp.view
def dim_booking_view():
    df = spark.readStream.table("ridestream_cata.bronze.silver_obt")
    df = df.select("ride_id","confirmation_number","dropoff_location_id","ride_status_id","dropoff_city_id","cancellation_reason_id","dropoff_address","dropoff_latitude","dropoff_longitude","booking_timestamp","dropoff_timestamp","pickup_address","pickup_latitude","pickup_longitude","pickup_location_id")
    df = df.dropDuplicates(subset=['ride_id'])
    return df

dp.create_streaming_table("dim_booking")
dp.create_auto_cdc_flow(
  target = "dim_booking",
  source = "dim_booking_view",
  keys = ["ride_id"],
  sequence_by = "ride_id",
  stored_as_scd_type = 1,
)




@dp.table
def dim_location_view():
    df = spark.readStream.table("ridestream_cata.bronze.silver_obt")
    df = df.select("pickup_city_id","pickup_city","updated_at","region","state")
    df = df.dropDuplicates(subset=['pickup_city_id','updated_at'])
    return df

dp.create_streaming_table("dim_location")
dp.create_auto_cdc_flow(
  target = "dim_location",
  source = "dim_location_view",
  keys = ["pickup_city_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = 2,
)


@dp.view
def fact_view():
    df = spark.readStream.table("ridestream_cata.bronze.silver_obt")
    df = spark.readStream.table("ridestream_cata.bronze.silver_obt")
    df = df.select("ride_id","pickup_city_id","payment_method_id","driver_id","passenger_id","vehicle_id","distance_miles","duration_minutes","base_fare","distance_fare","time_fare","surge_multiplier","total_fare","tip_amount","rating","base_rate","per_mile","per_minute")
    return df

dp.create_streaming_table("fact")
dp.create_auto_cdc_flow(
  target = "fact",
  source = "fact_view",
  keys = ["ride_id","pickup_city_id","payment_method_id","driver_id","passenger_id","vehicle_id"],
  sequence_by = "ride_id",
  stored_as_scd_type = 1,
)

