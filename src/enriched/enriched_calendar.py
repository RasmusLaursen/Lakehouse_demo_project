from src.helper import databricks_helper
from src.helper import lakeflow_declarative_pipeline
from src.helper import logging_helper
from pyspark.sql.functions import (
    col,
    year,
    month,
    dayofmonth,
    dayofweek,
    weekofyear,
    quarter,
    when,
)
from datetime import datetime, timedelta

# Initialize logger
logger = logging_helper.get_logger(__name__)

# Initialize Spark session
spark = databricks_helper.get_spark()

target_catalog = spark.conf.get("enriched_catalog")
target_schema = spark.conf.get("enriched_schema")

# Generate a list of dates from 1900-01-01 to 2100-12-31
start_date = datetime(1900, 1, 1)
end_date = datetime(2100, 12, 31)
delta = timedelta(days=1)

date_data = []
current_date = start_date

while current_date <= end_date:
    date_data.append({"date": current_date.strftime("%Y-%m-%d")})
    current_date += delta
# Convert the list to a DataFrame
date_df = spark.createDataFrame(date_data)

# Cast the `date` column to a DateType
date_df = date_df.withColumn("date", col("date").cast("date"))

# Add relevant date-related columns
date_df = (
    date_df.withColumn("year", year(col("date")))
    .withColumn("month", month(col("date")))
    .withColumn("day", dayofmonth(col("date")))
    .withColumn("day_of_week", dayofweek(col("date")))
    .withColumn("week_of_year", weekofyear(col("date")))
    .withColumn("quarter", quarter(col("date")))
    .withColumn(
        "is_weekend", when(col("day_of_week").isin(1, 7), True).otherwise(False)
    )
)

lakeflow_declarative_pipeline.ldp_table(
    name=f"{target_catalog}.{target_schema}.calendar",
    source_dataframe=date_df,
    comment=f"Enriched layer table for calendar",
)
