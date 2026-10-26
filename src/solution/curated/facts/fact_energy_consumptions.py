"""Curated fact table for lakehouse rentals.

This module uses the factory pattern to create DLT fact tables.
All logic has been consolidated into the CuratedFactFactory.
"""

from pyspark.sql import DataFrame
from src.framework.factory.fact_factory import FactFactory
from src.framework.helper import get_spark, get_pipeline_configurations
from pyspark.sql.functions import (
    col,
    avg,
    date_trunc,
    date_format,
    expr,
    from_utc_timestamp,
    to_date,
)

# Create the fact table for lakehouse rentals
spark = get_spark()
factory = FactFactory(spark)

catalogs = get_pipeline_configurations(spark, "catalogs")
schemas = get_pipeline_configurations(spark, "schemas")
timeseries_table = (
    f"{catalogs['base_catalog']}.{schemas['eloverblik_base_schema']}.timeseriesdata"
)
prices_table = f"{catalogs['base_catalog']}.{schemas['energidataservice_base_schema']}.dayaheadprices"


def _tranform_fact_energy_consumptions() -> DataFrame:
    # Batch read (materialized view): source is an AUTO CDC target and prices may arrive later
    df = spark.read.table(timeseries_table)

    # position is the 1-based hour within the period; period_start is UTC
    df = df.withColumn(
        "hour_utc",
        col("period_start").cast("timestamp")
        + expr("make_interval(0, 0, 0, 0, cast(position as int) - 1, 0, 0)"),
    )
    df = df.withColumn(
        "hour_dk", from_utc_timestamp(col("hour_utc"), "Europe/Copenhagen")
    )

    # Day-ahead prices are quarter-hourly (DKK/MWh); average them to hourly
    hourly_prices = (
        spark.read.table(prices_table)
        .where(col("PriceArea") == "DK2")
        .groupBy(
            date_trunc("hour", col("TimeUTC").cast("timestamp")).alias("price_hour_utc")
        )
        .agg(avg(col("DayAheadPriceDKK").cast("double")).alias("price_dkk_per_mwh"))
    )
    df = df.join(hourly_prices, col("hour_utc") == col("price_hour_utc"), "left")

    df = df.withColumn("quantity", col("quantity").cast("double"))
    df = df.withColumn("kwh_cost", col("quantity") * col("price_dkk_per_mwh") / 1000)

    df = df.withColumn("calendar_key", to_date(col("hour_dk")))
    df = df.withColumn("time_key", date_format(col("hour_dk"), "HHmm").cast("int"))
    df = df.withColumnRenamed("meteringPointId", "meteringpoint_key")

    df = df.select(
        "calendar_key", "meteringpoint_key", "time_key", "quantity", "kwh_cost"
    )
    return df


factory.create_fact(
    fact_name="fact_energy_consumptions",
    transform_fn=_tranform_fact_energy_consumptions,
)
