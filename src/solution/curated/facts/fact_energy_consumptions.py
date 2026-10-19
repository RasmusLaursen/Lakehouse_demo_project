"""Curated fact table for lakehouse rentals.

This module uses the factory pattern to create DLT fact tables.
All logic has been consolidated into the CuratedFactFactory.
"""
from pyspark.sql import DataFrame
from src.framework.factory.fact_factory import FactFactory
from src.framework.helper import get_spark, get_pipeline_configurations
from pyspark.sql.functions import col, lpad, lit, concat

# Create the fact table for lakehouse rentals
spark = get_spark()
factory = FactFactory(spark)

catalogs = get_pipeline_configurations(spark, "catalogs")
schemas = get_pipeline_configurations(spark, "schemas")
timeseries_table = f"{catalogs['base_catalog']}.{schemas['eloverblik_base_schema']}.timeseriesdata"
prices_table = f"{catalogs['base_catalog']}.{schemas['energidataservice_base_schema']}.dayaheadprices"


def _tranform_fact_energy_consumptions() -> DataFrame:
    df = spark.readStream.table(timeseries_table)

    df = df.join(
        spark.read.table(prices_table)
        .select("TimeDK", "PriceArea", "DayAheadPriceDKK"),
        (col("TimeDK").cast("timestamp") == col("period_start").cast("timestamp")) & (col("PriceArea") == "DK2"),
        "left"
    )

    df = df.withColumn("kwh_cost", (col("quantity").cast("float") * col("DayAheadPriceDKK").cast("float")) / 1000)

    df = df.withColumnsRenamed({
        'period_end': 'calendar_key',
        'meteringPointId': 'meteringpoint_key'
    })

    df = df.withColumn("time_key", lpad(col("position").cast("int"), 2, "0").cast("string").substr(1, 2))
    df = df.withColumn("time_key", concat(col("time_key"), lit("00")))


    df = df.select(
        "calendar_key",
        "meteringpoint_key",
        "time_key",
        "quantity",
        "kwh_cost"
    )
    return df

factory.create_fact(
    fact_name='fact_energy_consumptions',
    transform_fn=_tranform_fact_energy_consumptions,
)