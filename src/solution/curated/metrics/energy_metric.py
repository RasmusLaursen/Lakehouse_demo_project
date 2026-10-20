from src.framework.helper import get_spark, get_logger, parse_arguments

logger = get_logger(__name__)

METRIC_VIEW_NAME = "mv_energy_consumption"


def build_energy_metric_view_sql(
    curated_catalog: str, metrics_schema: str, dimensions_schema: str, facts_schema: str
) -> str:
    return f"""
CREATE OR REPLACE VIEW {curated_catalog}.{metrics_schema}.{METRIC_VIEW_NAME}
WITH METRICS
LANGUAGE YAML
COMMENT 'Hourly electricity consumption (Eloverblik) priced with DK2 day-ahead spot prices (Energi Data Service).'
AS $$
version: 1.1

source: {curated_catalog}.{facts_schema}.fact_energy_consumptions

joins:
  - name: meteringpoint
    source: {curated_catalog}.{dimensions_schema}.dim_meteringpoint
    on: source.meteringpoint_id = meteringpoint.meteringpoint_id
  - name: calendar
    source: {curated_catalog}.{dimensions_schema}.dim_calendar
    on: source.calendar_id = calendar.calendar_id
  - name: time
    source: {curated_catalog}.{dimensions_schema}.dim_time
    on: source.time_id = time.time_id

dimensions:
  - name: date
    expr: calendar.date
    display_name: Date
    comment: Consumption date in Danish local time
    synonyms: [day]
  - name: year
    expr: calendar.year
    display_name: Year
  - name: quarter
    expr: calendar.quarter
    display_name: Quarter
  - name: month
    expr: DATE_TRUNC('MONTH', calendar.date)
    display_name: Month
    comment: First day of the consumption month
  - name: week_of_year
    expr: calendar.week_of_year
    display_name: Week
  - name: day_of_week
    expr: calendar.day_of_week
    display_name: Day of Week
    comment: 1 = Sunday ... 7 = Saturday
  - name: is_weekend
    expr: calendar.is_weekend
    display_name: Is Weekend
  - name: hour_of_day
    expr: CAST(time.hour_of_day AS INT)
    display_name: Hour of Day
    comment: Hour (0-23) in Danish local time
    synonyms: [hour]
  - name: is_peak_hours
    expr: time.is_peak_hours
    display_name: Is Peak Hours
    comment: True between 17:00 and 20:00
  - name: is_business_hours
    expr: time.is_business_hours
    display_name: Is Business Hours
    comment: True between 09:00 and 17:00
  - name: metering_point
    expr: meteringpoint.meteringpoint_key
    display_name: Metering Point
    comment: 18-digit metering point ID (GSRN)
    synonyms: [meter, mp, gsrn]
  - name: metering_point_type
    expr: meteringpoint.type_of_m_p
    display_name: Metering Point Type
    comment: E17 consumption, E18 production
  - name: settlement_method
    expr: meteringpoint.settlement_method
    display_name: Settlement Method
  - name: city
    expr: meteringpoint.city_name
    display_name: City
  - name: postcode
    expr: meteringpoint.postcode
    display_name: Postcode
  - name: balance_supplier
    expr: meteringpoint.balance_supplier_name
    display_name: Balance Supplier
    synonyms: [supplier, electricity company]

measures:
  - name: total_consumption_kwh
    expr: SUM(source.quantity)
    display_name: Total Consumption (kWh)
    synonyms: [consumption, usage, kwh]
  - name: total_cost_dkk
    expr: SUM(source.kwh_cost)
    display_name: Total Spot Cost (DKK)
    comment: Consumption priced at DK2 day-ahead spot price, excl. tariffs, fees and VAT
    synonyms: [cost, spend, price paid]
  - name: avg_price_dkk_per_kwh
    expr: SUM(source.kwh_cost) / NULLIF(SUM(CASE WHEN source.kwh_cost IS NOT NULL THEN source.quantity END), 0)
    display_name: Avg Spot Price (DKK/kWh)
    comment: Consumption-weighted average spot price for hours with a known price
    synonyms: [average price, unit price]
  - name: avg_hourly_consumption_kwh
    expr: AVG(source.quantity)
    display_name: Avg Hourly Consumption (kWh)
  - name: peak_consumption_kwh
    expr: SUM(CASE WHEN time.is_peak_hours THEN source.quantity END)
    display_name: Peak Consumption (kWh)
  - name: peak_share
    expr: SUM(CASE WHEN time.is_peak_hours THEN source.quantity END) / NULLIF(SUM(source.quantity), 0)
    display_name: Peak Share
    comment: Share of consumption between 17:00 and 20:00
  - name: metering_point_count
    expr: COUNT(DISTINCT source.meteringpoint_id)
    display_name: Metering Points
$$
"""


def create_energy_metric_view():
    spark = get_spark()

    curated_catalog = parse_arguments("curated_catalog")
    metrics_schema = parse_arguments("curated_metrics_schema")
    dimensions_schema = parse_arguments("curated_dimensions_schema")
    facts_schema = parse_arguments("curated_facts_schema")

    logger.info(f"Creating metric view {curated_catalog}.{metrics_schema}.{METRIC_VIEW_NAME}")
    spark.sql(build_energy_metric_view_sql(curated_catalog, metrics_schema, dimensions_schema, facts_schema))
