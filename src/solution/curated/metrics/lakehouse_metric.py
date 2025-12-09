from src.framework.helper import databricks_helper

spark = databricks_helper.get_spark()


def create_lakehouse_metric_view():
    catalogs = databricks_helper.get_pipeline_configurations(spark, "catalogs")
    schemas = databricks_helper.get_pipeline_configurations(spark, "schemas")

    logger.debug("Catalogs configuration: " + str(catalogs))
    logger.debug("Schemas configuration: " + str(schemas))

    curated_catalog = catalogs.get("curated_catalog")
    curated_metrics_schema = schemas.get("curated_metrics_schema")
    dimensions_schema = schemas.get("curated_dimensions_schema")
    facts_schema = schemas.get("curated_facts_schema")

    spark.sql(
        f"""
          CREATE OR REPLACE VIEW {curated_catalog}.{curated_metrics_schema}.mv_lakehouse_rentals
            WITH METRICS
            LANGUAGE YAML
            COMMENT 'A Metric View.'
            AS $$
            version: 0.1

            source: {curated_catalog}.{facts_schema}.fact_lakehouse_rentals

            joins:
              - name: seller
                source: {curated_catalog}.{dimensions_schema}.dim_seller
                on: source.seller_id = seller.seller_id
              - name: customer
                source: {curated_catalog}.{dimensions_schema}.dim_customer
                on: source.customer_id = customer.customer_id
              - name: calendar
                source: {curated_catalog}.{dimensions_schema}.dim_calendar
                on: source.calendar_order_id = calendar.calendar_id
              - name: lakehouse
                source: {curated_catalog}.{dimensions_schema}.dim_lakehouse
                on: source.lakehouse_id = lakehouse.lakehouse_id

            dimensions:
              # Seller Details
              - name: Seller ID
                expr: seller_id
              - name: Seller Name
                expr: seller.name
              - name: Seller Phone Number
                expr: seller.phone_number
              - name: Seller Region
                expr: seller.region_name

              # Customer Details
              - name: Customer ID
                expr: customer.customer_id
              - name: Customer Name
                expr: customer.name
              - name: Customer Email
                expr: customer.email
              - name: Customer Postcode
                expr: customer.postal_code
              - name: Customer City
                expr: customer.city
              - name: Customer Country
                expr: customer.country

              
              # Calendar Details
              - name: Calendar Order ID
                expr: calendar.calendar_id
              - name: Order Date
                expr: calendar.date
              - name: Order Year
                expr: calendar.year
              - name: Order Month
                expr: calendar.month
              - name: Order Day
                expr: calendar.day
              - name: Order Day Of Week
                expr: calendar.day_of_week
              - name: Order Week Of Year
                expr: calendar.week_of_year

              # Lakehouse Details
              - name: Lakehouse ID
                expr: lakehouse.lakehouse_id
              - name: Lakehouse Name
                expr: lakehouse.name
              - name: Lakehouse Location
                expr: lakehouse.location
              - name: Lakehouse Is Pet Friendly
                expr: lakehouse.is_pet_friendly
              - name: Lakehouse Has Lake View
                expr: lakehouse.has_lake_view
              - name: Lakehouse Has Hot tub
                expr: lakehouse.has_hot_tub

            measures:
              - name: Total Cost
                expr: SUM(total_cost)
              - name: Average Rating
                expr: AVG(rating)
              - name: Total Reviews
                expr: COUNT(calendar_order_id)
            $$;
            """
    )
