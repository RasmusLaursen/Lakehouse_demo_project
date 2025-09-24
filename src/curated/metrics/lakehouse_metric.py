from src.helper.databricks_helper import get_spark
from src.helper import commen

spark = get_spark()

def create_lakehouse_metric_view():
  curated_catalog = commen.parse_arguments("curated_catalog")
  curated_metrics_schema = commen.parse_arguments("curated_metrics_schema")  
  dimensions_schema = commen.parse_arguments("curated_dimensions_schema")
  facts_schema = commen.parse_arguments("curated_facts_schema")

  spark.sql(f"""
          CREATE OR REPLACE VIEW {curated_catalog}.{curated_metrics_schema}.mv_lakehouse_rentals
            (
              Seller_ID,
              Customer_ID,
              Calendar_Order_ID,
              Lakehouse_ID,
              Seller_Name,
              Seller_Phone_Number,
              Seller_Region,
              Customer_Name,
              Customer_Email,
              Customer_Postcode,
              Customer_City,
              Customer_Country,
              Order_Date,
              Order_Year,
              Order_Month,
              Order_Day,
              Order_Day_Of_Week,
              Order_Week_Of_Year,
              Lakehouse_Name,
              Lakehouse_Location,
              Lakehouse_Is_Pet_Friendly,
              Lakehouse_Has_Lake_View,
              Lakehouse_Has_Hot_tub,
              Total_Cost,
              Average_Rating,
              Total_Reviews
            )
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
                expr: seller.region

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