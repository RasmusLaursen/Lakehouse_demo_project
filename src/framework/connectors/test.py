from pyspark.sql.datasource import DataSource, DataSourceReader, SimpleDataSourceStreamReader
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType
from pyspark.sql.datasource import InputPartition
from typing import Dict, Iterator, Tuple
import os
import json
import requests
import logging 


access_token = dbutils.secrets.get(scope="scope-demo-dev", key="eloverblik-api-token")

class EloverblikDataSource(DataSource):
    """
    An example data source for batch query using the `faker` library.
    """

    def __init__(self, options: Dict[str, str]) -> None:
        super().__init__(options)

    @classmethod
    def name(cls):
        return "eloverblik"

    def schema(self):
        # Try to get schema from options (as JSON string)
        schema_json = self.options.get("schema")
        if schema_json:
            try:
                # Parse JSON string to StructType
                schema_dict = json.loads(schema_json) if isinstance(schema_json, str) else schema_json
                return StructType.fromJson(schema_dict)
            except Exception as e:
                # Fall back to default schema if parsing fails
                pass
        
        # Default schema for metering points endpoint
        return StructType([
            StructField("streetCode", StringType(), True),
            StructField("streetName", StringType(), True),
            StructField("buildingNumber", StringType(), True),
            StructField("floorId", StringType(), True),
            StructField("roomId", StringType(), True),
            StructField("citySubDivisionName", StringType(), True),
            StructField("municipalityCode", StringType(), True),
            StructField("locationDescription", StringType(), True),
            StructField("settlementMethod", StringType(), True),
            StructField("meterReadingOccurrence", StringType(), True),
            StructField("firstConsumerPartyName", StringType(), True),
            StructField("secondConsumerPartyName", StringType(), True),
            StructField("meterNumber", StringType(), True),
            StructField("consumerStartDate", StringType(), True),
            StructField("meteringPointId", StringType(), True),
            StructField("typeOfMP", StringType(), True),
            StructField("balanceSupplierName", StringType(), True),
            StructField("postcode", StringType(), True),
            StructField("cityName", StringType(), True),
            StructField("hasRelation", BooleanType(), True),
            StructField("consumerCVR", StringType(), True),
            StructField("dataAccessCVR", StringType(), True),
            StructField("childMeteringPoints", ArrayType(StringType()), True)
        ])
    
    def reader(self, schema: StructType):
        return EloverblikDataSourceReader(schema, self.options)

    def simpleStreamReader(self, schema: StructType):
        return EloverblikTimeSeriesStreamReader(schema, self.options)    

class EloverblikDataSourceReader(DataSourceReader):

    def __init__(self, schema, options):
        self.schema: StructType = schema
        self.options = options
        # Get URLs from options with defaults
        self.token_url = options.get("token_url")
        self.data_url = options.get("data_url")
        self.token = self._get_token(access_token = options.get("token"))
        self.logger = get_logger(__name__)

        self.logger.info("runining")


    def _build_header(self, access_token:str):
        return {
            "Authorization": f"Bearer {access_token}"
        }
        
    def _get_token(self, access_token:str ):
        headers = self._build_header(access_token)
        token = requests.get(url=self.token_url, headers=headers)
        return token.json()["result"]
    
    def _get_data(self, token:str):
        headers = self._build_header(token)
        self.logger.info(f"url: {self.data_url}")
        data = requests.get(url=self.data_url, headers=headers)
        return data.json()["result"]

    def read(self, partition):
        response = self._get_data(token = self.token) 
        for _ in response:
            yield tuple(_.values())

class EloverblikTimeSeriesStreamReader(SimpleDataSourceStreamReader):
    """Streaming reader for Eloverblik time series data with date-based incremental loading."""
    
    def __init__(self, schema, options):
        self.schema = schema
        self.options = options
        self.access_token = options.get("token")
        self.token_url = options.get("token_url")
        self.logger = get_logger(__name__)

        print("running stream")
        
        # URL template with path parameters
        self.data_url_template = options.get("data_url", 
            "https://api.eloverblik.dk/customerapi/api/meterdata/gettimeseries/{dateFrom}/{dateTo}/{aggregation}")
        
        # Parameters for API call
        self.metering_point_id = options.get("metering_point_id")  # Required for POST body
        self.aggregation = options.get("aggregation", "Actual")  # Hour, Day, Month, Year
        self.start_date = options.get("start_date", "2024-01-01")  # Initial date
        self.days_per_batch = int(options.get("days_per_batch", "7"))  # Days to fetch per batch
        
        # Cache for deterministic replay
        self._offset_cache = {}
    
    def _build_header(self, access_token: str):
        return {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
    
    def _get_token(self, access_token: str):
        """Exchange refresh token for access token."""
        headers = self._build_header(access_token)
        response = requests.get(url=self.token_url, headers=headers)
        response.raise_for_status()
        return response.json()["result"]
    
    def _get_timeseries_data(self, token: str, date_from: str, date_to: str):
        """Fetch time series data for a date range."""
        # Build URL with path parameters
        url = self.data_url_template.format(
            dateFrom=date_from,
            dateTo=date_to,
            aggregation=self.aggregation
        )
        
        headers = self._build_header(token)

        if self.metering_point_id is None:
            raise ValueError("Metering point ID is required.")
        
        # POST body with metering point ID
        body = {
            "meteringPoints": {
                "meteringPoint": [self.metering_point_id]
            }
        }

        body = {'meteringPoints': {'meteringPoint': ['571313113160133023']}}

        print("URL:", url)
        print("Headers:", headers)
        print("Body:", body)        

        
        response = requests.post(url=url, headers=headers, json=body)
        response.raise_for_status()
        
        data = response.json()
        
        # Extract records from nested structure
        records = self._extract_records(data, date_from, date_to)
        return records
    
    def _extract_records(self, data: dict, date_from: str, date_to: str):
        """Extract time series points from nested API response."""
        records = []
        
        try:
            # Navigate nested structure: result.MyEnergyData_MarketDocument.TimeSeries.Period.Point
            result = data.get("result", {})
            market_doc = result.get("MyEnergyData_MarketDocument", {})
            
            time_series_list = market_doc.get("TimeSeries", [])
            if not isinstance(time_series_list, list):
                time_series_list = [time_series_list] if time_series_list else []
            
            for time_series in time_series_list:
                metering_point_id = time_series.get("mRID", self.metering_point_id)
                
                periods = time_series.get("Period", [])
                if not isinstance(periods, list):
                    periods = [periods] if periods else []
                
                for period in periods:
                    period_start = period.get("timeInterval", {}).get("start")
                    period_end = period.get("timeInterval", {}).get("end")
                    resolution = period.get("resolution")
                    
                    points = period.get("Point", [])
                    if not isinstance(points, list):
                        points = [points] if points else []
                    
                    for point in points:
                        # Extract quantity and quality from nested structure
                        out_quantity = point.get("out_Quantity", {})
                        
                        record = {
                            "meteringPointId": metering_point_id,
                            "position": point.get("position"),
                            "quantity": out_quantity.get("quantity"),
                            "quality": out_quantity.get("quality"),
                            "period_start": period_start,
                            "period_end": period_end,
                            "resolution": resolution,
                            "date_from": date_from,
                            "date_to": date_to
                        }
                        records.append(record)
        
        except Exception as e:
            print(f"[EloverblikStream] Error extracting records: {e}")
        
        return records
    
    def initialOffset(self):
        """Return the initial offset (starting date)."""
        return {"date": self.start_date}
    
    def read(self, start: dict) -> (Iterator[Tuple], dict):
        """Fetch data for the next date range."""
        from datetime import datetime, timedelta
        
        # Handle first call
        if start is None or "date" not in start:
            start = self.initialOffset()
        
        current_date = start["date"]
        print(f"[EloverblikStream] Reading from date={current_date}")
        
        # Calculate date range for this batch
        date_from_obj = datetime.strptime(current_date, "%Y-%m-%d")
        date_to_obj = date_from_obj + timedelta(days=self.days_per_batch)
        
        # Get current date (today) - strip time for date-only comparison
        today = datetime.now().date()
        date_from_date = date_from_obj.date()
        date_to_date = date_to_obj.date()
        
        # Check if date_from is already at or beyond current date
        if date_from_date >= today:
            print(f"[EloverblikStream] Reached current date {today}, no new data available")
            return (iter([]), start)
        
        # Cap date_to to current date if it would go beyond
        if date_to_date > today:
            date_to_obj = datetime.combine(today, datetime.min.time())
            print(f"[EloverblikStream] Capping end date to current date: {today}")
        
        date_from = date_from_obj.strftime("%Y-%m-%d")
        date_to = date_to_obj.strftime("%Y-%m-%d")
        
        # Get token and fetch data
        token = self._get_token(self.access_token)
        records = self._get_timeseries_data(token, date_from, date_to)
        
        print(f"[EloverblikStream] Fetched {len(records)} records for {date_from} to {date_to}")
        
        # Convert records to tuples
        tuples = [tuple(r.values()) for r in records]
        
        # Cache for replay
        self._offset_cache[current_date] = tuples
        
        # Next offset is the end date of this batch
        next_offset = {"date": date_to}
        
        return (iter(tuples), next_offset)
    
    def readBetweenOffsets(self, start: dict, end: dict) -> Iterator[Tuple]:
        """Deterministic replay of cached data."""
        start_date = start.get("date")
        
        if start_date in self._offset_cache:
            return iter(self._offset_cache[start_date])
        
        # If not cached, re-fetch (less ideal)
        print(f"[EloverblikStream] Cache miss for {start_date}, re-fetching")
        _, _ = self.read(start)
        return iter(self._offset_cache.get(start_date, []))
    
    def commit(self, end):
        """Clean up old cached data."""
        if end is None or "date" not in end:
            return
        
        # Keep only recent batches
        batches_to_keep = 5
        all_dates = sorted(self._offset_cache.keys())
        
        if len(all_dates) > batches_to_keep:
            dates_to_remove = all_dates[:-batches_to_keep]
            for date in dates_to_remove:
                del self._offset_cache[date]
                print(f"[EloverblikStream] Cleaned up cache for {date}")

# Example schema definition
custom_schema = StructType([
    StructField("streetCode", StringType(), True),
    StructField("streetName", StringType(), True),
    StructField("buildingNumber", StringType(), True),
    StructField("floorId", StringType(), True),
    StructField("roomId", StringType(), True),
    StructField("citySubDivisionName", StringType(), True),
    StructField("municipalityCode", StringType(), True),
    StructField("locationDescription", StringType(), True),
    StructField("settlementMethod", StringType(), True),
    StructField("meterReadingOccurrence", StringType(), True),
    StructField("firstConsumerPartyName", StringType(), True),
    StructField("secondConsumerPartyName", StringType(), True),
    StructField("meterNumber", StringType(), True),
    StructField("consumerStartDate", StringType(), True),
    StructField("meteringPointId", StringType(), True),
    StructField("typeOfMP", StringType(), True),
    StructField("balanceSupplierName", StringType(), True),
    StructField("postcode", StringType(), True),
    StructField("cityName", StringType(), True),
    StructField("hasRelation", BooleanType(), True),
    StructField("consumerCVR", StringType(), True),
    StructField("dataAccessCVR", StringType(), True),
    StructField("childMeteringPoints", ArrayType(StringType()), True)
])

spark.dataSource.register(EloverblikDataSource)

# dict_options = {
#     "token": access_token, 
#     "token_url": "https://api.eloverblik.dk/customerapi/api/token", 
#     "data_url": "https://api.eloverblik.dk/customerapi/api/meteringpoints/meteringpoints", 
#     "schema": json.dumps(custom_schema.jsonValue())
# }

# # Usage with custom schema (for different endpoints)
# # custom_schema = StructType([...])  # Define your custom schema
# spark.read.format("eloverblik")\
#     .options(**dict_options)\
#     .load()\
#     .display()
    

# Example schema definition
custom_schema_timeseries = StructType([
    StructField("meteringPointId", StringType(), True),
    StructField("position", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("quality", StringType(), True),
    StructField("period_start", StringType(), True),
    StructField("period_end", StringType(), True),
    StructField("resolution", StringType(), True),
    StructField("date_from", StringType(), True),
    StructField("date_to", StringType(), True)
])


dict_options_timeseries = {
    "token": access_token, 
    "token_url": "https://api.eloverblik.dk/customerapi/api/token", 
    "data_url": "https://api.eloverblik.dk/customerapi/api/meterdata/gettimeseries/{dateFrom}/{dateTo}/{aggregation}", 
    "schema": json.dumps(custom_schema_timeseries.jsonValue()),
    "metering_point_id":"571313113160133023",
    "start_date": "2026-01-01"
}    




# spark.read.format("eloverblik").option("token",access_token).load().display()
df = spark.readStream.format("eloverblik")\
    .options(**dict_options_timeseries)\
    .load()\
    .writeStream\
    .option("checkpointLocation", "/Volumes/landing_dev/dev_rasmuslaursen_lakehouse/timeseries_checkpoint")\
    .option("path", "/Volumes/landing_dev/dev_rasmuslaursen_lakehouse/timeseries_output")\
    .trigger(once=True)\
    .start()



