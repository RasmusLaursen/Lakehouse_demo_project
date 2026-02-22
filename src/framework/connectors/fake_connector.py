from pyspark import pipelines as dp
from pyspark.sql.functions import col

from pyspark.sql import Row, SparkSession, DataFrame

from pyspark.sql.datasource import DataSource, DataSourceStreamReader, SimpleDataSourceStreamReader, DataSourceStreamWriter
from pyspark.sql.types import StructType
from pyspark.sql.datasource import DataSourceStreamReader, InputPartition
from typing import Iterator, Tuple
import time
import os
os.environ["DATABRICKS_ENABLE_EXPERIMENTAL_FILES_API_CLIENT"] = "True" # set to true to enable new file features in sdk 
from databricks import sdk

class FakeStreamDataSource(DataSource):
    """
    An example data source for streaming read and write using the `faker` library.
    """

    @classmethod
    def name(cls):
        return "fakestream"

    def schema(self):
        return "name string, state string, t string"

    def streamReader(self, schema: StructType):
        return FakeStreamReader(schema, self.options)

    def read_batch(self, spark: "SparkSession") -> "DataFrame":
        """
        Read data as a batch DataFrame.
        
        Args:
            spark: Active SparkSession
            """
        df = self.read_stream(spark=spark)
        return df
    
    def read_stream(self, spark: "SparkSession") -> "DataFrame":
        """
        Read data as a streaming DataFrame.
        
        Args:
            spark: Active SparkSession
            
        Returns:
            Streaming DataFrame from the Eloverblik API
        """
        # Register this DataSource with Spark
        try:
            spark.dataSource.register(self.__class__)
            print(f"Registered DataSource for streaming: {self.name()}")
        except Exception as e:
            print(f"DataSource may already be registered: {e}")
        
        # Use Spark's readStream API
        df = spark.readStream.format(self.name()).load()
        print(f"Created streaming DataFrame for {self.name()}")
        
        return df        

    # If you don't need partitioning, you can implement the simpleStreamReader method instead of streamReader.
    # def simpleStreamReader(self, schema: StructType):
    #    return SimpleStreamReader()

    # def streamWriter(self, schema: StructType, overwrite: bool):
    #     return FakeStreamWriter(self.options)
    
from pyspark.sql.datasource import InputPartition
from typing import Iterator, Tuple
import os
import json

class RangePartition(InputPartition):
    def __init__(self, start, end):
        self.start = start
        self.end = end

class FakeStreamReader(DataSourceStreamReader):
    def __init__(self, schema, options):
        self.schema = schema
        self.options = options
        # No stateful variables needed - Spark handles offset tracking!
        
    def initialOffset(self) -> dict:
        """
        Returns the initial start offset if no checkpoint exists.
        Spark only calls this ONCE when checkpoint directory is empty.
        """
        return {"offset": (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() - 30*24*60*60)))}
    
    def latestOffset(self) -> dict:
        """
        Returns the current latest offset based on EXTERNAL source state.
        Spark calls this every microbatch to check for new data.
        
        Important: Calculate based on external state, not instance variables!
        """
        # Option 1: Time-based offset (for demo)
        latest =  (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() )))
        
        # Option 2: Query external system
        # latest = self._get_max_id_from_api()
        
        # Option 3: Read from file/database
        # latest = self._get_latest_record_count()
        
        return {"offset": latest}

    def _save_logs_to_volume2(self) -> None:
        """Save executor logs to Databricks volume using Files API."""      
        import json
        import io
        
        # Construct full file path
        file_path = f"/Volumes/raw_dev/dev_rasmuslaursen_eloverblik/eloverblik_log/logs/test_dabs.json"
        
        # Prepare log payload
        log_payload = {"test": "data", "timestamp": "2025-02-17"}

        f = io.BytesIO(json.dumps(log_payload).encode('utf-8'))
        
        # Create workspace client lazily to avoid module-import serialization issues
        workspace_client = sdk.WorkspaceClient(
            host = 'https://dbc-e3d11267-f1e6.cloud.databricks.com/',
            token = '<redacted-databricks-token>'
        )
        workspace_client.files.upload(file_path=file_path, contents=f, overwrite=True)     
    
    def partitions(self, start: dict, end: dict):
        """
        Spark passes the correct start offset from checkpoint.
        - First run: start = initialOffset(), end = latestOffset()
        - Later runs: start = last committed offset, end = latestOffset()
        """
        return [RangePartition(start["offset"], end["offset"])]
    
    def commit(self, end: dict):
        """
        Spark automatically persists this offset to checkpoint directory.
        You can optionally clean up resources here.
        """
        # Optional: Log or cleanup
        self._save_logs_to_volume2()
        print(f"Committed offset: {end['offset']}")
        pass
    
    def read(self, partition) -> Iterator[Tuple]:
        """
        Read data for the given partition range.
        """
        start, end = partition.start, partition.end
        from datetime import datetime, timedelta

        start_dt = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
        current_dt = start_dt

        while current_dt < end_dt:
            yield (
                f"name_{current_dt.strftime('%Y%m%d')}",
                f"state_{current_dt.strftime('%Y%m%d')}",
                current_dt.strftime("%Y-%m-%d %H:%M:%S")
            )
            current_dt += timedelta(days=1)


# spark.dataSource.register(FakeStreamDataSource)

# dp.create_streaming_table("raw_dev.dev_rasmuslaursen_eloverblik.fake_stream_dabs") # Required only if the target table doesn't exist.

# @dp.append_flow(
#   target = "raw_dev.dev_rasmuslaursen_eloverblik.fake_stream_dabs"
#   )
# def _load_fake_stream():
#     return spark.readStream.format("fakestream").load()

