from datetime import datetime, timezone
from fastapi import APIRouter
from typing import Dict, List
import os
import time
import io

from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.service.catalog import VolumeType
from faker import Faker

w = WorkspaceClient()

DATABRICKS_WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID") or None

databricks_cfg = Config(
    DATABRICKS_WAREHOUSE_ID = '894dc568253720e2'
)

def get_connection(warehouse_id: str):
    http_path = f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}"
    return sql.connect(
        server_hostname=databricks_cfg.host,
        http_path=http_path,
        credentials_provider=lambda: databricks_cfg.authenticate,
    )

# def query(sql_query: str, warehouse_id: str, as_dict: bool = True) -> List[Dict]:
#     conn = get_connection(warehouse_id)
#     try:
#         with conn.cursor() as cursor:
#             cursor.execute(sql_query)
#             result = cursor.fetchall()
#             columns = [col[0] for col in cursor.description]
#             return [dict(zip(columns, row)) for row in result]

#     except Exception as e:
#         raise Exception(f"DBSQL Query Failed: {str(e)}")

router = APIRouter()

def check_existing_volumes(catalog_name: str, schema_name: str, volume_name: str) -> bool:
    """
    Check if a volume with the specified name exists in the given catalog and schema.

    Args:
        catalog_name (str): The name of the catalog to search in.
        schema_name (str): The name of the schema to search in.
        volume_name (str): The name of the volume to check for existence.

    Returns:
        bool: True if the volume exists, False otherwise.
    """
    existing_volumes = w.volumes.list(catalog_name=catalog_name, schema_name=schema_name)
    for volume in existing_volumes:
        if volume.name == volume_name:
            return True
    return False

@router.get("/bookings")
async def create_volume_bookings(catalog_name:str, schema_name:str, volume_name:str) -> Dict[str, str]:
    """Return the API status."""
    if check_existing_volumes(catalog_name, schema_name, volume_name):
        return {"status": "error", "message": f"Volume {volume_name} already exists in {catalog_name}.{schema_name}"}
    else:
        try:
            created_volume = w.volumes.create(
                catalog_name=catalog_name,
                schema_name=schema_name,
                name=volume_name,
                volume_type= VolumeType.MANAGED
            )
            return {"status": "OK", "timestamp": datetime.now(timezone.utc).isoformat(), "volume_id": created_volume.volume_id}
        except Exception as e:
            return {"status": "error", "message": str(e)}
        

@router.get("/add_booking")
async def add_booking(catalog_name:str, schema_name:str, volume_name:str) -> Dict[str, str]:
    """Generate a fake lakehouse booking."""    
    fake = Faker()
    
    booking_transaction_id = str(fake.uuid4())

    booking = {
        "customer_name": fake.name(),
        "booking_id": booking_transaction_id,  # Ensure UUID is a string
        "customer_email": fake.email(),
        "booking_date": fake.date_time_this_year().isoformat(),
        "volume_name": volume_name,
        "status": "confirmed"
    }

    try:
        binary_data = io.BytesIO(str(booking).encode('utf-8'))  # Convert dict to bytes
    except Exception as e:
        return {"status": "error", "message": f"Failed to create booking data: {str(e)}"}

    try:
        volume_file_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{booking_transaction_id}.json"
        w.files.upload(volume_file_path, binary_data, overwrite=True)
    except Exception as e:
        return {"status": "error", "message": f"Failed to write booking: {str(e)}"}


    return {"status": "OK"}