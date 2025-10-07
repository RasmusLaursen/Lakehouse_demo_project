from pydantic import BaseModel

class TableConfig(BaseModel):
    keys: list
    sequence_column: str
    stored_as_scd_type: int
