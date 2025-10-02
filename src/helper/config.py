from pydantic import BaseModel, ValidationError

class TableConfig(BaseModel):
    keys: list
    sequence_column: str
    stored_as_scd_type: int
