from pydantic import BaseModel, field_validator, model_validator
from dataclasses import dataclass
from typing import Optional, List, Dict
from typing_extensions import Self


class TableConfig(BaseModel):
    keys: List[str]
    sequence_column: str
    stored_as_scd_type: int
    backfill: Optional[bool] = None
    track_history_column_list: Optional[List[str]] = None
    track_history_except_column_list: Optional[List[str]] = None
    column_list: Optional[List[str]] = None
    except_column_list: Optional[List[str]] = None
    apply_as_deletes: Optional[str] = None
    apply_as_truncates: Optional[str] = None
    ignore_null_updates: Optional[bool] = False
    data_quality: Optional[bool] = False

    @field_validator("stored_as_scd_type")
    def validate_scd_type(cls, v):
        if v not in [1, 2]:
            raise ValueError("stored_as_scd_type must be 1 or 2")
        return v

    @model_validator(mode="after")
    def validate_track_history_lists(self) -> Self:
        if self.stored_as_scd_type == 1 and (
            self.track_history_column_list or self.track_history_except_column_list
        ):
            raise ValueError(
                "track_history_column_list and track_history_except_column_list must be None when stored_as_scd_type is 1"
            )
        if self.track_history_column_list and self.track_history_except_column_list:
            raise ValueError(
                "Only one of track_history_column_list or track_history_except_column_list can be set"
            )
        if self.column_list and self.except_column_list:
            raise ValueError("Only one of column_list or except_column_list can be set")
        return self


class LayerConfig(BaseModel):
    source_system_name: Optional[str] = None
    load_type: Optional[str] = None
    file_type: Optional[str] = None
    objects: Dict[str, TableConfig]


@dataclass(frozen=True)
class DefaultTblProperties:
    delta_enableDeletionVectors: str = "true"
    delta_enableRowTracking: str = "true"
    delta_enableChangeDataFeed: str = "true"
    pipelines_changeDataCaptureMode: str = "TRACK_CHANGES"

    def as_dict(self):
        return {
            "delta.enableDeletionVectors": self.delta_enableDeletionVectors,
            "delta.enableRowTracking": self.delta_enableRowTracking,
            "delta.enableChangeDataFeed": self.delta_enableChangeDataFeed,
            "pipelines.changeDataCaptureMode": self.pipelines_changeDataCaptureMode,
        }


@dataclass(frozen=True)
class InternalAuditColumns:
    audit_column: str = "_metadata_ldp"
