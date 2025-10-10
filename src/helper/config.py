from pydantic import BaseModel
from dataclasses import dataclass, field


class TableConfig(BaseModel):
    keys: list
    sequence_column: str
    stored_as_scd_type: int
    backfill: str = None


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