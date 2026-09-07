"""Routes for ELT.InsertIngestInstance and ELT.UpdateIngestInstance."""
from uuid import UUID

from fastapi import APIRouter, Depends, status

from app.auth.aad import verify_token
from app.db.executor import execute_proc
from app.models.ingest import IngestInstanceCreate, IngestInstanceUpdate

router = APIRouter(prefix="/ingest/instances", tags=["ingest-instances"], dependencies=[Depends(verify_token)])

_SCHEMA = "ELT"


@router.post("", status_code=status.HTTP_201_CREATED)
def create_ingest_instance(body: IngestInstanceCreate) -> None:
    """Calls ELT.InsertIngestInstance — records a new (or reloaded) ingest run start."""
    execute_proc(
        _SCHEMA,
        "InsertIngestInstance",
        {
            "IngestID": body.ingest_id,
            "SourceFileDropFileSystem": body.source_file_drop_file_system,
            "SourceFileDropFolder": body.source_file_drop_folder,
            "SourceFileDropFile": body.source_file_drop_file,
            "DestinationRawFileSystem": body.destination_raw_file_system,
            "DestinationRawFolder": body.destination_raw_folder,
            "DestinationRawFile": body.destination_raw_file,
            "DestinationRawTable": body.destination_raw_table,
            "ReloadFlag": body.reload_flag,
            "ADFPipelineRunID": body.adf_pipeline_run_id,
        },
    )


@router.patch("/{adf_ingest_pipeline_run_id}", status_code=status.HTTP_204_NO_CONTENT)
def update_ingest_instance(adf_ingest_pipeline_run_id: UUID, body: IngestInstanceUpdate) -> None:
    """Calls ELT.UpdateIngestInstance — records the outcome of an ingest run."""
    execute_proc(
        _SCHEMA,
        "UpdateIngestInstance",
        {
            "ADFIngestPipelineRunID": adf_ingest_pipeline_run_id,
            "IngestStatus": body.ingest_status,
            "DataFromTimestamp": body.data_from_timestamp,
            "DataToTimestamp": body.data_to_timestamp,
            "DataFromNumber": body.data_from_number,
            "DataToNumber": body.data_to_number,
            "SourceCount": body.source_count,
            "IngestCount": body.ingest_count,
            "ReloadFlag": body.reload_flag,
        },
    )
