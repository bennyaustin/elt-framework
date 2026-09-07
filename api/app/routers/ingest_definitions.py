"""Routes for ELT.GetIngestDefinition, ELT.GetIngestDefinition_FileDrop, ELT.UpdateIngestDefinition."""
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from app.auth.aad import verify_token
from app.db.executor import execute_proc
from app.models.ingest import (
    IngestDefinitionFileDropQuery,
    IngestDefinitionFileDropRow,
    IngestDefinitionRow,
    IngestDefinitionUpdate,
)

router = APIRouter(prefix="/ingest/definitions", tags=["ingest-definitions"], dependencies=[Depends(verify_token)])

_SCHEMA = "ELT"


@router.get("", response_model=list[IngestDefinitionRow])
def get_ingest_definition(
    source_system_name: Annotated[str, Query(max_length=20)],
    stream_name: Annotated[str, Query(max_length=100)] = "%",
    max_ingest_instance: int = 10,
) -> list[IngestDefinitionRow]:
    """Calls ELT.GetIngestDefinition — returns the next batch of ingest work items."""
    rows = execute_proc(
        _SCHEMA,
        "GetIngestDefinition",
        {
            "SourceSystemName": source_system_name,
            "StreamName": stream_name,
            "MaxIngestInstance": max_ingest_instance,
        },
    )
    return [IngestDefinitionRow(**row) for row in rows]


@router.get("/file-drop", response_model=list[IngestDefinitionFileDropRow])
def get_ingest_definition_file_drop(
    params: Annotated[IngestDefinitionFileDropQuery, Query()],
) -> list[IngestDefinitionFileDropRow]:
    """Calls ELT.GetIngestDefinition_FileDrop — used by file-drop trigger pipelines."""
    rows = execute_proc(
        _SCHEMA,
        "GetIngestDefinition_FileDrop",
        {
            "SourceSystemName": params.source_system_name,
            "StreamName": params.stream_name,
            "SourceFolder": params.source_folder,
            "SourceFile": params.source_file,
        },
    )
    return [IngestDefinitionFileDropRow(**row) for row in rows]


@router.patch("/{ingest_id}", status_code=204)
def update_ingest_definition(ingest_id: int, body: IngestDefinitionUpdate) -> None:
    """Calls ELT.UpdateIngestDefinition — updates the delta watermark after an ingest run."""
    execute_proc(
        _SCHEMA,
        "UpdateIngestDefinition",
        {
            "IngestID": ingest_id,
            "LastDeltaDate": body.last_delta_date,
            "LastDeltaNumer": body.last_delta_number,
            "IngestStatus": body.ingest_status,
            "ReloadFlag": body.reload_flag,
        },
    )
