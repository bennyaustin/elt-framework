"""Routes for ELT.GetTransformDefinition_L1/L2 and ELT.UpdateTransformDefinition_L2."""
from typing import Annotated, Optional
from datetime import datetime

from fastapi import APIRouter, Depends, Query

from app.auth.aad import verify_token
from app.db.executor import execute_proc
from app.models.transform_l1 import TransformDefinitionL1Row
from app.models.transform_l2 import TransformDefinitionL2Row, TransformDefinitionL2Update

router = APIRouter(prefix="/transform/definitions", tags=["transform-definitions"], dependencies=[Depends(verify_token)])

_SCHEMA = "ELT"


@router.get("/l1", response_model=list[TransformDefinitionL1Row])
def get_transform_definition_l1(
    ingest_id: int,
    delta_date: Optional[datetime] = None,
) -> list[TransformDefinitionL1Row]:
    """Calls ELT.GetTransformDefinition_L1."""
    rows = execute_proc(
        _SCHEMA,
        "GetTransformDefinition_L1",
        {"IngestID": ingest_id, "DeltaDate": delta_date},
    )
    return [TransformDefinitionL1Row(**row) for row in rows]


@router.get("/l2", response_model=list[TransformDefinitionL2Row])
def get_transform_definition_l2(
    ingest_id: int,
    delta_date: Optional[datetime] = None,
    input_type: Annotated[Optional[str], Query(max_length=15)] = None,
) -> list[TransformDefinitionL2Row]:
    """Calls ELT.GetTransformDefinition_L2."""
    rows = execute_proc(
        _SCHEMA,
        "GetTransformDefinition_L2",
        {"IngestID": ingest_id, "DeltaDate": delta_date, "InputType": input_type},
    )
    return [TransformDefinitionL2Row(**row) for row in rows]


@router.patch("/l2/{l2_transform_id}", status_code=204)
def update_transform_definition_l2(l2_transform_id: int, body: TransformDefinitionL2Update) -> None:
    """Calls ELT.UpdateTransformDefinition_L2 — updates the L2 delta watermark."""
    execute_proc(
        _SCHEMA,
        "UpdateTransformDefinition_L2",
        {
            "L2TransformID": l2_transform_id,
            "LastDeltaDate": body.last_delta_date,
            "LastDeltaNumber": body.last_delta_number,
        },
    )
