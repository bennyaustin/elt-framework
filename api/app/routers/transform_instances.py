"""Routes for ELT.GetTransformInstance_L1/L2, ELT.InsertTransformInstance_L1/L2,
ELT.UpdateTransformInstance_L1/L2, and ELT.GetRunningTransformInstances."""
from typing import Optional

from fastapi import APIRouter, Depends, status

from app.auth.aad import verify_token
from app.db.executor import execute_proc
from app.models.transform_l1 import (
    TransformInstanceL1Create,
    TransformInstanceL1Row,
    TransformInstanceL1Update,
)
from app.models.transform_l2 import (
    RunningTransformInstancesRow,
    TransformInstanceL2Create,
    TransformInstanceL2Row,
    TransformInstanceL2Update,
)

router = APIRouter(prefix="/transform/instances", tags=["transform-instances"], dependencies=[Depends(verify_token)])

_SCHEMA = "ELT"


@router.get("/running", response_model=RunningTransformInstancesRow)
def get_running_transform_instances(timespan_hrs: int = 18) -> RunningTransformInstancesRow:
    """Calls ELT.GetRunningTransformInstances — count of currently running L1+L2 transforms."""
    rows = execute_proc(_SCHEMA, "GetRunningTransformInstances", {"timespanHrs": timespan_hrs})
    return RunningTransformInstancesRow(**rows[0])


@router.get("/l1", response_model=list[TransformInstanceL1Row])
def get_transform_instance_l1(
    source_system_name: str,
    stream_name: str = "%",
    max_transform_instance: int = 10,
    l1_transform_instance_id: Optional[int] = None,
    delay_l1_transformation_flag: Optional[int] = None,
) -> list[TransformInstanceL1Row]:
    """Calls ELT.GetTransformInstance_L1."""
    rows = execute_proc(
        _SCHEMA,
        "GetTransformInstance_L1",
        {
            "SourceSystemName": source_system_name,
            "StreamName": stream_name,
            "MaxTransformInstance": max_transform_instance,
            "L1TransformInstanceId": l1_transform_instance_id,
            "DelayL1TransformationFlag": delay_l1_transformation_flag,
        },
    )
    return [TransformInstanceL1Row(**row) for row in rows]


@router.get("/l2", response_model=list[TransformInstanceL2Row])
def get_transform_instance_l2(
    source_system_name: str,
    stream_name: str = "%",
    max_transform_instance: int = 10,
    l2_transform_instance_id: Optional[int] = None,
    delay_l2_transformation_flag: Optional[int] = None,
    input_type: str = "%",
    l2_transform_id: Optional[int] = None,
) -> list[TransformInstanceL2Row]:
    """Calls ELT.GetTransformInstance_L2."""
    rows = execute_proc(
        _SCHEMA,
        "GetTransformInstance_L2",
        {
            "SourceSystemName": source_system_name,
            "StreamName": stream_name,
            "MaxTransformInstance": max_transform_instance,
            "L2TransformInstanceId": l2_transform_instance_id,
            "DelayL2TransformationFlag": delay_l2_transformation_flag,
            "InputType": input_type,
            "L2TransformID": l2_transform_id,
        },
    )
    return [TransformInstanceL2Row(**row) for row in rows]


@router.post("/l1", status_code=status.HTTP_201_CREATED)
def create_transform_instance_l1(body: TransformInstanceL1Create) -> None:
    """Calls ELT.InsertTransformInstance_L1 — records/reactivates an L1 transform run."""
    execute_proc(
        _SCHEMA,
        "InsertTransformInstance_L1",
        {
            "L1TransformID": body.l1_transform_id,
            "IngestInstanceID": body.ingest_instance_id,
            "IngestID": body.ingest_id,
            "ComputeName": body.compute_name,
            "ComputePath": body.compute_path,
            "CustomParameters": body.custom_parameters,
            "InputRawFileSystem": body.input_raw_file_system,
            "InputRawFileFolder": body.input_raw_file_folder,
            "InputRawFile": body.input_raw_file,
            "InputRawFileDelimiter": body.input_raw_file_delimiter,
            "InputFileHeaderFlag": body.input_file_header_flag,
            "InputRawTable": body.input_raw_table,
            "DataFromTimestamp": body.data_from_timestamp,
            "DataToTimestamp": body.data_to_timestamp,
            "OutputL1CurateFileSystem": body.output_l1_curate_file_system,
            "OutputL1CuratedFolder": body.output_l1_curated_folder,
            "OutputL1CuratedFile": body.output_l1_curated_file,
            "OutputL1CuratedFileDelimiter": body.output_l1_curated_file_delimiter,
            "OutputL1CuratedFileFormat": body.output_l1_curated_file_format,
            "OutputL1CuratedFileWriteMode": body.output_l1_curated_file_write_mode,
            "OutputDWStagingTable": body.output_dw_staging_table,
            "LookupColumns": body.lookup_columns,
            "OutputDWTable": body.output_dw_table,
            "OutputDWTableWriteMode": body.output_dw_table_write_mode,
            "IngestCount": body.ingest_count,
            "IngestADFPipelineRunID": body.ingest_adf_pipeline_run_id,
        },
    )


@router.post("/l2", status_code=status.HTTP_201_CREATED)
def create_transform_instance_l2(body: TransformInstanceL2Create) -> None:
    """Calls ELT.InsertTransformInstance_L2 — records/reactivates an L2 transform run."""
    execute_proc(
        _SCHEMA,
        "InsertTransformInstance_L2",
        {
            "L2TransformID": body.l2_transform_id,
            "IngestID": body.ingest_id,
            "L1TransformID": body.l1_transform_id,
            "ComputeName": body.compute_name,
            "ComputePath": body.compute_path,
            "CustomParameters": body.custom_parameters,
            "InputFileSystem": body.input_file_system,
            "InputFileFolder": body.input_file_folder,
            "InputFile": body.input_file,
            "InputFileDelimiter": body.input_file_delimiter,
            "InputFileHeaderFlag": body.input_file_header_flag,
            "InputDWTable": body.input_dw_table,
            "WatermarkColName": body.watermark_col_name,
            "DataFromTimestamp": body.data_from_timestamp,
            "DataToTimestamp": body.data_to_timestamp,
            "DataFromNumber": body.data_from_number,
            "DataToNumber": body.data_to_number,
            "LastDeltaDate": body.last_delta_date,
            "LastDeltaNumber": body.last_delta_number,
            "OutputL2CurateFileSystem": body.output_l2_curate_file_system,
            "OutputL2CuratedFolder": body.output_l2_curated_folder,
            "OutputL2CuratedFile": body.output_l2_curated_file,
            "OutputL2CuratedFileDelimiter": body.output_l2_curated_file_delimiter,
            "OutputL2CuratedFileFormat": body.output_l2_curated_file_format,
            "OutputL2CuratedFileWriteMode": body.output_l2_curated_file_write_mode,
            "OutputDWStagingTable": body.output_dw_staging_table,
            "LookupColumns": body.lookup_columns,
            "OutputDWTable": body.output_dw_table,
            "OutputDWTableWriteMode": body.output_dw_table_write_mode,
            "IngestADFPipelineRunID": body.ingest_adf_pipeline_run_id,
            "L1TransformADFPipelineRunID": body.l1_transform_adf_pipeline_run_id,
            "MaxRetries": body.max_retries,
        },
    )


@router.patch("/l1/{l1_transform_instance_id}", status_code=status.HTTP_204_NO_CONTENT)
def update_transform_instance_l1(l1_transform_instance_id: int, body: TransformInstanceL1Update) -> None:
    """Calls ELT.UpdateTransformInstance_L1 — records the outcome of an L1 transform run."""
    execute_proc(
        _SCHEMA,
        "UpdateTransformInstance_L1",
        {
            "L1TransformInstanceId": l1_transform_instance_id,
            "L1TransformStatus": body.l1_transform_status,
            "L1TransformADFPipelineRunID": body.l1_transform_adf_pipeline_run_id,
            "IngestCount": body.ingest_count,
            "L1TransformInsertCount": body.l1_transform_insert_count,
            "L1TransformUpdateCount": body.l1_transform_update_count,
            "L1TransformDeleteCount": body.l1_transform_delete_count,
            "MaxRetries": body.max_retries,
        },
    )


@router.patch("/l2/{l2_transform_instance_id}", status_code=status.HTTP_204_NO_CONTENT)
def update_transform_instance_l2(l2_transform_instance_id: int, body: TransformInstanceL2Update) -> None:
    """Calls ELT.UpdateTransformInstance_L2 — records the outcome of an L2 transform run."""
    execute_proc(
        _SCHEMA,
        "UpdateTransformInstance_L2",
        {
            "L2TransformInstanceId": l2_transform_instance_id,
            "L2TransformStatus": body.l2_transform_status,
            "L2TransformADFPipelineRunID": body.l2_transform_adf_pipeline_run_id,
            "InputCount": body.input_count,
            "L2TransformInsertCount": body.l2_transform_insert_count,
            "L2TransformUpdateCount": body.l2_transform_update_count,
            "L2TransformDeleteCount": body.l2_transform_delete_count,
            "DataFromTimestamp": body.data_from_timestamp,
            "DataToTimestamp": body.data_to_timestamp,
            "DataFromNumber": body.data_from_number,
            "DataToNumber": body.data_to_number,
            "MaxRetries": body.max_retries,
        },
    )
