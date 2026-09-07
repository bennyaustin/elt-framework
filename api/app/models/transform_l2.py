"""Pydantic schemas mirroring ELT.L2Transform* stored procedure parameters and result columns."""
from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


# --- ELT.GetTransformDefinition_L2 ---
class TransformDefinitionL2Query(BaseModel):
    ingest_id: int = Field(..., alias="IngestID")
    delta_date: Optional[datetime] = Field(None, alias="DeltaDate")
    input_type: Optional[str] = Field(None, max_length=15, alias="InputType")

    model_config = {"populate_by_name": True}


class TransformDefinitionL2Row(BaseModel):
    l2_transform_id: int = Field(..., alias="L2TransformID")
    ingest_id: int = Field(..., alias="IngestID")
    l1_transform_id: Optional[int] = Field(None, alias="L1TransformID")
    compute_name: Optional[str] = Field(None, alias="ComputeName")
    compute_path: Optional[str] = Field(None, alias="ComputePath")
    custom_parameters: Optional[str] = Field(None, alias="CustomParameters")
    input_type: Optional[str] = Field(None, alias="InputType")
    input_file_system: Optional[str] = Field(None, alias="InputFileSystem")
    input_file_folder: Optional[str] = Field(None, alias="InputFileFolder")
    input_file: Optional[str] = Field(None, alias="InputFile")
    input_file_delimiter: Optional[str] = Field(None, alias="InputFileDelimiter")
    input_file_header_flag: Optional[bool] = Field(None, alias="InputFileHeaderFlag")
    input_dw_table: Optional[str] = Field(None, alias="InputDWTable")
    watermark_col_name: Optional[str] = Field(None, alias="WatermarkColName")
    data_from_timestamp: Optional[datetime] = Field(None, alias="DataFromTimestamp")
    data_to_timestamp: Optional[datetime] = Field(None, alias="DataToTimestamp")
    data_from_number: Optional[int] = Field(None, alias="DataFromNumber")
    data_to_number: Optional[int] = Field(None, alias="DataToNumber")
    max_interval_minutes: Optional[int] = Field(None, alias="MaxIntervalMinutes")
    max_interval_number: Optional[int] = Field(None, alias="MaxIntervalNumber")
    max_retries: Optional[int] = Field(None, alias="MaxRetries")
    output_l2_curate_file_system: Optional[str] = Field(None, alias="OutputL2CurateFileSystem")
    output_l2_curated_folder: Optional[str] = Field(None, alias="OutputL2CuratedFolder")
    output_l2_curated_file: Optional[str] = Field(None, alias="OutputL2CuratedFile")
    output_l2_curated_file_delimiter: Optional[str] = Field(None, alias="OutputL2CuratedFileDelimiter")
    output_l2_curated_file_format: Optional[str] = Field(None, alias="OutputL2CuratedFileFormat")
    output_l2_curated_file_write_mode: Optional[str] = Field(None, alias="OutputL2CuratedFileWriteMode")
    output_dw_staging_table: Optional[str] = Field(None, alias="OutputDWStagingTable")
    lookup_columns: Optional[str] = Field(None, alias="LookupColumns")
    output_dw_table: Optional[str] = Field(None, alias="OutputDWTable")
    output_dw_table_write_mode: Optional[str] = Field(None, alias="OutputDWTableWriteMode")

    model_config = {"populate_by_name": True}


# --- ELT.UpdateTransformDefinition_L2 ---
class TransformDefinitionL2Update(BaseModel):
    last_delta_date: Optional[datetime] = Field(None, alias="LastDeltaDate")
    last_delta_number: Optional[int] = Field(None, alias="LastDeltaNumber")

    model_config = {"populate_by_name": True}


# --- ELT.GetTransformInstance_L2 ---
class TransformInstanceL2Query(BaseModel):
    source_system_name: str = Field(..., max_length=20, alias="SourceSystemName")
    stream_name: str = Field("%", max_length=100, alias="StreamName")
    max_transform_instance: int = Field(10, alias="MaxTransformInstance")
    l2_transform_instance_id: Optional[int] = Field(None, alias="L2TransformInstanceId")
    delay_l2_transformation_flag: Optional[int] = Field(None, alias="DelayL2TransformationFlag")
    input_type: str = Field("%", max_length=15, alias="InputType")
    l2_transform_id: Optional[int] = Field(None, alias="L2TransformID")

    model_config = {"populate_by_name": True}


class TransformInstanceL2Row(BaseModel):
    l2_transform_instance_id: int = Field(..., alias="L2TransformInstanceID")
    l2_transform_id: int = Field(..., alias="L2TransformID")
    ingest_id: int = Field(..., alias="IngestID")
    l1_transform_id: Optional[int] = Field(None, alias="L1TransformID")
    compute_path: Optional[str] = Field(None, alias="ComputePath")
    compute_name: Optional[str] = Field(None, alias="ComputeName")
    custom_parameters: Optional[str] = Field(None, alias="CustomParameters")
    input_type: Optional[str] = Field(None, alias="InputType")
    input_file_system: Optional[str] = Field(None, alias="InputFileSystem")
    input_file_folder: Optional[str] = Field(None, alias="InputFileFolder")
    input_file: Optional[str] = Field(None, alias="InputFile")
    input_file_delimiter: Optional[str] = Field(None, alias="InputFileDelimiter")
    input_file_header_flag: Optional[bool] = Field(None, alias="InputFileHeaderFlag")
    input_dw_table: Optional[str] = Field(None, alias="InputDWTable")
    watermark_col_name: Optional[str] = Field(None, alias="WatermarkColName")
    data_from_timestamp: Optional[datetime] = Field(None, alias="DataFromTimestamp")
    data_to_timestamp: Optional[datetime] = Field(None, alias="DataToTimestamp")
    data_from_number: Optional[int] = Field(None, alias="DataFromNumber")
    data_to_number: Optional[int] = Field(None, alias="DataToNumber")
    output_l2_curate_file_system: Optional[str] = Field(None, alias="OutputL2CurateFileSystem")
    output_l2_curated_folder: Optional[str] = Field(None, alias="OutputL2CuratedFolder")
    output_l2_curated_file: Optional[str] = Field(None, alias="OutputL2CuratedFile")
    output_l2_curated_file_delimiter: Optional[str] = Field(None, alias="OutputL2CuratedFileDelimiter")
    output_l2_curated_file_format: Optional[str] = Field(None, alias="OutputL2CuratedFileFormat")
    output_l2_curated_file_write_mode: Optional[str] = Field(None, alias="OutputL2CuratedFileWriteMode")
    output_dw_staging_table: Optional[str] = Field(None, alias="OutputDWStagingTable")
    lookup_columns: Optional[str] = Field(None, alias="LookupColumns")
    output_dw_table: Optional[str] = Field(None, alias="OutputDWTable")
    output_dw_table_write_mode: Optional[str] = Field(None, alias="OutputDWTableWriteMode")
    re_run_l2_transform_flag: Optional[bool] = Field(None, alias="ReRunL2TransformFlag")
    max_retries: Optional[int] = Field(None, alias="MaxRetries")

    model_config = {"populate_by_name": True}


# --- ELT.InsertTransformInstance_L2 ---
class TransformInstanceL2Create(BaseModel):
    l2_transform_id: Optional[int] = Field(None, alias="L2TransformID")
    ingest_id: int = Field(..., alias="IngestID")
    l1_transform_id: int = Field(..., alias="L1TransformID")
    compute_name: Optional[str] = Field(None, max_length=100, alias="ComputeName")
    compute_path: Optional[str] = Field(None, max_length=200, alias="ComputePath")
    custom_parameters: Optional[str] = Field(None, alias="CustomParameters")
    input_file_system: Optional[str] = Field(None, max_length=50, alias="InputFileSystem")
    input_file_folder: Optional[str] = Field(None, max_length=200, alias="InputFileFolder")
    input_file: Optional[str] = Field(None, max_length=200, alias="InputFile")
    input_file_delimiter: Optional[str] = Field(None, max_length=1, alias="InputFileDelimiter")
    input_file_header_flag: Optional[bool] = Field(None, alias="InputFileHeaderFlag")
    input_dw_table: Optional[str] = Field(None, max_length=200, alias="InputDWTable")
    watermark_col_name: Optional[str] = Field(None, max_length=50, alias="WatermarkColName")
    data_from_timestamp: Optional[datetime] = Field(None, alias="DataFromTimestamp")
    data_to_timestamp: Optional[datetime] = Field(None, alias="DataToTimestamp")
    data_from_number: Optional[int] = Field(None, alias="DataFromNumber")
    data_to_number: Optional[int] = Field(None, alias="DataToNumber")
    last_delta_date: Optional[datetime] = Field(None, alias="LastDeltaDate")
    last_delta_number: Optional[int] = Field(None, alias="LastDeltaNumber")
    output_l2_curate_file_system: str = Field(..., max_length=50, alias="OutputL2CurateFileSystem")
    output_l2_curated_folder: str = Field(..., max_length=200, alias="OutputL2CuratedFolder")
    output_l2_curated_file: str = Field(..., max_length=200, alias="OutputL2CuratedFile")
    output_l2_curated_file_delimiter: Optional[str] = Field(None, max_length=1, alias="OutputL2CuratedFileDelimiter")
    output_l2_curated_file_format: Optional[str] = Field(None, max_length=10, alias="OutputL2CuratedFileFormat")
    output_l2_curated_file_write_mode: Optional[str] = Field(None, max_length=20, alias="OutputL2CuratedFileWriteMode")
    output_dw_staging_table: Optional[str] = Field(None, max_length=200, alias="OutputDWStagingTable")
    lookup_columns: Optional[str] = Field(None, max_length=4000, alias="LookupColumns")
    output_dw_table: Optional[str] = Field(None, max_length=200, alias="OutputDWTable")
    output_dw_table_write_mode: Optional[str] = Field(None, max_length=20, alias="OutputDWTableWriteMode")
    ingest_adf_pipeline_run_id: Optional[UUID] = Field(None, alias="IngestADFPipelineRunID")
    l1_transform_adf_pipeline_run_id: Optional[UUID] = Field(None, alias="L1TransformADFPipelineRunID")
    max_retries: Optional[int] = Field(None, alias="MaxRetries")

    model_config = {"populate_by_name": True}


# --- ELT.UpdateTransformInstance_L2 ---
class TransformInstanceL2Update(BaseModel):
    l2_transform_status: str = Field(..., max_length=20, alias="L2TransformStatus")
    l2_transform_adf_pipeline_run_id: UUID = Field(..., alias="L2TransformADFPipelineRunID")
    input_count: Optional[int] = Field(None, alias="InputCount")
    l2_transform_insert_count: Optional[int] = Field(None, alias="L2TransformInsertCount")
    l2_transform_update_count: Optional[int] = Field(None, alias="L2TransformUpdateCount")
    l2_transform_delete_count: Optional[int] = Field(None, alias="L2TransformDeleteCount")
    data_from_timestamp: Optional[datetime] = Field(None, alias="DataFromTimestamp")
    data_to_timestamp: Optional[datetime] = Field(None, alias="DataToTimestamp")
    data_from_number: Optional[int] = Field(None, alias="DataFromNumber")
    data_to_number: Optional[int] = Field(None, alias="DataToNumber")
    max_retries: Optional[int] = Field(None, alias="MaxRetries")

    model_config = {"populate_by_name": True}


# --- ELT.GetRunningTransformInstances ---
class RunningTransformInstancesQuery(BaseModel):
    timespan_hrs: int = Field(18, alias="timespanHrs")

    model_config = {"populate_by_name": True}


class RunningTransformInstancesRow(BaseModel):
    running_count: int = Field(..., alias="RunningCount")

    model_config = {"populate_by_name": True}
