"""Pydantic schemas mirroring ELT.L1Transform* stored procedure parameters and result columns."""
from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


# --- ELT.GetTransformDefinition_L1 ---
class TransformDefinitionL1Query(BaseModel):
    ingest_id: int = Field(..., alias="IngestID")
    delta_date: Optional[datetime] = Field(None, alias="DeltaDate")

    model_config = {"populate_by_name": True}


class TransformDefinitionL1Row(BaseModel):
    l1_transform_id: int = Field(..., alias="L1TransformID")
    ingest_id: int = Field(..., alias="IngestID")
    compute_name: Optional[str] = Field(None, alias="ComputeName")
    compute_path: Optional[str] = Field(None, alias="ComputePath")
    custom_parameters: Optional[str] = Field(None, alias="CustomParameters")
    input_raw_file_delimiter: Optional[str] = Field(None, alias="InputRawFileDelimiter")
    input_file_header_flag: Optional[bool] = Field(None, alias="InputFileHeaderFlag")
    input_raw_table: Optional[str] = Field(None, alias="InputRawTable")
    output_l1_curate_file_system: Optional[str] = Field(None, alias="OutputL1CurateFileSystem")
    output_l1_curated_folder: Optional[str] = Field(None, alias="OutputL1CuratedFolder")
    output_l1_curated_file: Optional[str] = Field(None, alias="OutputL1CuratedFile")
    output_l1_curated_file_delimiter: Optional[str] = Field(None, alias="OutputL1CuratedFileDelimiter")
    output_l1_curated_file_format: Optional[str] = Field(None, alias="OutputL1CuratedFileFormat")
    output_l1_curated_file_write_mode: Optional[str] = Field(None, alias="OutputL1CuratedFileWriteMode")
    lookup_columns: Optional[str] = Field(None, alias="LookupColumns")
    output_dw_staging_table: Optional[str] = Field(None, alias="OutputDWStagingTable")
    output_dw_table: Optional[str] = Field(None, alias="OutputDWTable")
    output_dw_table_write_mode: Optional[str] = Field(None, alias="OutputDWTableWriteMode")
    max_retries: Optional[int] = Field(None, alias="MaxRetries")

    model_config = {"populate_by_name": True}


# --- ELT.GetTransformInstance_L1 ---
class TransformInstanceL1Query(BaseModel):
    source_system_name: str = Field(..., max_length=20, alias="SourceSystemName")
    stream_name: str = Field("%", max_length=100, alias="StreamName")
    max_transform_instance: int = Field(10, alias="MaxTransformInstance")
    l1_transform_instance_id: Optional[int] = Field(None, alias="L1TransformInstanceId")
    delay_l1_transformation_flag: Optional[int] = Field(None, alias="DelayL1TransformationFlag")

    model_config = {"populate_by_name": True}


class TransformInstanceL1Row(BaseModel):
    l1_transform_instance_id: int = Field(..., alias="L1TransformInstanceID")
    l1_transform_id: int = Field(..., alias="L1TransformID")
    ingest_id: int = Field(..., alias="IngestID")
    compute_name: Optional[str] = Field(None, alias="ComputeName")
    compute_path: Optional[str] = Field(None, alias="ComputePath")
    custom_parameters: Optional[str] = Field(None, alias="CustomParameters")
    input_raw_file_system: Optional[str] = Field(None, alias="InputRawFileSystem")
    input_raw_file_folder: Optional[str] = Field(None, alias="InputRawFileFolder")
    input_raw_file: Optional[str] = Field(None, alias="InputRawFile")
    input_raw_file_delimiter: Optional[str] = Field(None, alias="InputRawFileDelimiter")
    input_file_header_flag: Optional[bool] = Field(None, alias="InputFileHeaderFlag")
    input_raw_table: Optional[str] = Field(None, alias="InputRawTable")
    data_from_timestamp: Optional[datetime] = Field(None, alias="DataFromTimestamp")
    data_to_timestamp: Optional[datetime] = Field(None, alias="DataToTimestamp")
    output_l1_curate_file_system: Optional[str] = Field(None, alias="OutputL1CurateFileSystem")
    output_l1_curated_folder: Optional[str] = Field(None, alias="OutputL1CuratedFolder")
    output_l1_curated_file: Optional[str] = Field(None, alias="OutputL1CuratedFile")
    output_l1_curated_file_delimiter: Optional[str] = Field(None, alias="OutputL1CuratedFileDelimiter")
    output_l1_curated_file_format: Optional[str] = Field(None, alias="OutputL1CuratedFileFormat")
    output_l1_curated_file_write_mode: Optional[str] = Field(None, alias="OutputL1CuratedFileWriteMode")
    output_dw_staging_table: Optional[str] = Field(None, alias="OutputDWStagingTable")
    lookup_columns: Optional[str] = Field(None, alias="LookupColumns")
    output_dw_table: Optional[str] = Field(None, alias="OutputDWTable")
    output_dw_table_write_mode: Optional[str] = Field(None, alias="OutputDWTableWriteMode")
    re_run_l1_transform_flag: Optional[bool] = Field(None, alias="ReRunL1TransformFlag")
    max_retries: Optional[int] = Field(None, alias="MaxRetries")
    watermark_col_name: Optional[str] = Field(None, alias="WatermarkColName")

    model_config = {"populate_by_name": True}


# --- ELT.InsertTransformInstance_L1 ---
class TransformInstanceL1Create(BaseModel):
    l1_transform_id: Optional[int] = Field(None, alias="L1TransformID")
    ingest_instance_id: Optional[int] = Field(None, alias="IngestInstanceID")
    ingest_id: int = Field(..., alias="IngestID")
    compute_name: Optional[str] = Field(None, max_length=100, alias="ComputeName")
    compute_path: Optional[str] = Field(None, max_length=200, alias="ComputePath")
    custom_parameters: Optional[str] = Field(None, alias="CustomParameters")
    input_raw_file_system: Optional[str] = Field(None, max_length=50, alias="InputRawFileSystem")
    input_raw_file_folder: Optional[str] = Field(None, max_length=200, alias="InputRawFileFolder")
    input_raw_file: Optional[str] = Field(None, max_length=200, alias="InputRawFile")
    input_raw_file_delimiter: Optional[str] = Field(None, max_length=1, alias="InputRawFileDelimiter")
    input_file_header_flag: Optional[bool] = Field(None, alias="InputFileHeaderFlag")
    input_raw_table: Optional[str] = Field(None, max_length=200, alias="InputRawTable")
    data_from_timestamp: Optional[datetime] = Field(None, alias="DataFromTimestamp")
    data_to_timestamp: Optional[datetime] = Field(None, alias="DataToTimestamp")
    output_l1_curate_file_system: Optional[str] = Field(None, max_length=50, alias="OutputL1CurateFileSystem")
    output_l1_curated_folder: Optional[str] = Field(None, max_length=200, alias="OutputL1CuratedFolder")
    output_l1_curated_file: Optional[str] = Field(None, max_length=200, alias="OutputL1CuratedFile")
    output_l1_curated_file_delimiter: Optional[str] = Field(None, max_length=1, alias="OutputL1CuratedFileDelimiter")
    output_l1_curated_file_format: Optional[str] = Field(None, max_length=10, alias="OutputL1CuratedFileFormat")
    output_l1_curated_file_write_mode: Optional[str] = Field(None, max_length=20, alias="OutputL1CuratedFileWriteMode")
    output_dw_staging_table: Optional[str] = Field(None, max_length=200, alias="OutputDWStagingTable")
    lookup_columns: Optional[str] = Field(None, max_length=4000, alias="LookupColumns")
    output_dw_table: Optional[str] = Field(None, max_length=200, alias="OutputDWTable")
    output_dw_table_write_mode: Optional[str] = Field(None, max_length=20, alias="OutputDWTableWriteMode")
    ingest_count: Optional[int] = Field(None, alias="IngestCount")
    ingest_adf_pipeline_run_id: Optional[UUID] = Field(None, alias="IngestADFPipelineRunID")

    model_config = {"populate_by_name": True}


# --- ELT.UpdateTransformInstance_L1 ---
class TransformInstanceL1Update(BaseModel):
    l1_transform_status: str = Field(..., max_length=20, alias="L1TransformStatus")
    l1_transform_adf_pipeline_run_id: UUID = Field(..., alias="L1TransformADFPipelineRunID")
    ingest_count: Optional[int] = Field(None, alias="IngestCount")
    l1_transform_insert_count: Optional[int] = Field(None, alias="L1TransformInsertCount")
    l1_transform_update_count: Optional[int] = Field(None, alias="L1TransformUpdateCount")
    l1_transform_delete_count: Optional[int] = Field(None, alias="L1TransformDeleteCount")
    max_retries: Optional[int] = Field(None, alias="MaxRetries")

    model_config = {"populate_by_name": True}
