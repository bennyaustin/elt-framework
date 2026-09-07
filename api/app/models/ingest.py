"""Pydantic schemas mirroring ELT.Ingest* stored procedure parameters and result columns."""
from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


# --- ELT.GetIngestDefinition ---
class IngestDefinitionQuery(BaseModel):
    source_system_name: str = Field(..., max_length=20, alias="SourceSystemName")
    stream_name: str = Field("%", max_length=100, alias="StreamName")
    max_ingest_instance: int = Field(10, alias="MaxIngestInstance")

    model_config = {"populate_by_name": True}


class IngestDefinitionRow(BaseModel):
    ingest_id: int = Field(..., alias="IngestID")
    source_system_name: str = Field(..., alias="SourceSystemName")
    stream_name: str = Field(..., alias="StreamName")
    backend: Optional[str] = Field(None, alias="Backend")
    entity_name: Optional[str] = Field(None, alias="EntityName")
    watermark_col_name: Optional[str] = Field(None, alias="WatermarkColName")
    last_delta_date: Optional[datetime] = Field(None, alias="LastDeltaDate")
    data_from_timestamp: Optional[datetime] = Field(None, alias="DataFromTimestamp")
    data_to_timestamp: Optional[datetime] = Field(None, alias="DataToTimestamp")
    last_delta_number: Optional[int] = Field(None, alias="LastDeltaNumber")
    data_from_number: Optional[int] = Field(None, alias="DataFromNumber")
    data_to_number: Optional[int] = Field(None, alias="DataToNumber")
    data_format: Optional[str] = Field(None, alias="DataFormat")
    source_structure: Optional[str] = Field(None, alias="SourceStructure")
    max_interval_minutes: Optional[int] = Field(None, alias="MaxIntervalMinutes")
    max_interval_number: Optional[int] = Field(None, alias="MaxIntervalNumber")
    data_mapping: Optional[str] = Field(None, alias="DataMapping")
    run_sequence: Optional[int] = Field(None, alias="RunSequence")
    active_flag: Optional[bool] = Field(None, alias="ActiveFlag")
    l1_transformation_reqd_flag: Optional[bool] = Field(None, alias="L1TransformationReqdFlag")
    l2_transformation_reqd_flag: Optional[bool] = Field(None, alias="L2TransformationReqdFlag")
    delay_l1_transformation_flag: Optional[bool] = Field(None, alias="DelayL1TransformationFlag")
    delay_l2_transformation_flag: Optional[bool] = Field(None, alias="DelayL2TransformationFlag")
    destination_raw_file_system: Optional[str] = Field(None, alias="DestinationRawFileSystem")
    destination_raw_folder: Optional[str] = Field(None, alias="DestinationRawFolder")
    destination_raw_file: Optional[str] = Field(None, alias="DestinationRawFile")
    destination_raw_table: Optional[str] = Field(None, alias="DestinationRawTable")
    source_sql: Optional[str] = Field(None, alias="SourceSQL")
    stat_sql: Optional[str] = Field(None, alias="StatSQL")
    reload_flag: Optional[bool] = Field(None, alias="ReloadFlag")
    adf_pipeline_run_id: Optional[UUID] = Field(None, alias="ADFPipelineRunID")

    model_config = {"populate_by_name": True}


# --- ELT.GetIngestDefinition_FileDrop ---
class IngestDefinitionFileDropQuery(BaseModel):
    source_system_name: str = Field(..., max_length=20, alias="SourceSystemName")
    stream_name: str = Field(..., max_length=100, alias="StreamName")
    source_folder: str = Field(..., max_length=250, alias="SourceFolder")
    source_file: str = Field(..., max_length=200, alias="SourceFile")

    model_config = {"populate_by_name": True}


class IngestDefinitionFileDropRow(BaseModel):
    ingest_id: int = Field(..., alias="IngestID")
    source_system_name: str = Field(..., alias="SourceSystemName")
    stream_name: str = Field(..., alias="StreamName")
    backend: Optional[str] = Field(None, alias="Backend")
    data_format: Optional[str] = Field(None, alias="DataFormat")
    source_file_drop_file_system: Optional[str] = Field(None, alias="SourceFileDropFileSystem")
    source_file_drop_folder: Optional[str] = Field(None, alias="SourceFileDropFolder")
    source_file_drop_file: Optional[str] = Field(None, alias="SourceFileDropFile")
    source_file_delimiter: Optional[str] = Field(None, alias="SourceFileDelimiter")
    source_file_header_flag: Optional[bool] = Field(None, alias="SourceFileHeaderFlag")
    destination_raw_file_system: Optional[str] = Field(None, alias="DestinationRawFileSystem")
    destination_raw_folder: Optional[str] = Field(None, alias="DestinationRawFolder")
    destination_raw_file: Optional[str] = Field(None, alias="DestinationRawFile")
    data_mapping: Optional[str] = Field(None, alias="DataMapping")
    reload_flag: Optional[bool] = Field(None, alias="ReloadFlag")
    l1_transformation_reqd_flag: Optional[bool] = Field(None, alias="L1TransformationReqdFlag")
    l2_transformation_reqd_flag: Optional[bool] = Field(None, alias="L2TransformationReqdFlag")
    delay_l1_transformation_flag: Optional[bool] = Field(None, alias="DelayL1TransformationFlag")
    delay_l2_transformation_flag: Optional[bool] = Field(None, alias="DelayL2TransformationFlag")
    adf_ingest_pipeline_run_id: Optional[UUID] = Field(None, alias="ADFIngestPipelineRunID")

    model_config = {"populate_by_name": True}


# --- ELT.UpdateIngestDefinition ---
class IngestDefinitionUpdate(BaseModel):
    last_delta_date: Optional[datetime] = Field(None, alias="LastDeltaDate")
    last_delta_number: Optional[int] = Field(None, alias="LastDeltaNumer")
    ingest_status: str = Field(..., max_length=20, alias="IngestStatus")
    reload_flag: bool = Field(False, alias="ReloadFlag")

    model_config = {"populate_by_name": True}


# --- ELT.InsertIngestInstance ---
class IngestInstanceCreate(BaseModel):
    ingest_id: int = Field(..., alias="IngestID")
    source_file_drop_file_system: Optional[str] = Field(None, max_length=50, alias="SourceFileDropFileSystem")
    source_file_drop_folder: Optional[str] = Field(None, max_length=200, alias="SourceFileDropFolder")
    source_file_drop_file: Optional[str] = Field(None, max_length=200, alias="SourceFileDropFile")
    destination_raw_file_system: Optional[str] = Field(None, max_length=50, alias="DestinationRawFileSystem")
    destination_raw_folder: Optional[str] = Field(None, max_length=200, alias="DestinationRawFolder")
    destination_raw_file: Optional[str] = Field(None, max_length=200, alias="DestinationRawFile")
    destination_raw_table: Optional[str] = Field(None, max_length=200, alias="DestinationRawTable")
    reload_flag: bool = Field(False, alias="ReloadFlag")
    adf_pipeline_run_id: Optional[UUID] = Field(None, alias="ADFPipelineRunID")

    model_config = {"populate_by_name": True}


# --- ELT.UpdateIngestInstance ---
class IngestInstanceUpdate(BaseModel):
    ingest_status: Optional[str] = Field(None, max_length=20, alias="IngestStatus")
    data_from_timestamp: Optional[datetime] = Field(None, alias="DataFromTimestamp")
    data_to_timestamp: Optional[datetime] = Field(None, alias="DataToTimestamp")
    data_from_number: Optional[int] = Field(None, alias="DataFromNumber")
    data_to_number: Optional[int] = Field(None, alias="DataToNumber")
    source_count: Optional[int] = Field(None, alias="SourceCount")
    ingest_count: Optional[int] = Field(None, alias="IngestCount")
    reload_flag: bool = Field(..., alias="ReloadFlag")

    model_config = {"populate_by_name": True}
