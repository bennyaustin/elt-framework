﻿CREATE TABLE [ELT].[L2TransformInstance]
(
	[L2TransformInstanceID] int not null identity,
	[L2TransformID] int null,
	[IngestID] int null,
	[L1TransformID] int null,
	[ComputePath] varchar(200) null,
	[ComputeName] varchar(100) null,
	[CustomParameters] varchar(max) null,
	[InputFileSystem] varchar(50) null,
	[InputFileFolder] varchar(200) null,
	[InputFile] varchar(200) null,
	[InputFileDelimiter] char(1) null,
	[InputFileHeaderFlag] bit null,
	[InputDWTable] varchar(200) null,
	[WatermarkColName] varchar(50) null,
	[DataFromTimestamp] dateTime2 null,
	[DataToTimestamp] datetime2 null,
	[DataFromNumber] int null,
	[DataToNumber] int null,
	[OutputL2CurateFileSystem] varchar(50) not null,
	[OutputL2CuratedFolder] varchar(200) not null,
	[OutputL2CuratedFile] varchar(200) not null,
	[OutputL2CuratedFileDelimiter] char(1) null,
	[OutputL2CuratedFileFormat] varchar(10) null,
	[OutputL2CuratedFileWriteMode] varchar(20) null,
	[OutputDWStagingTable] varchar(200) null,
	[LookupColumns] varchar(4000) null,
	[OutputDWTable] varchar(200) null,
	[OutputDWTableWriteMode] varchar(20) null,
	[InputCount] int null,
	[L2TransformInsertCount] int null,
	[L2TransformUpdateCount] int null,
	[L2TransformDeleteCount] int null,
	[L2TransformStartTimestamp] datetime null,
	[L2TransformEndTimestamp] datetime null,
	[L2TransformStatus] varchar(20) null,
	[RetryCount] int null,
	[ActiveFlag] bit not null,
	[ReRunL2TransformFlag] bit null,
	[IngestADFPipelineRunID] uniqueidentifier null,
	[L1TransformADFPipelineRunID] uniqueidentifier null,
	[L2TransformADFPipelineRunID] uniqueidentifier null,
	[CreatedBy] nvarchar(128) not null,
	[CreatedTimestamp] datetime not null,
	[ModifiedBy] nvarchar(128) null,
	[ModifiedTimestamp] datetime null, 
    [L2SnapshotGroupID] INT NULL, 
    [L2SnapshotInstanceID] INT NULL
)
