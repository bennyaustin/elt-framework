﻿CREATE TABLE [ELT].[L1TransformInstance]
(
	[L1TransformInstanceID] int not null identity,
	[L1TransformID] int not null,
	[IngestInstanceID] int null,
	[IngestID] int not null,
	[ComputeName] varchar(100) null,
	[ComputePath] varchar(200) null,
	[CustomParameters] varchar(max) null,
	[InputRawFileSystem] varchar(50) not null,
	[InputRawFileFolder] varchar(200) not null,
	[InputRawFile] varchar(200) not null,
	[InputRawFileDelimiter] char(1) null,
	[InputFileHeaderFlag] bit null,
	[OutputL1CurateFileSystem] varchar(50) not null,
	[OutputL1CuratedFolder] varchar(200) not null,
	[OutputL1CuratedFile] varchar(200) not null,
	[OutputL1CuratedFileDelimiter] char(1) null,
	[OutputL1CuratedFileFormat] varchar(10) null,
	[OutputL1CuratedFileWriteMode] varchar(20) null,
	[OutputDWStagingTable] varchar(200) null,
	[LookupColumns] varchar(4000) null,
	[OutputDWTable] varchar(200) null,
	[OutputDWTableWriteMode] varchar(20) null,
	[IngestCount] int null,
	[L1TransformInsertCount] int null,
	[L1TransformUpdateCount] int null,
	[L1TransformDeleteCount] int null,
	[L1TransformStartTimestamp] datetime null,
	[L1TransformEndTimestamp] datetime null,
	[L1TransformStatus] varchar(20) null,
	[RetryCount] int null,
	[ActiveFlag] bit not null,
	[ReRunL1TransformFlag] bit null,
	[IngestADFPipelineRunID] uniqueidentifier null,
	[L1TransformADFPipelineRunID] uniqueidentifier null,
	[CreatedBy] nvarchar(128) not null,
	[CreatedTimestamp] datetime not null,
	[ModifiedBy] nvarchar(128) null,
	[ModifiedTimestamp] datetime null
)
