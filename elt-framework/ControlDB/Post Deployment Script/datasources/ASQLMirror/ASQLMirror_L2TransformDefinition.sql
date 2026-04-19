IF OBJECT_ID('tempdb..#ASQLMirror_L2TransformDefinition') IS NOT NULL DROP TABLE #ASQLMirror_L2TransformDefinition;

CREATE TABLE #ASQLMirror_L2TransformDefinition(
	[IngestID] [int] NULL,
	[L1TransformID] [int] NULL,
	[ComputeName] [varchar](100) NULL,
	[InputFileSystem] [varchar](50) NULL,
	[InputFileFolder] [varchar](200) NULL,
	[InputFile] [varchar](200) NULL,
	[InputDWTable] [varchar](200) NULL,
	[OutputDWTable] [varchar](200) NULL,
	[OutputDWTableWriteMode] [varchar](20) NULL,
	[ActiveFlag] [bit] NOT NULL
    );

Insert into #ASQLMirror_L2TransformDefinition
SELECT 
    L1.[IngestID]
    ,L1.[L1TransformID]
    ,'gold.mirror_create_'+ Trim(Lower(I.StreamName)) + '_monthly_snapshot'  AS [ComputeName]
    ,L1.[OutputL1CurateFileSystem] AS [InputFileSystem]
    ,L1.[OutputL1CuratedFolder] AS [InputFileFolder]
    ,L1.[OutputL1CuratedFile] AS [InputFile]
    ,L1.[OutputDWTable] AS [InputDWTable]
    ,'gold.Mirror_'+ Trim(I.StreamName) + '_monthly_snapshot'  AS [OutputDWTable]
    ,'overwrite' AS [OutputDWTableWriteMode]
    , 1 AS [ActiveFlag]
FROM [ELT].[L1TransformDefinition] L1
INNER JOIN [ELT].[IngestDefinition] I ON L1.IngestID = I.IngestID
    AND I.SourceSystemName='WWI-mirror'
    AND I.WatermarkColName IS NOT NULL --Only for transaction tables
    AND I.ActiveFlag=1
WHERE L1.ActiveFlag=1;

MERGE INTO [ELT].[L2TransformDefinition] AS TARGET
USING #ASQLMirror_L2TransformDefinition AS SOURCE  
ON TARGET.OutputDWTable = SOURCE.OutputDWTable
AND TARGET.InputDWTable = SOURCE.InputDWTable
WHEN MATCHED THEN 
    UPDATE SET 
        TARGET.IngestID = SOURCE.IngestID,
        TARGET.L1TransformID = SOURCE.L1TransformID,
        TARGET.ComputeName = SOURCE.ComputeName,
        TARGET.InputFileSystem = SOURCE.InputFileSystem,
        TARGET.InputFileFolder = SOURCE.InputFileFolder,
        TARGET.InputFile = SOURCE.InputFile,
        TARGET.InputDWTable = SOURCE.InputDWTable,
        TARGET.OutputDWTable = SOURCE.OutputDWTable,
        TARGET.OutputDWTableWriteMode = SOURCE.OutputDWTableWriteMode,
        TARGET.ActiveFlag = SOURCE.ActiveFlag
WHEN NOT MATCHED BY TARGET THEN
    INSERT (IngestID, 
            L1TransformID, 
            ComputeName, 
            InputFileSystem, 
            InputFileFolder, 
            InputFile, 
            InputDWTable, 
            OutputDWTable, 
            OutputDWTableWriteMode, 
            ActiveFlag)
    VALUES (IngestID, 
            L1TransformID, 
            ComputeName, 
            InputFileSystem, 
            InputFileFolder, 
            InputFile, 
            InputDWTable, 
            OutputDWTable, 
            OutputDWTableWriteMode, 
            ActiveFlag);