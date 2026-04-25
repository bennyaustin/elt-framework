IF OBJECT_ID('tempdb..#ASQLMirror_L1TransformDefinition') IS NOT NULL DROP TABLE #ASQLMirror_L1TransformDefinition;

--Create Temp table with same structure as L1TransformDefinition
CREATE TABLE #ASQLMirror_L1TransformDefinition(
	[IngestID] [int] NOT NULL,
	[ComputeName] [varchar](100) NULL,
    [InputRawTable] [varchar](200) NULL,
	[LookupColumns] [varchar](4000) NULL,
	[OutputDWTable] [varchar](200) NULL,
	[OutputDWTableWriteMode] [varchar](20) NULL,
    [OutputL1CurateFileSystem] varchar(50) not null,
	[OutputL1CuratedFolder] varchar(200) not null,
	[OutputL1CuratedFile] varchar(200) not null,
	[WatermarkColName] [varchar](50) NULL,
	[ActiveFlag] [bit] NOT NULL
    );

INSERT INTO #ASQLMirror_L1TransformDefinition
    SELECT  [IngestID]
    ,'L1Transform-Generic-Fabric' AS [ComputeName]
    ,[DestinationRawTable] AS [InputRawTable]
    , (CASE [EntityName]
			WHEN 'Purchasing.PurchaseOrders' THEN 'PurchaseOrderID' 
			WHEN 'Purchasing.SupplierTransactions' THEN 'SupplierTransactionID'
			WHEN 'Sales.CustomerTransactions' THEN 'CustomerTransactionID'
			WHEN 'Sales.Orders' THEN 'OrderID'
			ELSE NULL 
		END) AS [LookupColumns]
    , 'silver.Mirror_'+ Replace([EntityName],'.','_')  [OutputDWTable]
    ,(CASE WHEN [WatermarkColName] IS NOT NULL THEN 'append' ELSE 'overwrite' END) AS [OutputDWTableWriteMode]
    ,'Tables' AS [OutputL1CurateFileSystem]
    ,SUBSTRING([EntityName],1,(CHARINDEX('.', [EntityName])-1)) AS [OutputL1CuratedFolder]
    ,SUBSTRING([EntityName], (CHARINDEX('.',[EntityName])+1)) AS [OutputL1CuratedFile]    
    , [WatermarkColName]
    , 1 AS [ActiveFlag]
    FROM  [ELT].[IngestDefinition]
    WHERE [SourceSystemName]='WWI-mirror'
    AND [ActiveFlag]=1;

    --Merge with Temp table for re-runnability
    MERGE INTO [ELT].[L1TransformDefinition] AS TARGET
    USING #ASQLMirror_L1TransformDefinition AS SOURCE
    ON TARGET.OutputDWTable = SOURCE.OutputDWTable
    AND TARGET.InputRawTable = SOURCE.InputRawTable
    WHEN MATCHED THEN 
        UPDATE SET 
            TARGET.IngestID = SOURCE.IngestID,
            TARGET.ComputeName = SOURCE.ComputeName,
            TARGET.InputRawTable = SOURCE.InputRawTable,
            TARGET.LookupColumns = SOURCE.LookupColumns,
            TARGET.OutputDWTable = SOURCE.OutputDWTable,
            TARGET.OutputDWTableWriteMode = SOURCE.OutputDWTableWriteMode,
            TARGET.OutputL1CurateFileSystem = SOURCE.OutputL1CurateFileSystem,
            TARGET.OutputL1CuratedFolder = SOURCE.OutputL1CuratedFolder,
            TARGET.OutputL1CuratedFile = SOURCE.OutputL1CuratedFile,
            TARGET.WatermarkColName = SOURCE.WatermarkColName,
            TARGET.ActiveFlag = SOURCE.ActiveFlag
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (IngestID, 
                ComputeName, 
                InputRawTable, 
                LookupColumns, 
                OutputDWTable, 
                OutputDWTableWriteMode,
                OutputL1CurateFileSystem, 
                OutputL1CuratedFolder, 
                OutputL1CuratedFile, 
                WatermarkColName, 
                ActiveFlag)
        VALUES (SOURCE.IngestID, 
                SOURCE.ComputeName,
                SOURCE.InputRawTable,
                SOURCE.LookupColumns,
                SOURCE.OutputDWTable,
                SOURCE.OutputDWTableWriteMode,
                SOURCE.OutputL1CurateFileSystem,
                SOURCE.OutputL1CuratedFolder,
                SOURCE.OutputL1CuratedFile,
                SOURCE.WatermarkColName,
                SOURCE.ActiveFlag);