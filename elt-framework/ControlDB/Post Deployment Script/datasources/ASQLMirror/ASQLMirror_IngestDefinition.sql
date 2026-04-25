IF OBJECT_ID('tempdb..#ASQLMirror_IngestDefinition') IS NOT NULL DROP TABLE #ASQLMirror_IngestDefinition;

--Create Temp table with same structure as IngestDefinition
CREATE TABLE #ASQLMirror_IngestDefinition
(
	[SourceSystemName] [varchar](50) NOT NULL,
	[StreamName] [varchar](100) NULL,
	[SourceSystemDescription] [varchar](200) NULL,
	[Backend] [varchar](30) NULL,
	[EntityName] [varchar](100) NULL,
	[WatermarkColName] [varchar](50) NULL,
	[LastDeltaDate] [datetime2](7) NULL,
    [MaxIntervalMinutes] [int] NULL,
    [DestinationRawTable] [varchar](200) NULL,
	[ActiveFlag] [bit] NOT NULL,
	[L1TransformationReqdFlag] [bit] NOT NULL,
	[L2TransformationReqdFlag] [bit] NOT NULL,
	[DelayL1TransformationFlag] [bit] NOT NULL,
	[DelayL2TransformationFlag] [bit] NOT NULL
);

--Application.PaymentMethods
INSERT INTO #ASQLMirror_IngestDefinition
SELECT 'WWI-mirror' AS [SourceSystemName],
    'PaymentMethods' AS [StreamName],
    'Azure SQL Mirror Source System' AS [SourceSystemDescription],
    'ASQLMirror' AS [Backend],
    'Application.PaymentMethods' AS [EntityName],
    NULL AS [WatermarkColName],
    NULL AS [LastDeltaDate],
    NULL AS [MaxIntervalMinutes], 
    'Application.PaymentMethods' AS [DestinationRawTable],
    1 AS [ActiveFlag],
    1 AS [L1TransformationReqdFlag],
    0 AS [L2TransformationReqdFlag],
    0 AS [DelayL1TransformationFlag],
    0 AS [DelayL2TransformationFlag]

UNION
--Application.People
SELECT 'WWI-mirror' AS [SourceSystemName],
    'People' AS [StreamName],
    'Azure SQL Mirror Source System' AS [SourceSystemDescription],
    'ASQLMirror' AS [Backend],
    'Application.People' AS [EntityName],
    NULL AS [WatermarkColName],
    NULL AS [LastDeltaDate],
    NULL AS [MaxIntervalMinutes],
    'Application.People' AS [DestinationRawTable],
    1 AS [ActiveFlag],
    1 AS [L1TransformationReqdFlag],
    0 AS [L2TransformationReqdFlag],
    0 AS [DelayL1TransformationFlag],
    0 AS [DelayL2TransformationFlag]

UNION
--Application.TransactionTypes
SELECT 'WWI-mirror' AS [SourceSystemName],
    'TransactionTypes' AS [StreamName],
    'Azure SQL Mirror Source System' AS [SourceSystemDescription],
    'ASQLMirror' AS [Backend],
    'Application.TransactionTypes' AS [EntityName],
    NULL AS [WatermarkColName],
    NULL AS [LastDeltaDate],
    NULL AS [MaxIntervalMinutes],
    'Application.TransactionTypes' AS [DestinationRawTable],
    1 AS [ActiveFlag],
    1 AS [L1TransformationReqdFlag],
    0 AS [L2TransformationReqdFlag],
    0 AS [DelayL1TransformationFlag],
    0 AS [DelayL2TransformationFlag] 

UNION
--Purchasing.PurchaseOrders
SELECT 'WWI-mirror' AS [SourceSystemName],
    'PurchaseOrders' AS [StreamName],
    'Azure SQL Mirror Source System' AS [SourceSystemDescription],
    'ASQLMirror' AS [Backend],
    'Purchasing.PurchaseOrders' AS [EntityName],
    'LastEditedWhen' AS [WatermarkColName],
    '2013-01-02' AS [LastDeltaDate],
     1440 AS [MaxIntervalMinutes], --1 day interval
    'Purchasing.PurchaseOrders' AS [DestinationRawTable],
    1 AS [ActiveFlag],
    1 AS [L1TransformationReqdFlag],
    1 AS [L2TransformationReqdFlag],
    0 AS [DelayL1TransformationFlag],
    0 AS [DelayL2TransformationFlag]    

UNION
--Purchasing.SupplierTransactions
SELECT 'WWI-mirror' AS [SourceSystemName],
    'SupplierTransactions' AS [StreamName],
    'Azure SQL Mirror Source System' AS [SourceSystemDescription],
    'ASQLMirror' AS [Backend],
    'Purchasing.SupplierTransactions' AS [EntityName],
    'LastEditedWhen' AS [WatermarkColName],
    '2013-01-07' AS [LastDeltaDate],
     1440 AS [MaxIntervalMinutes], --1 day interval
    'Purchasing.SupplierTransactions' AS [DestinationRawTable],
    1 AS [ActiveFlag],
    1 AS [L1TransformationReqdFlag],
    1 AS [L2TransformationReqdFlag],
    0 AS [DelayL1TransformationFlag],
    0 AS [DelayL2TransformationFlag]       

UNION
--Sales.CustomerTransactions
SELECT 'WWI-mirror' AS [SourceSystemName],
    'CustomerTransactions' AS [StreamName],
    'Azure SQL Mirror Source System' AS [SourceSystemDescription],
    'ASQLMirror' AS [Backend],
    'Sales.CustomerTransactions' AS [EntityName],
    'LastEditedWhen' AS [WatermarkColName],
    '2013-01-02' AS [LastDeltaDate],
     1440 AS [MaxIntervalMinutes], --1 day interval
    'Sales.CustomerTransactions' AS [DestinationRawTable],
    1 AS [ActiveFlag],
    1 AS [L1TransformationReqdFlag],
    1 AS [L2TransformationReqdFlag],
    0 AS [DelayL1TransformationFlag],
    0 AS [DelayL2TransformationFlag]          

UNION
--Sales.Orders
SELECT 'WWI-mirror' AS [SourceSystemName],
    'Orders' AS [StreamName],
    'Azure SQL Mirror Source System' AS [SourceSystemDescription],
    'ASQLMirror' AS [Backend],
    'Sales.Orders' AS [EntityName],
    'LastEditedWhen' AS [WatermarkColName],
    '2013-01-01' AS [LastDeltaDate],
     1440 AS [MaxIntervalMinutes], --1 day interval
    'Sales.Orders' AS [DestinationRawTable],
    1 AS [ActiveFlag],
    1 AS [L1TransformationReqdFlag],
    1 AS [L2TransformationReqdFlag],
    0 AS [DelayL1TransformationFlag],
    0 AS [DelayL2TransformationFlag]           
;

--Merge with IngestDefinition for re-runnability
MERGE INTO [ELT].[IngestDefinition] AS tgt
USING #ASQLMirror_IngestDefinition AS src
ON src.[SourceSystemName]=tgt.[SourceSystemName] AND src.[StreamName]=tgt.[StreamName]
WHEN MATCHED THEN
    UPDATE SET tgt.[SourceSystemDescription] = src.[SourceSystemDescription],
               tgt.[Backend] = src.[Backend],
               tgt.[EntityName] = src.[EntityName],
               tgt.[WatermarkColName] = src.[WatermarkColName],
               tgt.[LastDeltaDate] = src.[LastDeltaDate],
               tgt.[MaxIntervalMinutes] = src.[MaxIntervalMinutes],
               tgt.[DestinationRawTable] = src.[DestinationRawTable],
               tgt.[ActiveFlag] = src.[ActiveFlag],
               tgt.[L1TransformationReqdFlag]  = src.[L1TransformationReqdFlag],
               tgt.[L2TransformationReqdFlag] = src.[L2TransformationReqdFlag],
               tgt.[DelayL1TransformationFlag] = src.[DelayL1TransformationFlag],
               tgt.[DelayL2TransformationFlag] = src.[DelayL2TransformationFlag],
               tgt.[ModifiedBy] = USER_NAME(),
               tgt.[ModifiedTimestamp] = GetDate()
WHEN NOT MATCHED BY TARGET THEN
    INSERT(	[SourceSystemName],
            [StreamName],
            [SourceSystemDescription],
            [Backend],
            [EntityName],
            [WatermarkColName],
            [LastDeltaDate],
            [MaxIntervalMinutes],
            [DestinationRawTable],
            [ActiveFlag],
            [L1TransformationReqdFlag],
            [L2TransformationReqdFlag],
            [DelayL1TransformationFlag],
            [DelayL2TransformationFlag],
            [ModifiedBy],
            [ModifiedTimestamp])
    VALUES (src.[SourceSystemName],
            src.[StreamName],
            src.[SourceSystemDescription],
            src.[Backend],
            src.[EntityName],
            src.[WatermarkColName],
            src.[LastDeltaDate],
            src.[MaxIntervalMinutes],
            src.[DestinationRawTable],			
            src.[ActiveFlag],
            src.[L1TransformationReqdFlag],
            src.[L2TransformationReqdFlag],
            src.[DelayL1TransformationFlag],
            src.[DelayL2TransformationFlag],			
            USER_NAME(),
            GetDate());