ALTER TABLE [ELT].[IngestDefinition]
	ADD CONSTRAINT [DC_IngestDefinition_DelayL1TransformationFlag]
	DEFAULT 1
	FOR [DelayL1TransformationFlag]
