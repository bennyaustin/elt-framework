ALTER TABLE [ELT].[IngestDefinition]
	ADD CONSTRAINT [DC_IngestDefinition_DelayL2TransformationFlag]
	DEFAULT 1
	FOR [DelayL2TransformationFlag]
