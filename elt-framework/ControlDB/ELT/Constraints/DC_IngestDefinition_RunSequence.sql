ALTER TABLE [ELT].[IngestDefinition] 
	ADD CONSTRAINT [DC_IngestDefinition_RunSequence] 
	DEFAULT 100 
	FOR [RunSequence] 
