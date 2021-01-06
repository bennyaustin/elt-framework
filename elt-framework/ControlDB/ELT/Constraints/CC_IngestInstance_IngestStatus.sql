ALTER TABLE [ELT].[IngestInstance]
	ADD CONSTRAINT [CC_IngestInstance_IngestStatus]
	CHECK ([IngestStatus]='ReRunFailure' OR [IngestStatus]='ReRunSuccess' OR [IngestStatus]='Running' OR [IngestStatus]='Failure' OR [IngestStatus]='Success')