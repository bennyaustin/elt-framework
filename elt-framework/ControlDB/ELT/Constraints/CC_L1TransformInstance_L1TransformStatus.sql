ALTER TABLE [ELT].[L1TransformInstance]
	ADD CONSTRAINT [CC_L1TransformInstance_L1TransformStatus]
	CHECK ([L1TransformStatus] ='ReRunFailure' OR [L1TransformStatus] ='ReRunSuccess' OR [L1TransformStatus] ='Running' OR [L1TransformStatus] ='DWUpload' OR [L1TransformStatus]='Failure' OR [L1TransformStatus]='Success')
