ALTER TABLE [ELT].[L2TransformInstance]
	ADD CONSTRAINT [CC_L2TransformInstance_L2TransformStatus]
	CHECK ([L2TransformStatus] ='ReRunFailure' OR [L2TransformStatus] ='ReRunSuccess' OR [L2TransformStatus] ='Running' OR [L2TransformStatus] ='DWUpload' OR [L2TransformStatus]='Failure' OR [L2TransformStatus]='Success')
