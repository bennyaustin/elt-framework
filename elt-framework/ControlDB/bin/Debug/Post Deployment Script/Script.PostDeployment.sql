/*
Post-Deployment Script Template							
--------------------------------------------------------------------------------------
 This file contains SQL statements that will be appended to the build script.		
 Use SQLCMD syntax to include a file in the post-deployment script.			
 Example:      :r .\myfile.sql								
 Use SQLCMD syntax to reference a variable in the post-deployment script.		
 Example:      :setvar TableName MyTable							
               SELECT * FROM [$(TableName)]					
--------------------------------------------------------------------------------------
*/

--IngestDefinition
:r .\datasources\RestAPI\IngestDefinition\PurviewRestAPI.sql
:r .\datasources\RestAPI\IngestDefinition\AzureRestAPI.sql
:r .\datasources\UploadFiles\IngestDefinition\CustomFormRecognizerModel.sql

--L1TransformDefinition
:r .\datasources\RestAPI\L1TransformDefinition\L1T_PurviewRestAPI.sql
:r .\datasources\RestAPI\L1TransformDefinition\L1T_AzureRestAPI.sql
:r .\datasources\UploadFiles\L1TransformDefinition\L1T_CustomFormRecognizerModel.sql
