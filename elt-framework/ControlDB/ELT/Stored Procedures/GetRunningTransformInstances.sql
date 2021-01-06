CREATE PROCEDURE [ELT].[GetRunningTransformInstances]
	@timespanHrs int = 18 --Default Timespan 18hrs
AS
	--Get the count of transform instances (L1 and L2) still active and running in last @timespanHrs
	DECLARE @L1Count INT, @L2Count INT

	IF @timespanHrs IS NULL OR @timespanHrs<=0 
		SET @timespanHrs=24

	SELECT @L1Count = COUNT(1)
	FROM ELT.L1TransformInstance
	WHERE L1TransformStatus='Running' 
	AND DateDiff(hour,L1TransformStartTimestamp,getdate()) <= @timespanHrs

	SELECT @L2Count= COUNT(1)
	FROM ELT.L2TransformInstance
	WHERE L2TransformStatus='Running'
	AND DateDiff(hour,L2TransformStartTimestamp,getdate()) <= @timespanHrs

	SELECT (@L1Count + @L2Count) AS RunningCount

RETURN
