ALTER DATABASE parkrun
SET RECOVERY SIMPLE
GO
DBCC SHRINKFILE (parkrun_log, 1)
GO
ALTER DATABASE parkrun
SET RECOVERY FULL