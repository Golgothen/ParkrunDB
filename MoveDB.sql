alter database tempdb modify file (NAME = tempdev, FILENAME = 'C:\SQLServer\tempdb.mdf');
go
alter database tempdb modify file (NAME = templog, FILENAME = 'C:\SQLServer\templog.ldf');
go
