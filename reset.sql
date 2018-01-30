use Parkrun;
delete from EventPositions;
delete from Events;
delete from Athletes WHERE AthleteID > 0;
DBCC CHECKIDENT ('Events', RESEED, 0);
GO