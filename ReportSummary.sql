select top 10 ParkrunName, ThisWeek from qryWeeklyParkrunEventSize where Region = 'Victoria' order by ThisWeek desc
select top 10 ParkrunName, ThisWeek/Average from qryWeeklyParkrunEventSize where Region = 'Victoria' order by (ThisWeek/Average) desc
select top 10 ParkrunName, ThisWeek/LastWeek from qryWeeklyParkrunEventSize where Region = 'Victoria' order by (ThisWeek/LastWeek) desc
--select top 1 ParkrunName, ThisWeek from qryWeeklyParkrunEventSize where Region = 'Victoria'
--select top 1 ParkrunName, ThisWeek from qryWeeklyParkrunEventSize where Region = 'Victoria'
--select top 1 ParkrunName, ThisWeek from qryWeeklyParkrunEventSize where Region = 'Victoria'
