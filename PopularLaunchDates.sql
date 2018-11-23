
with launches (EventNumber, LaunchDate) AS (
	SELECT Events.EventNumber, Events.EventDate
		FROM Events INNER JOIN Parkruns ON Events.ParkrunID = Parkruns.ParkrunID
		WHERE Events.EventNumber=1
)
,
MostPopularLaunchDates (LaunchDate, LaunchCount) AS (
	SELECT LaunchDate, Count(EventNumber) AS LaunchCount
		FROM launches
		GROUP BY LaunchDate
		--ORDER BY Count(EventNumber) DESC
)

SELECT LaunchCount, MostPopularLaunchDates.LaunchDate, Country, Region, ParkrunName
	FROM Countries 
		INNER JOIN MostPopularLaunchDates 
			INNER JOIN Events 
				INNER JOIN Parkruns ON Events.ParkrunID = Parkruns.ParkrunID
			ON MostPopularLaunchDates.LaunchDate = Events.EventDate
			INNER JOIN Regions ON Parkruns.RegionID = Regions.RegionID
		ON Countries.CountryID = Regions.CountryID
	WHERE Events.EventNumber=1
	ORDER BY MostPopularLaunchDates.LaunchCount DESC, MostPopularLaunchDates.LaunchDate DESC , Countries.Country, Regions.Region, Parkruns.ParkrunName

