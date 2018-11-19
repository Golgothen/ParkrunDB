-- ================================================
-- Template generated from Template Explorer using:
-- Create Inline Function (New Menu).SQL
--
-- Use the Specify Values for Template Parameters 
-- command (Ctrl-Shift-M) to fill in the parameter 
-- values below.
--
-- This block of comments will not be included in
-- the definition of the function.
-- ================================================
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE FUNCTION getWilsonIndex 
(	
	-- Add the parameters for the function here
	@AthleteID int
)
RETURNS TABLE 
AS
RETURN 
(
	SELECT Parkruns.ParkrunName, Max(EventNumber+1) AS NextEvent, Regions.Region, Countries.Country
		FROM Countries 
			INNER JOIN Events 
				INNER JOIN Parkruns ON Events.ParkrunID = Parkruns.ParkrunID 
				INNER JOIN Regions ON Parkruns.RegionID = Regions.RegionID 
			ON Countries.CountryID = Regions.CountryID
		GROUP BY Parkruns.ParkrunName, Regions.Region, Countries.Country
		HAVING Max(EventNumber+1) Not In 
			(SELECT Events.EventNumber
				FROM EventPositions INNER JOIN Events ON EventPositions.EventID = Events.EventID
			GROUP BY EventPositions.AthleteID, Events.EventNumber
			HAVING EventPositions.AthleteID=@AthleteID)
		AND Countries.Country='Australia'
)
GO
