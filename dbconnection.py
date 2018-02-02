import pyodbc

xstr = lambda s: '' if s is None else str(s)

class Connection():
    def __init__(self):

        server = 'localhost'
        database = 'Parkrun'
        userstring = 'Trusted_Connection=yes'
        driver = 'SQL Server Native Client 11.0'
        self.connectString = 'DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';'+userstring
        pyodbc.pooling = False

        self.conn = pyodbc.connect(self.connectString)

        self.cachedParkrun = None
        self.cachedAgeCat = None
    
    def execute(self, sql):
        try:
            return self.conn.execute(sql)
        except:
            raise
        
    def getParkrunID(self, parkrunName):
        if self.cachedParkrun is None:
            self.cachedParkrun = {}
            c = self.execute("SELECT ParkrunID, ParkrunName FROM Parkruns")
            for row in c.fetchall():
                self.cachedParkrun[row[1]] = row[0]
        return self.cachedParkrun[parkrunName]

    def getAgeCatID(self, AgeCategory):
        if AgeCategory is None:
            return 1
        if self.cachedAgeCat is None:
            self.cachedAgeCat = {}
            c = self.execute("SELECT AgeCategoryID, AgeCategory FROM AgeCategories")
            for row in c.fetchall():
                self.cachedAgeCat[row[1]] = row[0]
        return self.cachedAgeCat[AgeCategory]
    
    def addParkrunEvent(self, parkrun):
        sql = "INSERT INTO Events (ParkrunID, EventNumber, EventDate) VALUES (" + str(self.getParkrunID(parkrun['Name'])) + ", " + str(parkrun['EventNumber']) + ", CAST('" + parkrun['EventDate'].strftime('%Y-%m-%d') + "' AS date))"
        c = self.execute(sql)
        sql = "SELECT SCOPE_IDENTITY()"
        c = self.execute(sql)
        id = int(c.fetchone()[0])
        c.commit()
        return id

    def replaceParkrunEvent(self, row):
        sql = "SELECT EventID FROM getEventID('" + row['Name'] + "', " + xstr(row['EventNumber']) + ")"
        c = self.execute(sql)
        if c.rowcount != 0:
            id = int(c.fetchone()[0])
            sql = "DELETE FROM Events WHERE EventID = " + xstr(id)
            c = self.execute(sql)
            c.commit()
        sql = "INSERT INTO Events (ParkrunID, EventNumber, EventDate) VALUES (" + str(self.getParkrunID(row['Name'])) + ", " + str(row['EventNumber']) + ", CAST('" + row['EventDate'].strftime('%Y-%m-%d') + "' AS date))"
        c = self.execute(sql)
        sql = "SELECT SCOPE_IDENTITY()"
        c = self.execute(sql)
        id = int(c.fetchone()[0])
        c.commit()
        return id

    def checkParkrunEvent(self, row):
        sql = "SELECT Runners FROM getParkrunEventRunners('" + row['Name'] + "', " + xstr(row['EventNumber']) + ")"
        c = self.execute(sql)
        if c.rowcount == 0:
            r = 0
        else:
            r = c.fetchone()[0]
        return r == row['Runners']

    def addAthlete(self, athlete):
        sql = "SELECT AthleteID FROM Athletes WHERE AthleteID = " + str(athlete['AthleteID'])
        c = self.execute(sql)
        if c.rowcount == 0:
            try:
                sql = "INSERT INTO Athletes (AthleteID, FirstName, LastName, Club, AgeCategoryID, Gender, StravaID) VALUES (" + xstr(athlete['AthleteID']) + ", '" + xstr(athlete['FirstName']) + "', '" + xstr(athlete['LastName']) + "', '" + xstr(athlete['Club']) + "', " + xstr(self.getAgeCatID(athlete['Age Cat'])) + ", '" + xstr(athlete['Gender']) + "', '" + xstr(athlete['StravaID']) + "')"
                c = self.execute(sql)
                c.commit()
            except pyodbc.Error as e:
                c.rollback()
                if e.args[0] == 23000:
                    # On rare occasions, an athlete can be entered by another thread/process at the same time, causing a key violation.
                    return
                else:
                    raise
            
    def addParkrunEventPosition(self, position):
        self.addAthlete(position)
        sql = "INSERT INTO EventPositions (EventID, AthleteID, Position, GunTime, AgeCategoryID, AgeGrade, Comment) VALUES (" + xstr(position['EventID']) + ", " + xstr(position['AthleteID']) + ", " + xstr(position['Pos']) + ", CAST('" + xstr(position['Time']) + "' as time(0)), " + xstr(self.getAgeCatID(position['Age Cat'])) + ", " + xstr(position['Age Grade']) + ", '" + xstr(position['Note']) + "')" 
        c = self.execute(sql)
        c.commit()
    
    def updateParkrunURL(self, parkrun, verified, valid):
        sql = "UPDATE Parkruns SET URLVerified = " + str(int(verified)) + ", URLValid = " + str(int(valid)) + " WHERE ParkrunName = '" + parkrun + "'"
        c = self.execute(sql)
        c.commit()
        
    def close(self):
        self.conn.close()
        del self.conn
