import pyodbc

xstr = lambda s: 'NULL' if s is None else str(s)

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
        #self.cachedAthlete = None
        self.cachedClub = None
    
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

    def getClub(self, club):
        if self.cachedClub is None:
            self.cachedClub = {}
            c = self.execute("SELECT ClubID, ClubName from Clubs")
            for row in c.fetchall():
                self.cachedClub[row[1]] = row[0]
        if club is None: return None
        if club not in self.cachedClub:
            sql = "INSERT INTO Clubs (ClubName) VALUES ('" + club + "')"
            c = self.execute(sql)
            sql = "SELECT SCOPE_IDENTITY()"
            c = self.execute(sql)
            id = int(c.fetchone()[0])
            c.commit()
            self.cachedClub[club] = id
            return id
        else:
            return self.cachedClub[club]
            
    def addAthlete(self, athlete):
        sql = "SELECT AthleteID FROM Athletes WHERE AthleteID = " + str(athlete['AthleteID'])
        c = self.execute(sql)
        if c.rowcount == 0:
            try:
                sql = "INSERT INTO Athletes (AthleteID, FirstName, LastName, AgeCategoryID, Gender"
                values = " VALUES (" + xstr(athlete['AthleteID']) + ", '" + xstr(athlete['FirstName']) + "', '" + xstr(athlete['LastName']) + "', '"  + xstr(self.getAgeCatID(athlete['Age Cat'])) + ", '" + xstr(athlete['Gender']) 
                if athlete['Club'] is not None:
                    sql += ", ClubID"
                    values += "', " + xstr(self.getClu(bathlete['Club']))
                if athlete['StravaID'] is not None:
                    sql += ", StravaID"
                    values += "', '" + xstr(athlete['StravaID'])
                sql += ")" + values + ")"
                c = self.execute(sql)
                c.commit()
            except pyodbc.Error as e:
                c.rollback()
                if e.args[0] == 23000:
                    # On rare occasions, an athlete can be entered by another thread/process at the same time, causing a key violation.
                    return
                else:
                    raise
        else:
            r = c.fetchall()
            sql = "UPDATE Athletes SET AgeCategoryID = " + xstr(self.getAgeCatID(athlete['Age Cat'])) + " WHERE AthleteID = " + xstr(athlete['AthleteID'])
            c = self.execute(sql)
            sql = "UPDATE Athletes SET ClubID = " + xstr(self.getClub(athlete['Club'])) + " WHERE AthleteID = " + xstr(athlete['AthleteID'])
            c = self.execute(sql)
            c.commit()
            
    def addParkrunEventPosition(self, position):
        self.addAthlete(position)
        sql = "INSERT INTO EventPositions (EventID, AthleteID, Position"
        values = " VALUES (" + xstr(position['EventID']) + ", " + xstr(position['AthleteID']) + ", " + xstr(position['Pos'])

        if position['Time'] is not  None:
            sql += ", GunTime" 
            values += ", CAST('" + xstr(position['Time']) + "' as time(0))"
        if position['Age Cat'] is not  None:
            sql += ", AgeCategoryID" 
            values += ", " + xstr(self.getAgeCatID(position['Age Cat']))
        if position['Age Grade'] is not  None:
            sql += ", AgeGrade" 
            values += ", " + xstr(position['Age Grade'])
        if position['Note'] is not  None:
            sql += ", Comment" 
            values +=  ", '" + xstr(position['Note']) + "'"
        sql += ")" + values + ")"
        
        c = self.execute(sql)
        c.commit()
    
    def updateParkrunURL(self, parkrun, verified, valid):
        sql = "UPDATE Parkruns SET URLVerified = " + str(int(verified)) + ", URLValid = " + str(int(valid)) + " WHERE ParkrunName = '" + parkrun + "'"
        c = self.execute(sql)
        c.commit()
        
    def close(self):
        self.conn.close()
        del self.conn
