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
            if sql[:6].upper() == "SELECT":
                data = []
                headings = []
                c = self.conn.execute(sql)
                if c.description is not None:
                    for h in c.description:
                        headings.append(h[0])
                    for row in c.fetchall():
                        d = {}
                        for h, v in zip(headings, row):
                            d[h]=v
                        data.append(d)
                    return data
                else:
                    return None
            if sql[:6].upper() == "INSERT":
                c = self.conn.execute(sql)
                c = self.conn.execute("SELECT SCOPE_IDENTITY()")
                if c.rowcount != 0:
                    t = c.fetchone()[0]
                    if t is not None:
                        data = int(t)
                    else:
                        data=None
                else:
                    data=None
                c.commit()
                return data
            if sql[:6].upper() in ["DELETE", "UPDATE"]:
                c = self.conn.execute(sql)
                c.commit()
                return None
        except:
            if sql[:6] in ['INSERT', 'DELETE', 'UPDATE']:
                c.rollback()
            raise
        
    def getParkrunID(self, parkrunName):
        if self.cachedParkrun is None:
            data = self.execute("SELECT ParkrunID, ParkrunName FROM Parkruns")
            self.cachedParkrun = {}
            for row in data:
                self.cachedParkrun[row['ParkrunName']] = row['ParkrunID'] 
        return self.cachedParkrun[parkrunName]

    def getAgeCatID(self, AgeCategory):
        if AgeCategory is None:
            return 1
        if self.cachedAgeCat is None:
            data = self.execute("SELECT AgeCategoryID, AgeCategory FROM AgeCategories")
            self.cachedAgeCat = {}
            for row in data:
                self.cachedAgeCat[row['AgeCategory']] = row['AgeCategoryID']
        return self.cachedAgeCat[AgeCategory]
    
    def addParkrunEvent(self, parkrun):
        return self.execute("INSERT INTO Events (ParkrunID, EventNumber, EventDate) VALUES (" + str(self.getParkrunID(parkrun['Name'])) + ", " + str(parkrun['EventNumber']) + ", CAST('" + parkrun['EventDate'].strftime('%Y-%m-%d') + "' AS date))")

    def replaceParkrunEvent(self, row):
        data = self.execute("SELECT EventID FROM getEventID('" + row['Name'] + "', " + xstr(row['EventNumber']) + ")")
        if len(data) != 0:
            c = self.execute("DELETE FROM Events WHERE EventID = " + xstr(data[0]['EventID']))
        return self.execute("INSERT INTO Events (ParkrunID, EventNumber, EventDate) VALUES (" + str(self.getParkrunID(row['Name'])) + ", " + str(row['EventNumber']) + ", CAST('" + row['EventDate'].strftime('%Y-%m-%d') + "' AS date))")

    def checkParkrunEvent(self, row):
        data = self.execute("SELECT Runners FROM getParkrunEventRunners('" + row['Name'] + "', " + xstr(row['EventNumber']) + ")")
        if len(data) == 0:
            r =  0
        else:
            r = data[0]['Runners']
        return r == row['Runners']

    def getClub(self, club):
        if self.cachedClub is None:
            data = self.execute("SELECT ClubID, ClubName from Clubs")
            self.cachedClub = {}
            for row in data:
                self.cachedClub[row['ClubName']] = row['ClubID']
        if club is None: return None
        if club not in self.cachedClub:
            id = self.execute("INSERT INTO Clubs (ClubName) VALUES ('" + club + "')")
            self.cachedClub[club] = id
            return id
        else:
            return self.cachedClub[club]
            
    def addAthlete(self, athlete):
        data = self.execute("SELECT AthleteID, AgeCategoryID, ClubID FROM Athletes WHERE AthleteID = " + str(athlete['AthleteID']))
        if len(data) == 0:
            try:
                sql = "INSERT INTO Athletes (AthleteID, FirstName, LastName, AgeCategoryID, Gender"
                values = " VALUES (" + xstr(athlete['AthleteID']) + ", '" + xstr(athlete['FirstName']) + "', '" + xstr(athlete['LastName']) + "', "  + xstr(self.getAgeCatID(athlete['Age Cat'])) + ", '" + xstr(athlete['Gender']) + "'" 
                if athlete['Club'] is not None:
                    sql += ", ClubID"
                    values += ", " + xstr(self.getClub(athlete['Club']))
                if athlete['StravaID'] is not None:
                    sql += ", StravaID"
                    values += ", '" + xstr(athlete['StravaID']) + "'"
                sql += ")" + values + ")"
                self.execute(sql)
            except pyodbc.Error as e:
                if e.args[0] == 23000:
                    # On rare occasions, an athlete can be entered by another thread/process at the same time, causing a key violation.
                    return
                else:
                    raise
        else:
            if data[0]['AgeCategoryID'] != self.getAgeCatID(athlete['Age Cat']):
                self.execute("UPDATE Athletes SET AgeCategoryID = " + xstr(self.getAgeCatID(athlete['Age Cat'])) + " WHERE AthleteID = " + xstr(athlete['AthleteID']))
            if data[0]['ClubID'] != self.getClub(athlete['Club']):
                self.execute("UPDATE Athletes SET ClubID = " + xstr(self.getClub(athlete['Club'])) + " WHERE AthleteID = " + xstr(athlete['AthleteID']))
            
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
        self.execute(sql)
    
    def updateParkrunURL(self, parkrun, verified, valid):
        self.execute("UPDATE Parkruns SET URLVerified = " + str(int(verified)) + ", URLValid = " + str(int(valid)) + " WHERE ParkrunName = '" + parkrun + "'")
        
    def close(self):
        self.conn.close()
        del self.conn
