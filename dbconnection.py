import pyodbc, logging, logging.config

xstr = lambda s: 'NULL' if s is None else str(s)

class Connection():
    def __init__(self, config):

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
        self.config = config
        logging.config.dictConfig(config)
        self.logger = logging.getLogger(__name__)

    
    def execute(self, sql):
        self.logger.debug(sql)
        c = None
        try:
            if sql[:6].upper() == "SELECT":
                if "FROM" in sql.upper():
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
                else:
                    c = self.conn.execute(sql)
            if sql[:6].upper() == "INSERT":
                c = self.conn.execute(sql)
                c = self.conn.execute("SELECT SCOPE_IDENTITY()")
                if c.rowcount != 0:
                    t = c.fetchone()[0]
                    if t is not None:
                        data = int(t)
                        self.logger.debug('INSERT returned SCOPE_IDENTITY() {}'.format(int(t)))
                    else:
                        data=None
                        self.logger.debug('INSERT returned no SCOPE_IDENTITY()')
                else:
                    data=None
                c.commit()
                return data
            if sql[:6].upper() in ["DELETE", "UPDATE"]:
                c = self.conn.execute(sql)
                c.commit()
                return None
        except:
            self.logger.error('Error occured executing statement',exc_info = True, stack_info = True)
            if sql[:6] in ['INSERT', 'DELETE', 'UPDATE']:
                if c is not None:
                    self.logger.error('Rolling back previous statement')
                    c.rollback()
            raise
        
    def getParkrunID(self, parkrunName):
        if self.cachedParkrun is None:
            data = self.execute("SELECT ParkrunID, ParkrunName FROM Parkruns")
            self.cachedParkrun = {}
            for row in data:
                self.cachedParkrun[row['ParkrunName']] = row['ParkrunID']
            self.logger.debug('Added {} records to Parkrun cache'.format(len(self.cachedParkrun))) 
        return self.cachedParkrun[parkrunName]

    def getAgeCatID(self, athlete):
        if athlete['Age Cat'] is None:
            return 1
        if self.cachedAgeCat is None:
            data = self.execute("SELECT AgeCategoryID, AgeCategory FROM AgeCategories")
            self.cachedAgeCat = {}
            for row in data:
                self.cachedAgeCat[row['AgeCategory']] = row['AgeCategoryID']
            self.logger.debug('Added {} records to Age Category cache'.format(len(self.cachedAgeCat))) 
        if athlete['Age Cat'] in self.cachedAgeCat:
            return self.cachedAgeCat[athlete['Age Cat']]
        else:
            ageGroup = athlete['Age Cat'][2:]
            if ageGroup == '---':
                ageCat = 'S'
            elif ageGroup == 'C':
                ageCat = ''
                ageGroup = 'WC'
            else:
                startage = int(ageGroup.split('-')[0])
                if startage in [10, 11, 15]:
                    ageCat = 'J'
                if startage in [18, 20, 25, 30]:
                    ageCat = 'S'
                if startage > 30:
                    ageCat = 'V'
            if athlete['Gender'] == 'M':
                ageCat += 'M' + ageGroup
            else:
                ageCat += 'W' + ageGroup
            self.logger.debug('Improvised AgeCat lookup.  Came up with {} from {}'.format(ageCat, athlete['Age Cat']))
            return self.cachedAgeCat[ageCat]
    
    def addParkrunEvent(self, parkrun):
        return self.execute("INSERT INTO Events (ParkrunID, EventNumber, EventDate) VALUES (" + str(self.getParkrunID(parkrun['Name'])) + ", " + str(parkrun['EventNumber']) + ", CAST('" + parkrun['EventDate'].strftime('%Y-%m-%d') + "' AS date))")

    def replaceParkrunEvent(self, row):
        data = self.execute("SELECT EventID FROM getEventID('" + row['Name'] + "', " + xstr(row['EventNumber']) + ")")
        if len(data) != 0:
            self.execute("DELETE FROM Events WHERE EventID = " + xstr(data[0]['EventID']))
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
            self.logger.debug('Added {} records to Club cache'.format(len(self.cachedClub))) 
        if club is None: return None
        if club not in self.cachedClub:
            c_id = self.execute("INSERT INTO Clubs (ClubName) VALUES ('" + club + "')")
            self.cachedClub[club] = c_id
            return c_id
        else:
            return self.cachedClub[club]
            
    def addAthlete(self, athlete):
        data = self.execute("SELECT AthleteID, FirstName, LastName, AgeCategoryID, ClubID FROM Athletes WHERE AthleteID = " + str(athlete['AthleteID']))
        if len(data) == 0:
            try:
                sql = "INSERT INTO Athletes (AthleteID, FirstName, LastName, AgeCategoryID, Gender"
                values = " VALUES (" + xstr(athlete['AthleteID']) + ", '" + xstr(athlete['FirstName'][:50]) + "', '" + xstr(athlete['LastName'][:50]) + "', "  + xstr(self.getAgeCatID(athlete)) + ", '" + xstr(athlete['Gender']) + "'" 
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
                    self.logger.error('Error adding athlete {}'.format(athlete), exec_info = True, stack_info = True)
                    raise
        else:
            if athlete['AthleteID'] != 0:
                if data[0]['AgeCategoryID'] != self.getAgeCatID(athlete) or \
                   data[0]['ClubID'] != self.getClub(athlete['Club']) or \
                   data[0]['FirstName'][:50] != athlete['FirstName'][:50] or \
                   data[0]['LastName'][:50] != athlete['LastName'][:50]:
                    self.execute("UPDATE Athletes SET AgeCategoryID = " + xstr(self.getAgeCatID(athlete)) + \
                                 ", ClubID = " + xstr(self.getClub(athlete['Club'])) + \
                                 ", FirstName = '" + athlete['FirstName'][:50] + "'" + \
                                 ", LastName = '" + athlete['LastName'][:50] + "'" + \
                                 " WHERE AthleteID = " + str(athlete['AthleteID']))
            
    def addParkrunEventPosition(self, position):
        self.addAthlete(position)
        sql = "INSERT INTO EventPositions (EventID, AthleteID, Position"
        values = " VALUES (" + xstr(position['EventID']) + ", " + xstr(position['AthleteID']) + ", " + xstr(position['Pos'])

        if position['Time'] is not  None:
            sql += ", GunTime" 
            values += ", CAST('" + xstr(position['Time']) + "' as time(0))"
        if position['Age Cat'] is not  None:
            sql += ", AgeCategoryID" 
            values += ", " + xstr(self.getAgeCatID(position))
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
