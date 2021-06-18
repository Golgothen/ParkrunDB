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
        self.cachedVolunteer = None
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
                    return c.fetchall()[0][0]
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
            self.logger.error('Error occured executing statement: {}'.format(sql),exc_info = True, stack_info = True)
            if sql[:6] in ['INSERT', 'DELETE', 'UPDATE']:
                if c is not None:
                    self.logger.error('Rolling back previous statement')
                    c.rollback()
            raise
        
    def getParkrunID(self, parkrunName):
        return self.execute("SELECT dbo.getParkrunID('{}')".format(parkrunName))

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
            self.logger.info(athlete)
            ageGroup = athlete['Age Cat'][2:]
            if ageGroup == '---':
                ageCat = 'S'
            elif ageGroup == 'C':
                ageCat = ''
                ageGroup = 'WC'
            else:
                try:
                    startage = int(ageGroup.replace('+','').split('-')[0])
                    if startage in [10, 11, 15]:
                        ageCat = 'J'
                    if startage in [18, 20, 25, 30]:
                        ageCat = 'S'
                    if startage > 30:
                        ageCat = 'V'
                except:
                    ageCat = 'S'
                    ageGroup = '---'
            if athlete['Gender'] == 'M':
                ageCat += 'M' + ageGroup
            else:
                ageCat += 'W' + ageGroup
            self.logger.info('Improvised AgeCat lookup.  Came up with {} from {}'.format(ageCat, athlete['Age Cat']))
            return self.cachedAgeCat[ageCat]
    
    def addParkrunEvent(self, parkrun):
        return self.execute("INSERT INTO Events (ParkrunID, EventNumber, EventDate) VALUES (" + str(self.getParkrunID(parkrun['EventURL'])) + ", " + str(parkrun['EventNumber']) + ", CAST('" + parkrun['EventDate'].strftime('%Y-%m-%d') + "' AS date))")

    def replaceParkrunEvent(self, row):
        EventID = self.execute("SELECT dbo.getEventID('{}', {})".format(row['EventURL'], row['EventNumber']))
        if EventID is not None:
            self.execute("DELETE FROM EventPositions WHERE EventID = {}".format(EventID))
            #self.execute("DELETE FROM Events WHERE EventID = {}".format(EventID))
        #return self.execute("INSERT INTO Events (ParkrunID, EventNumber, EventDate) VALUES (" + str(self.getParkrunID(row['EventURL'])) + ", " + str(row['EventNumber']) + ", CAST('" + row['EventDate'].strftime('%Y-%m-%d') + "' AS date))")
        return EventID

    def checkParkrunEvent(self, row):
        r = self.execute("SELECT dbo.getParkrunEventRunners('{}', {})".format(row['EventURL'], row['EventNumber']))
        if r is None:
            return False
        else:
            if r != row['Runners']:
                return False
            else:
                return True

    def checkParkrunVolunteers(self, row):
        r = self.execute("SELECT dbo.getParkrunEventVolunteers('{}', {})".format(row['EventURL'], row['EventNumber']))
        if r is None:
            return False
        else:
            if r != row['Runners']:
                return False
            else:
                return True

    def checkParkrunVolunteers(self, row):
        r = self.execute("SELECT dbo.getParkrunEventVolunteers('{}', {})".format(row['EventURL'], row['EventNumber']))
        if r != row['Volunteers']:
            return False
        else:
            return True

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
            
    def getVolunteer(self, volunteer):
        if self.cachedVolunteer is None:
            data = self.execute("SELECT VolunteerPositionID, VolunteerPosition from VolunteerPositions")
            self.cachedVolunteer = {}
            for row in data:
                self.cachedVolunteer[row['VolunteerPosition']] = row['VolunteerPositionID']
            self.logger.debug('Added {} records to Volunteer Position cache'.format(len(self.cachedVolunteer))) 
        if volunteer is None: return None
        if volunteer not in self.cachedClub:
            c_id = self.execute("INSERT INTO VolunteerPositions (VolunteerPosition) VALUES ('" + volunteer + "')")
            self.cachedVolunteer[volunteer] = c_id
            return c_id
        else:
            return self.cachedVolunteer[volunteer]
            
    def addAthlete(self, athlete):
        self.logger.debug(athlete)
        data = self.execute("SELECT AthleteID, FirstName, LastName, AgeCategoryID, ClubID FROM Athletes WHERE AthleteID = {}".format(athlete['AthleteID']))
        self.logger.debug(data)
        if len(data) == 0:
            try:
                sql = "INSERT INTO Athletes (AthleteID, FirstName, LastName, AgeCategoryID"
                values = " VALUES (" + xstr(athlete['AthleteID']) + ", '" + xstr(athlete['FirstName'][:50]) + "', '" + xstr(athlete['LastName'][:50]) + "', "  + xstr(self.getAgeCatID(athlete)) 
                if athlete['Club'] is not None:
                    sql += ", ClubID"
                    values += ", " + xstr(self.getClub(athlete['Club']))
                #if athlete['StravaID'] is not None:
                #    sql += ", StravaID"
                #    values += ", '" + xstr(athlete['StravaID']) + "'"
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
                   data[0]['LastName'][:50].upper() != xstr(athlete['LastName'][:50].upper()):
                    self.execute("UPDATE Athletes SET AgeCategoryID = " + xstr(self.getAgeCatID(athlete)) + \
                                 ", ClubID = " + xstr(self.getClub(athlete['Club'])) + \
                                 ", FirstName = '" + xstr(athlete['FirstName'][:50]) + "'" + \
                                 ", LastName = '" + xstr(athlete['LastName'][:50]) + "'" + \
                                 " WHERE AthleteID = " + xstr(athlete['AthleteID']))
            
    def addParkrunEventPosition(self, position, addAthlete = True):
        if addAthlete:
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
            values +=  ", '" + position['Note'][:30].replace("'","") + "'"
        sql += ")" + values + ")"
        #print(sql)
        self.execute(sql)

    def updateParkrunEventPosition(self, position, addAthlete = True):
        if addAthlete:
            self.addAthlete(position)
            
        test = f"SELECT EventID, AthleteID, Position, GunTime, AgeCategoryID, AgeGrade, Comment FROM EventPositions WHERE EventID = {xstr(position['EventID'])} AND Position = {xstr(position['Pos'])}"
        existing = self.execute(test)
        #print(existing)
        sql = None
        if len(existing) == 0:  #No record for this position, add a new one
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
                values +=  ", '" + position['Note'][:30].replace("'","") + "'"
            sql += ")" + values + ")"
        else:
            existing=existing[0]
            #print(existing)
            #print(position)
            if existing['AthleteID'] != position['AthleteID']:
                sql = f"UPDATE EventPositions SET AthleteID = {xstr(position['AthleteID'])}"
        
                if position['Time'] is not  None:
                    sql += f", GunTime = CAST('{xstr(position['Time'])}' as time(0))" 
                if position['Age Cat'] is not  None:
                    sql += f", AgeCategoryID = {xstr(self.getAgeCatID(position))}" 
                if position['Age Grade'] is not  None:
                    sql += f", AgeGrade = {xstr(position['Age Grade'])}" 
                if position['Note'] is not  None and position['Note'] != 'PB':
                    sql += ", Comment = '" + position['Note'][:30].replace("'","") + "'" 
                sql += f" WHERE EventID = {xstr(position['EventID'])} AND Position = {xstr(position['Pos'])}"
        
        #print(sql)
        if sql != None:
            self.execute(sql)
    
    def updateParkrunURL(self, parkrun, verified, valid):
        self.execute("UPDATE Parkruns SET URLVerified = " + str(int(verified)) + ", URLValid = " + str(int(valid)) + " WHERE ParkrunName = '" + parkrun + "'")
        
    def close(self):
        self.conn.close()
        del self.conn
