from dbconnection import Connection

class ParkrunList():
    def __init__(self):
        self.__parkruns = {}
        self.__index = 0
        self.__c = Connection()
        
    def addCountries(self, countries):
        for c in countries:
            sql = "SELECT * FROM getLastImportedEventByCountry('" + c + "')"
            cur = self.__c.execute(sql)
            for row in cur.fetchall():
                 if row[1] not in self.__parkruns:
                     self.__parkruns[row[1]] = {'Name':row[0], 'url':row[1], 'lastEvent':row[2]}
    
    def addRegions(self, regions):
        for r in regions:
            sql = "SELECT * FROM getLastImportedEventByRegion('" + r + "')"
            cur = self.__c.execute(sql)
            for row in cur.fetchall():
                 if row[1] not in self.__parkruns:
                     self.__parkruns[row[1]] = {'Name':row[0], 'url':row[1], 'lastEvent':row[2]}
    
    def addEvents(self, events):
        for e in events:
            sql = "SELECT * FROM getLastImportedEvent('" + e + "')"
            cur = self.__c.execute(sql)
            for row in cur.fetchall():
                 if row[1] not in self.__parkruns:
                     self.__parkruns[row[1]] = {'Name':row[0], 'url':row[1], 'lastEvent':row[2]}
    
    def __iter__(self):
        for k in self.__parkruns.keys():
            yield self.__parkruns[k]
            
    def __len__(self):
        return(len(self.__parkruns))