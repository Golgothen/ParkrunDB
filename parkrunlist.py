from dbconnection import Connection

class ParkrunList():
    def __init__(self):
        self.__parkruns = {}
        self.__index = 0
        
    def countries(self, countries, add = True):
        for c in countries:
            sql = "SELECT * FROM getLastImportedEventByCountry('" + c + "')"
            self.__update(sql, add)
    
    def regions(self, regions, add = True):
        for r in regions:
            sql = "SELECT * FROM getLastImportedEventByRegion('" + r + "')"
            self.__update(sql, add)
    
    def events(self, events, add = True):
        for e in events:
            sql = "SELECT * FROM getLastImportedEventByEvent('" + e + "')"
            self.__update(sql, add)
    
    def addAll(self):
        sql = "SELECT * FROM getLastImportedEvent()"
        self.__update(sql, True)

    def __update(self, sql, add = True):
        c = Connection()
        cursor = c.execute(sql)
        for row in cursor.fetchall():
            if add:
                 if row[1] not in self.__parkruns:
                     self.__parkruns[row[1]] = {'Name':row[0], 'url':row[1], 'lastEvent':row[5]}
            else:
                if row[1] in self.__parkruns:
                    del self.__parkruns[row[1]]
            
    def __iter__(self):
        for k in self.__parkruns.keys():
            yield self.__parkruns[k]
            
    def __len__(self):
        return(len(self.__parkruns))