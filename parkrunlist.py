#from mplogger import *
from dbconnection import Connection
from worker import Mode
import logging, logging.config

class ParkrunList():
    def __init__(self, config, mode):
        self.__parkruns = {}
        #self.__index = 0
        self.config = config
        logging.config.dictConfig(config)
        self.logger = logging.getLogger(__name__)
        self.mode = mode
        self.inactive = False
        self.year = 0
        
    def countries(self, countries, add):
        for c in countries:
            if add:
                self.logger.debug('Adding {} to Countries.'.format(c))
            else:
                self.logger.debug('Removing {} from Countries.'.format(c))
            sql = "SELECT * FROM getLastImportedEventByCountry('{}')".format(c)
            self.__update(sql, add)
    
    def regions(self, regions, add):
        for r in regions:
            if add:
                self.logger.debug('Adding {} to Regions.'.format(r))
            else:
                self.logger.debug('Removing {} from Regions.'.format(r))
            sql = "SELECT * FROM getLastImportedEventByRegion('{}')".format(r)
            self.__update(sql, add)
    
    def events(self, events, add):
        if type(events).__name__ == 'list':
            for e in events:
                if add:
                    self.logger.debug('Adding {} to Events.'.format(e))
                else:
                    self.logger.debug('Removing {} from Events.'.format(e))
                sql = "SELECT * FROM getLastImportedEventByEvent('{}')".format(e)
                self.__update(sql, add)
            return
        if type(events).__name__ == 'str':
            sql = "SELECT * FROM getLastImportedEventByEvent('{}')".format(events)
            self.__update(sql, add)
    
    def addAll(self):
        self.logger.debug('Adding everything.')
        sql = "SELECT * FROM getLastImportedEvent()"
            
        self.__update(sql, True)

    def __update(self, sql, add):
        c = Connection(self.config)
        if not self.inactive or self.year != 0:
            sql += ' WHERE '
            if not self.inactive:
                sql += "Active = 1"
            if self.year != 0:
                if not self.inactive:
                    sql += ' AND '
                sql += 'datepart(year,LaunchDate) <= {}'.format(self.year)
        if self.mode == Mode.NEWEVENTS:
            if not self.inactive or self.year != 0:
                sql += ' AND (LastUpdated < dateadd(day, -dbo.getReportDelay(), getdate()) or LastUpdated IS NULL)'
            else:
                sql += ' WHERE (LastUpdated < dateadd(day, -dbo.getReportDelay(), getdate()) or LastUpdated IS NULL)'
                
        data = c.execute(sql)
        for row in data:
            if add:
                if row['Parkrun'] not in self.__parkruns:
                    self.logger.debug('Adding event {}'.format(row['Parkrun']))
                    self.__parkruns[row['Parkrun']] = {'Name'             :row['Parkrun'],
                                                       'URL'              :row['URL'],
                                                       'EventURL'         :row['URL'].split('/')[3],
                                                       'lastEvent'        :row['LastEventNumber'],
                                                       'EventHistoryURL'  :row['EventHistoryURL'],
                                                       'EventNumberURL'   :row['EventNumberURL'],
                                                       'LatestResultsURL' :row['LatestResultsURL']}
            else:
                if row['Parkrun'] in self.__parkruns:
                    self.logger.debug('Removing event {}'.format(row['Parkrun']))
                    del self.__parkruns[row['Parkrun']]
        #data = c.execute("SELECT * FROM getParkrunCancellationsThisWeek")
        #if self.mode == Mode.NEWEVENTS:
        #    for row in data:
        #        if row['Parkrun'] in self.__parkruns:
        #            self.logger.debug('Removing cancelled event {}'.format(row['Parkrun']))
        #            del self.__parkruns[row['Parkrun']]
            
    def __iter__(self):
        for k in self.__parkruns.keys():
            yield self.__parkruns[k]
            
    def __len__(self):
        return(len(self.__parkruns))