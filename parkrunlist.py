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
        
    def countries(self, countries, add):
        for c in countries:
            if add:
                self.logger.debug('Adding {} to Countries.'.format(c))
            else:
                self.logger.debug('Removing {} from Countries.'.format(c))
            sql = "SELECT * FROM getLastImportedEventByCountry('{}')".format(c)
            if not self.inactive:
                sql += " where Active = 1"
            self.__update(sql, add)
    
    def regions(self, regions, add):
        for r in regions:
            if add:
                self.logger.debug('Adding {} to Regions.'.format(r))
            else:
                self.logger.debug('Removing {} from Regions.'.format(r))
            sql = "SELECT * FROM getLastImportedEventByRegion('{}')".format(r)
            if not self.inactive:
                sql += " where Active = 1"
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
                if not self.inactive:
                    sql += " where Active = 1"
            return
        if type(events).__name__ == 'str':
            sql = "SELECT * FROM getLastImportedEventByEvent('{}')".format(events)
            self.__update(sql, add)
            if not self.inactive:
                sql += " where Active = 1"
    
    def addAll(self):
        self.logger.debug('Adding everything.')
        sql = "SELECT * FROM getLastImportedEvent()"
        if not self.inactive:
            sql += " where Active = 1"
            
        self.__update(sql, True)

    def __update(self, sql, add):
        c = Connection(self.config)
        if self.mode == Mode.NEWEVENTS:
            sql += " where LastUpdated < dateadd(day,-7,getdate()) or LastUpdated IS NULL"
        data = c.execute(sql)
        for row in data:
            if add:
                if row['Parkrun'] not in self.__parkruns:
                    self.logger.debug('Adding event {}'.format(row['Parkrun']))
                    self.__parkruns[row['Parkrun']] = {'Name'             :row['Parkrun'],
                                                       'url'              :row['URL'],
                                                       'lastEvent'        :row['LastEventNumber'],
                                                       'EventHistoryURL'  :row['EventHistoryURL'],
                                                       'EventNumberURL'   :row['EventNumberURL'],
                                                       'LatestResultsURL' :row['LatestResultsURL']}
            else:
                if row['Parkrun'] in self.__parkruns:
                    self.logger.debug('Removing event {}'.format(row['Parkrun']))
                    del self.__parkruns[row['Parkrun']]
            
    def __iter__(self):
        for k in self.__parkruns.keys():
            yield self.__parkruns[k]
            
    def __len__(self):
        return(len(self.__parkruns))