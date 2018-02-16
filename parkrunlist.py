#from mplogger import *
from dbconnection import Connection
import logging, logging.config

class ParkrunList():
    def __init__(self, config):
        self.__parkruns = {}
        self.__index = 0
        self.config = config
        logging.config.dictConfig(config)
        self.logger = logging.getLogger(__name__)
        
    def countries(self, countries, add = True):
        for c in countries:
            self.logger.debug('Adding {} to Countries.'.format(c))
            sql = "SELECT * FROM getLastImportedEventByCountry('" + c + "')"
            self.__update(sql, add)
    
    def regions(self, regions, add = True):
        for r in regions:
            self.logger.debug('Adding {} to Regions.'.format(r))
            sql = "SELECT * FROM getLastImportedEventByRegion('" + r + "')"
            self.__update(sql, add)
    
    def events(self, events, add = True):
        for e in events:
            self.logger.debug('Adding {} to Events.'.format(e))
            sql = "SELECT * FROM getLastImportedEventByEvent('" + e + "')"
            self.__update(sql, add)
    
    def addAll(self):
        self.logger.debug('Adding everything.')
        sql = "SELECT * FROM getLastImportedEvent()"
        self.__update(sql, True)

    def __update(self, sql, add = True):
        c = Connection(self.config)
        data = c.execute(sql)
        for row in data:
            if add:
                if row['Parkrun'] not in self.__parkruns:
                    self.__parkruns[row['Parkrun']] = {'Name'             :row['Parkrun'],
                                                       'url'              :row['URL'],
                                                       'lastEvent'        :row['LastEventNumber'],
                                                       'EventHistoryURL'  :row['EventHistoryURL'],
                                                       'EventNumberURL'   :row['EventNumberURL'],
                                                       'LatestResultsURL' :row['LatestResultsURL']}
            else:
                if row['URL'] in self.__parkruns:
                    del self.__parkruns[row['URL']]
            
    def __iter__(self):
        for k in self.__parkruns.keys():
            yield self.__parkruns[k]
            
    def __len__(self):
        return(len(self.__parkruns))