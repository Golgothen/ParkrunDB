import multiprocessing, lxml.html, logging, logging.config, signal #, os

from urllib.request import urlopen, Request #, localhost
from urllib.error import HTTPError
from time import sleep
from dbconnection import Connection
from datetime import datetime
from message import Message
from enum import Enum

xstr = lambda s: '' if s is None else str(s)

# Test new repo sync in VisualStudio

class Mode(Enum):
    NORMAL = 0
    CHECKURLS = 1
    NEWEVENTS = 2
    
    @classmethod
    def default(cls):
        return cls.NORMAL

class Worker(multiprocessing.Process):
    def __init__(self, q, m, i, mode, config, delay):
        super(Worker, self).__init__()
        self.inQ = q  #input Queue
        self.msgQ = m  #message queue
        self.id = i
        self.mode = mode
        self.config = config
        self.delay = delay
        #self.loggingQueue = loggingQueue
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        
    def run(self):
        c = Connection(self.config)
        logging.config.dictConfig(self.config)
        self.logger = logging.getLogger(__name__)
        self.logger.info('Process {} Running'.format(self.id))
        self.msgQ.put(Message('Process',self.id, 'Running'))
        while True:
            parkrun = self.inQ.get()
            self.logger.debug(parkrun)
            if parkrun is None:
                self.logger.info('Process {} Exiting'.format(self.id))
                self.msgQ.put(Message('Process', self.id, 'Exiting'))
                break
            self.logger.debug('Process {} got record {}'.format(self.id, parkrun['EventURL']))
            if parkrun['lastEvent'] is None: parkrun['lastEvent'] = 0
            if self.mode == Mode.CHECKURLS:
                if self.getURL(parkrun['URL']) is not None:
                    c.updateParkrunURL(parkrun['Name'], True, True)
                    self.msgQ.put(Message('Process', self.id, 'Verified ' + parkrun['Name'] + ' valid'))
                else:
                    c.updateParkrunURL(parkrun['Name'], True, False)
                    self.msgQ.put(Message('Error', self.id, 'Could not verify ' + parkrun['Name'] + ' as valid'))
            
            if self.mode == Mode.NEWEVENTS:
                runnersAdded = False
                self.logger.info('Process {} checking for new results for {}'.format(self.id, parkrun['EventURL']))
                self.msgQ.put(Message('Process', self.id, 'Checking for new results for ' + parkrun['Name'] ))
                parkrun['EventNumber'], parkrun['EventDate'], data = self.getLatestEvent(parkrun['URL'] + parkrun['LatestResultsURL'])
                if data is not None:
                    runnersAdded = True
                    self.logger.debug('Event {} got {} events in history'.format(parkrun['EventURL'], len(data)))
                    parkrun['Runners'] = len(data)
                    # Add the event if it's a new event
                    # Check the event has the correct number of runners
                    if not c.checkParkrunEvent(parkrun):
                        self.logger.info('Parkrun {} event {}: runners did not match - reimporting.'.format(parkrun['Name'], parkrun['EventNumber']))
                        #if not, delete the old event record and re-import the data
                        self.msgQ.put(Message('Process', self.id, 'Updating ' + parkrun['Name'] + ' event ' + xstr(parkrun['EventNumber'])))
                        eventID = c.replaceParkrunEvent(parkrun)
                        self.logger.debug('getLastEvent found {} runners'.format(len(data)))
                        for row in data:
                            row['EventID'] = eventID
                            c.addParkrunEventPosition(row)
                        sleep(self.delay)
                
            if self.mode == Mode.NORMAL:
                runnersAdded = False
                data = self.getEventHistory(parkrun['URL'] + parkrun['EventHistoryURL'])
                if data is not None:
                    self.logger.debug('Event {} got {} events in history'.format(parkrun['URL'], len(data)))
                    for row in data:
                        row['Name'] = parkrun['Name']
                        row['EventURL'] = parkrun['EventURL']
                        # Add the event if it's a new event
                        self.msgQ.put(Message('Process', self.id, 'Checking ' + row['Name'] + ' event ' + xstr(row['EventNumber'])))
                        self.logger.debug(row)
                        self.logger.debug('Process {} Checking {} event {}'.format(self.id, row['EventURL'], xstr(row['EventNumber'])))
                        # Check the event has the correct number of runners
                        if not c.checkParkrunEvent(row):
                            #if not, delete the old event record and re-import the data
                            self.logger.info('Parkrun {} event {}: runners did not match - reimporting.'.format(parkrun['EventURL'], row['EventNumber']))
                            self.msgQ.put(Message('Process', self.id, 'Updating ' + row['Name'] + ' event ' + xstr(row['EventNumber'])))
                            eventID = c.replaceParkrunEvent(row)
                            eData = self.getEvent(parkrun['URL'] + parkrun['EventNumberURL'], row['EventNumber'])
                            if eData is not None:
                                runnersAdded = True
                                self.logger.debug('getEvent found {} runners'.format(len(eData)))
                                for eRow in eData:
                                    eRow['EventID'] = eventID
                                    c.addParkrunEventPosition(eRow)
                                sleep(self.delay)
                            else:
                                self.logger.debug('getEvent found no runners')
                else:
                    self.logger.warning('Parkrun {} returns no history page.'.format(parkrun['Name']))
            if runnersAdded:
                c.execute("update p set p.LastUpdated = e.LastEvent from parkruns as p inner join (select ParkrunID, max(EventDate) as LastEvent from events group by ParkrunID) as e on p.ParkrunID = e.ParkrunID")
            self.logger.debug('Sleeping for {} seconds'.format(self.delay))
            sleep(self.delay)
        c.close()
        
    def getURL(self, url):
        completed = False
        while not completed:
            try:
                self.logger.debug('Hitting {}'.format(url))
                f = urlopen(Request(url, data=None, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'}))
                completed = True
            except HTTPError as e:
                self.logger.warning('Got HTTP Error {}'.format(e.code))
                if e.code == 404:
                    self.msgQ.put(Message('Error',self.id, 'Bad URL ' + url))
                    return None
                if e.code == 403:
                    self.msgQ.put(Message('Error',self.id, 'Forbidden ' + url))
                    return None
                self.msgQ.put(Message('Error', self.id, 'Got response {}. retrying in 1 second'.format(e.code)))
                sleep(1)
            except:
                self.logger.warning('Unexpected network error. URL: ' + url)
                self.msgQ.put(Message('Error', self.id, 'Bad URL ' + url))
                return None
        temp = f.read().decode('utf-8', errors='ignore')
        self.logger.debug('URL returned string of length {}'.format(len(temp)))
        return lxml.html.fromstring(temp) 
    
    def getEventTable(self, root):
        
        table = root.xpath('//*[@id="results"]')[0]
        
        headings = ['Pos','parkrunner','Time','Age Cat','Age Grade','Gender','Gender Pos','Club','Note',] #'Strava']
        
        rows = table.xpath('//tbody/tr')
        
        if len(rows) > 0:
            if len(rows[0].getchildren()) < 11:  # France results have no position or Gender position columns
                headings = ['parkrunner','Time','Age Cat','Age Grade','Gender','Club','Note']#,'Strava']
        else:
            headings = ['Pos','parkrunner','Time','Age Cat','Age Grade','Gender','Gender Pos','Club','Note'] #,'Strava']
        data = []
        
        for row in rows:
            d = {}
            for h, v in zip(headings, row.getchildren()):
                if h in ['Time','Note']:
                    d[h]=v.text
                    if h == 'Time':
                        if d[h] is not None:
                            if len(d[h])<6:
                                d[h] = '0:' + d[h]
                if h == 'Pos':
                    d[h]=int(v.text)
                if h == 'Age Grade':
                    if v.text is not None:
                        d[h]=float(v.text.split()[0])
                    else:
                        d[h]=None
                if h == 'parkrunner':
                    if len(v.getchildren())>0:
                        d['FirstName']=v.getchildren()[0].text.split()[0].replace("'","''")
                        lastName = ''
                        for i in range(1, len(v.getchildren()[0].text.split())):
                            lastName+=v.getchildren()[0].text.split()[i].capitalize().replace("'","''") + ' '
                        if lastName != '':
                            d['LastName'] = lastName.strip()
                        else:
                            d['LastName'] = ''
                        d['AthleteID']=int(v.getchildren()[0].get('href').split('=')[1])
                    else:
                        d['FirstName']=v.text.replace("'","''")
                        d['LastName']=None
                        d['AthleteID']=0
                #if h == 'Strava':
                #    if len(v.getchildren())>0:
                #        added = False
                #        for c in v.getchildren():
                #            if 'strava' in c.get('href'):
                #                d['StravaID']=c.get('href').split('/')[4]
                #                added = True
                #        if not added:
                #            d['StravaID']=None
                #    else:
                #        d['StravaID']=None
                if h == 'Age Cat':
                    if len(v.getchildren())>0:
                        d[h]=v.getchildren()[0].text
                    else:
                        d[h]=None
                if h == 'Gender':
                    if v.text is not None:
                        d[h]=v.text
                    else:
                        d[h]='M'
                if h == 'Club':
                    if len(v.getchildren())>0:
                        if v.getchildren()[0].text is not None:
                            d[h]=v.getchildren()[0].text.replace("'","''")
                        else:
                            d[h]=None
                    else:
                        d[h]=None
            data.append(d)
        if len(data) > 0:
            if 'Pos' not in data[0].keys():
                data = sorted(data, key=lambda k: '0:00:00' if k['Time'] is None else k['Time'])
                for i in range(len(data)):
                    data[i]['Pos'] = i + 1
            return data
        else:
            return None
    
    def getEvent(self, url, parkrunEvent):
        self.logger.debug('Hitting {}'.format(url + str(parkrunEvent)))
        root = self.getURL(url + str(parkrunEvent))
        #Test if we got a valid response'
        if root is None:  #most likely a 404 error
            self.logger.warning('Error retrieving event')
            self.msgQ.put(Message('Error', self.id, 'Error getting event. Check url ' + url))
            return None
        if len(root.xpath('//*[@id="content"]/h1')) == 0:
            self.logger.warning('Error retrieving event')
            self.msgQ.put(Message('Error', self.id, 'Possible URL error getting event. Check url ' + url))
            return None
        return self.getEventTable(root)
    
    def getLatestEvent(self, url):
        self.logger.debug('Hitting {}'.format(url))
        root = self.getURL(url)
        
        #Test if we got a valid response
        if root is None:  #most likely a 404 error
            self.logger.warning('Error retrieving event')
            self.msgQ.put(Message('Error', self.id, 'Error getting event. Check url ' + url))
            return 0, None, None
        if len(root.xpath('//*[@id="content"]/h1')) > 0:
            self.logger.warning('Error retrieving event')
            self.msgQ.put(Message('Error', self.id, 'Possible URL error getting event. Check url ' + url))
            return 0, None, None
        
        try:
            eventHTML = root.xpath('//*[@id="content"]/h2')[0].text
        except IndexError:
            self.logger.warning('Error retrieving event')
            self.msgQ.put(Message('Error', self.id, 'Possible page error retrieving url ' + url))
            return 0, None, None

        if len(eventHTML.split('#')[1].split('-')[0].strip()) == 0:
            return 0, None, None
        eventNumber =  int(eventHTML.split('#')[1].split('-')[0].strip())
        eventDate = datetime.strptime(eventHTML[len(eventHTML)-10:],'%d/%m/%Y')
        
        return eventNumber, eventDate, self.getEventTable(root)

    def getEventHistory(self, url):
        self.logger.debug('Hitting {}'.format(url))
        root = self.getURL(url)
        
        #Test if we got a valid response
        if root is None:  #most likely a 404 error
            self.logger.warning('Error retrieving event. URL: ' + url)
            self.msgQ.put(Message('Error', self.id, 'Possible 404 error getting event history. Check url ' + url))
            return None

        if len(root.xpath('//*[@id="content"]/h1')) > 0:
            self.logger.warning('Error retrieving event')
            self.msgQ.put(Message('Error', self.id, 'URL error in event history. Check ' + url))
            return None

        table = root.xpath('//*[@id="results"]')[0]
        headings = ['EventNumber','EventDate','Runners','Volunteers']    
        rows = table.xpath('//tbody/tr')
        
        data = []
        for row in rows:
            d = {}
            for h, v in zip(headings, row.getchildren()):
                if h == 'EventNumber':
                    d[h] = int(v.getchildren()[0].text)
                if h in ['Runners','Volunteers']:
                    d[h] = int(v.text)
                if h == 'EventDate':
                    d[h] = datetime.strptime(v.getchildren()[0].text,"%d/%m/%Y")
            data.insert(0,d)
        return data

