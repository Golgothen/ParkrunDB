import multiprocessing
from urllib.request import urlopen, Request #, localhost
from urllib.error import HTTPError
import os
import lxml.html
from time import sleep
from dbconnection import Connection
from datetime import datetime
from message import Message
from enum import Enum

xstr = lambda s: '' if s is None else str(s)

class Mode(Enum):
    NORMAL = 0
    CHECKURLS = 1
    NEWEVENTS = 2
    
    @classmethod
    def default(cls):
        return cls.NORMAL

class Worker(multiprocessing.Process):
    def __init__(self, q, m, i, mode):
        super(Worker, self).__init__()
        self.inQ = q  #input Queue
        #self.l = l
        #self.c = Connection()
        self.msgQ = m  #message queue
        self.id = i
        self.mode = mode
        
    def run(self):
        c = Connection()
        self.msgQ.put(Message('Process',self.id, 'Running'))
        while True:
            #self.l.acquire()
            parkrun = self.inQ.get()
            #self.l.release()
            if parkrun is None:
                self.msgQ.put(Message('Process', self.id, 'Exiting'))
                break
            if parkrun['lastEvent'] is None: parkrun['lastEvent'] = 0
            if self.mode == Mode.CHECKURLS:
                if self.getURL(parkrun['url']) is not None:
                    c.updateParkrunURL(parkrun['Name'], True, True)
                    self.msgQ.put(Message('Process', self.id, 'Verified ' + parkrun['Name'] + ' valid'))
                else:
                    c.updateParkrunURL(parkrun['Name'], True, False)
                    self.msgQ.put(Message('Error', self.id, 'Could not verify ' + parkrun['Name'] + ' as valid'))
            else:
                data = self.getEventHistory(parkrun['url'])
                if data is not None:
                    for row in data:
                        row['Name'] = parkrun['Name']
                        # Add the event if it's a new event
                        if self.mode == Mode.NORMAL or self.mode == Mode.NEWEVENTS:
                            if row['EventNumber'] > parkrun['lastEvent']:
                                self.msgQ.put(Message('Process', self.id, 'Adding ' + row['Name'] + ' event ' + xstr(row['EventNumber'])))
                                eventID = c.addParkrunEvent(row)
                                eData = self.getEvent(parkrun['url'], row['EventNumber'])
                                if eData is not None:
                                    for eRow in eData:
                                        eRow['EventID'] = eventID
                                        c.addParkrunEventPosition(eRow)
                        if self.mode == Mode.NORMAL:
                            self.msgQ.put(Message('Process', self.id, 'Checking ' + row['Name'] + ' event ' + xstr(row['EventNumber'])))
                            # Check the event has the correct number of runners
                            if not c.checkParkrunEvent(row):
                                #if not, delete the old event record and re-import the data
                                self.msgQ.put(Message('Process', self.id, 'Replacing ' + row['Name'] + ' event ' + xstr(row['EventNumber'])))
                                eventID = c.replaceParkrunEvent(row)
                                eData = self.getEvent(parkrun['url'], row['EventNumber'])
                                if eData is not None:
                                    for eRow in eData:
                                        eRow['EventID'] = eventID
                                        c.addParkrunEventPosition(eRow)
        c.close()
        
    def getURL(self, url):
        completed = False
        while not completed:
            try:
                f = urlopen(Request(url, data=None, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'}))
                completed = True
            except HTTPError as e:
                if e.code == 404:
                    self.msgQ.put(Message('Error',self.id, 'Bad URL ' + url))
                    return None
                self.msgQ.put(Message('Error', self.id, 'Got response {}. retrying in 1 second'.format(e.code)))
                sleep(1)
            except:
                self.msgQ.put(Message('Error', self.id, 'Bad URL ' + url))
                return None
        return f.read().decode('utf-8')
    
    def getEvent(self, url, parkrunEvent):
        url = url + "/results/weeklyresults/?runSeqNumber=" + str(parkrunEvent)
        html = self.getURL(url)
        #Test if we got a valid response'
        if html is None:  #most likely a 404 error
            self.msgQ.put(Message('Error', self.id, 'Error getting event. Check url ' + url))
            return None
        if '<h1>Something odd has happened, so here are the most first finishers</h1>' in html:  
            self.msgQ.put(Message('Error', self.id, 'Possible URL error getting event. Check url ' + url))
            return None
        html = '<table' + html.split('<table')[1]
        html = html.split('</p>')[0]
        table = lxml.html.fromstring(html)
        
        headings = ['Pos','parkrunner','Time','Age Cat','Age Grade','Gender','Gender Pos','Club','Note','Strava']
        
        rows = table.xpath('//tbody/tr')
        
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
                        d[h]=0.0
                if h == 'parkrunner':
                    if len(v.getchildren())>0:
                        d['FirstName']=v.getchildren()[0].text.split()[0].replace("'","''")
                        if len(v.getchildren()[0].text.split())>1:
                            d['LastName']=v.getchildren()[0].text.split()[1].capitalize().replace("'","''")
                        else:
                            d['LastName'] = None
                        d['AthleteID']=int(v.getchildren()[0].get('href').split('=')[1])
                    else:
                        d['FirstName']=v.text.replace("'","''")
                        d['LastName']=None
                        d['AthleteID']=0
                if h == 'Strava':
                    if len(v.getchildren())>0:
                        added = False
                        for c in v.getchildren():
                            if 'strava' in c.get('href'):
                                d['StravaID']=c.get('href').split('/')[4]
                                added = True
                        if not added:
                            d['StravaID']=None
                    else:
                        d['StravaID']=None
                if h == 'Age Cat':
                    if len(v.getchildren())>0:
                        d[h]=v.getchildren()[0].text
                    else:
                        d[h]=''
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
        return data
    
    def getEventHistory(self, url):
        url = url + "/results/eventhistory/"
        html = self.getURL(url)
        #Test if we got a valid response'
        if html is None:  #most likely a 404 error
            self.msgQ.put(Message('Error', self.id, 'Possible 404 error gettint event history. Check url ' + url))
            return None
        if '<h1>Something odd has happened, so here are the most first finishers</h1>' in html:    
            self.msgQ.put(Message('Error', self.id, 'URL error in event history. Check ' + url))
            return None
        html = '<table' + html.split('<table')[1]
        html = html.split('<div')[0]
        table = lxml.html.fromstring(html)
        
        headings = ['EventNumber','EventDate','Runners']    
        rows = table.xpath('//tbody/tr')
        
        data = []
        for row in rows:
            d = {}
            for h, v in zip(headings, row.getchildren()):
                if h == 'EventNumber':
                    d[h] = int(v.getchildren()[0].text)
                if h == 'Runners':
                    d[h] = int(v.text)
                if h == 'EventDate':
                    d[h] = datetime.strptime(v.getchildren()[0].text,"%d/%m/%Y")
            data.insert(0,d)
        return data

