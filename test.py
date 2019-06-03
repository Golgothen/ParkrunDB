import multiprocessing, lxml.html, logging, logging.config, signal #, os

from urllib.request import urlopen, Request #, localhost
from urllib.error import HTTPError
from time import sleep
from dbconnection import Connection
from datetime import datetime
from message import Message
from enum import Enum
from mplogger import *
xstr = lambda s: '' if s is None else str(s)

class Mode(Enum):
    NORMAL = 0
    CHECKURLS = 1
    NEWEVENTS = 2
    
    @classmethod
    def default(cls):
        return cls.NORMAL

def getURL(url):
    completed = False
    print('Hit {}'.format(url))
    while not completed:
        try:
            f = urlopen(Request(url, data=None, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'}))
            completed = True
        except:
            return None
    temp = f.read().decode('utf-8', errors='ignore')
    return lxml.html.fromstring(temp) 

def getEventTable(root):
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


root = getURL('https://www.parkrun.com.au/toolerncreek/results/latestresults/')
c = Connection(sender_config)


volunteerNames = [x.strip() for x in root.xpath('//*[@id="content"]/div[2]/p[1]')[0].text.split(':')[1].split(',')]
volunteers = []
parkrun = root.xpath('//*[@id="content"]/h2')[0].text.strip().split(' parkrun')[0]
eventnumber = int(root.xpath('//*[@id="content"]/h2')[0].text.strip().split('#')[1].strip().split()[0])
date = datetime.strptime(root.xpath('//*[@id="content"]/h2')[0].text.strip().split(' -')[1].strip(),'%d/%m/%Y')
results = getEventTable(root)
#if c.execute("SELECT dbo.getParkrunType('{}')".format(parkrun)) == 'Standard':

for v in volunteerNames:
    volunteers.append(c.execute("SELECT * FROM getAthleteParkrunVolunteerBestMatch('{}','{}','{}')".format(v.split()[0],v.split()[1],parkrun))[0])

# Locate the tail walker(s)
found = True
while found:
    found = False
    tailwalker = None
    for v in volunteers:
        try:
            tailwalker = next(r for r in results if r['AthleteID'] == v['AthleteID'] and r['Pos'] == len(results))
            found = True
            if len(c.execute("SELECT * FROM EventVolunteers WHERE EventID = dbo.getEventID('{}', {}) AND AthleteID = {} AND VolunteerPositionID = 1".format(parkrun, eventnumber, tailwalker['AthleteID']))) == 0:
                print("INSERT INTO EventVolunteers (EventID, AthleteID, VolunteerPositionID) VALUES (dbo.getEventID('{}', {}), {}, 1)".format(parkrun, eventnumber, tailwalker['AthleteID']))
                c.execute("INSERT INTO EventVolunteers (EventID, AthleteID, VolunteerPositionID) VALUES (dbo.getEventID('{}', {}), {}, 1)".format(parkrun, eventnumber, tailwalker['AthleteID']))
            volunteers = [v for v in volunteers if not (v['AthleteID'] == tailwalker['AthleteID'])]
            results = results[:-1]
            break
        except StopIteration:
            continue
    

for v in volunteers:
    athletepage = getURL('https://www.parkrun.com.au/results/athleteresultshistory/?athleteNumber={}'.format(v['AthleteID']))
    table = athletepage.xpath('//*[@id="results"]/tbody')[2]
    v['Volunteer'] = {}
    for r in table.getchildren():
        if int(r.getchildren()[0].text) == date.year:
            if r.getchildren()[1].text == 'Tail Walker':
                continue
            v['Volunteer'][r.getchildren()[1].text] = int(r.getchildren()[2].text)
    athletevol = c.execute("SELECT * FROM qryAthleteVolunteerSummaryByYear WHERE AthleteID = {} AND Year = {}".format(v['AthleteID'], date.year))
    if len(athletevol) > 0:
        for al in athletevol:
            v['Volunteer'][al['VolunteerPosition']] -= al['Count']
            if v['Volunteer'][al['VolunteerPosition']] == 0:
                del v['Volunteer'][al['VolunteerPosition']]
    if v != volunteers[-1]:
        sleep(2)

while len(volunteers)>0:
    for v in volunteers:
        if len(v['Volunteer']) == 1:
            if len(c.execute("SELECT * FROM EventVolunteers WHERE EventID = dbo.getEventID('{}', {}) AND AthleteID = {} AND VolunteerPositionID = dbo.getVolunteerID('{}')".format(parkrun, eventnumber, v['AthleteID'], list(v['Volunteer'].keys())[0]))) == 0:
                c.execute("INSERT INTO EventVolunteers (EventID, AthleteID, VolunteerPositionID) VALUES (dbo.getEventID('{}', {}), {}, dbo.getVolunteerID('{}'))".format(parkrun, eventnumber, v['AthleteID'], list(v['Volunteer'].keys())[0]))
            for x in volunteers:
                if x == v:
                    continue
                if list(v['Volunteer'].keys())[0] in x['Volunteer']:
                    print('Removing {} from {}'.format(list(v['Volunteer'].keys())[0], x['AthleteID']))
                    del x['Volunteer'][list(v['Volunteer'].keys())[0]]
            volunteers = [x for x in volunteers if not (v['AthleteID'] == x['AthleteID'])]
            print(len(volunteers))
            break

                
    
    

