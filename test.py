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

c = Connection(sender_config)

root = getURL('https://www.parkrun.com.au/albert-melbourne/results/weeklyresults/?runSeqNumber=125')
eventURL = 'albert-melbourne'

def getVolunteers(root):
    
volunteerNames = [x.strip() for x in root.xpath('//*[@id="content"]/div[2]/p[1]')[0].text.split(':')[1].split(',')]
volunteers = []
parkrun = root.xpath('//*[@id="content"]/h2')[0].text.strip().split(' parkrun')[0]
eventnumber = int(root.xpath('//*[@id="content"]/h2')[0].text.strip().split('#')[1].strip().split()[0])
date = datetime.strptime(root.xpath('//*[@id="content"]/h2')[0].text.strip().split(' -')[1].strip(),'%d/%m/%Y')
results = getEventTable(root)
#if c.execute("SELECT dbo.getParkrunType('{}')".format(parkrun)) == 'Standard':

for v in volunteerNames:
    fn = ''
    ln = ''
    n = v.split()
    fn  = n[0]
    del n[0]
    for l in n:
        ln += l + ' ' 
    fn = fn.replace("'","''").strip()
    ln = ln.replace("'","''").strip()
    candidates = c.execute("SELECT * FROM getAthleteParkrunVolunteerBestMatch('{}','{}','{}')".format(fn,ln,eventURL))
    if len(candidates) > 0:
        volunteers.append(candidates[0])
    else:
        print("Could not find suitable candidate for {} {} at {}, event {}".format(fn, ln, eventURL, eventnumber))
    
# Remove athletes that already have volunteered for this event
found = True
while found:
    for v in volunteers:
        found = False
        if len(c.execute("SELECT * FROM EventVolunteers WHERE EventID = dbo.getEventID('{}', {}) AND AthleteID = {}".format(parkrun, eventnumber, v['AthleteID']))) > 0:
            found = True
            print('Deleting {} {} ({})'.format(v['FirstName'], v['LastName'], v['AthleteID']))
            volunteers = [x for x in volunteers if x['AthleteID'] != v['AthleteID']]
            break
    
# Retrieve all remaining volunteer stats and remove accounted stats.
for v in volunteers:
    athletepage = getURL('https://www.parkrun.com.au/results/athleteresultshistory/?athleteNumber={}'.format(v['AthleteID']))
    if len(athletepage.xpath('//*[@id="results"]/tbody')) > 2:
        table = athletepage.xpath('//*[@id="results"]/tbody')[2]
    else:
        print("Athlete {} {} ({}) has no volunteer history.  Possibly identified incorrect athlete for {} event {}".format(v['FirstName'], v['LastName'], v['AthleteID'], parkrun, eventnumber))
        table = None
    v['Volunteer'] = {}
    if table is not None:
        for r in table.getchildren():
            if int(r.getchildren()[0].text) == date.year:
                v['Volunteer'][r.getchildren()[1].text] = int(r.getchildren()[2].text)
        athletevol = c.execute("SELECT * FROM qryAthleteVolunteerSummaryByYear WHERE AthleteID = {} AND Year = {}".format(v['AthleteID'], date.year))
        if len(athletevol) > 0:
            for al in athletevol:
                v['Volunteer'][al['VolunteerPosition']] -= al['Count']
                if v['Volunteer'][al['VolunteerPosition']] == 0:
                    del v['Volunteer'][al['VolunteerPosition']]
    if v != volunteers[-1]:
        sleep(2)
    
#Search results for volunteer athlete ID's that appear in the results
req = c.execute("SELECT * FROM VolunteerPositions WHERE CanRun = 1")
l = []
for r in req:
    l.append(r['VolunteerPosition'])

for v in volunteers:
    try:
        a = next(r for r in results if r['AthleteID'] == v['AthleteID'])
        v['Volunteer'] = {k: v['Volunteer'][k] for k in l if k in v['Volunteer']}
    except StopIteration:
        pass

# TODO: Locate tail walkers correctly from the rear of the field

 
#Remove volunteer positions from people who don't appear in the results
req = c.execute("SELECT * FROM VolunteerPositions WHERE MustRun = 1")
l = []
for r in req:
    l.append(r['VolunteerPosition'])

for v in volunteers:
    try:
        a = next(r for r in results if r['AthleteID'] == v['AthleteID'])
    except StopIteration:
        v['Volunteer'] = {k: v['Volunteer'][k] for k in v['Volunteer'] if k not in l}

#Search for vital roles
req = c.execute("SELECT * FROM VolunteerPositions WHERE Required = 1 and VolunteerPosition <> 'Tail Walker'")
l = []
for r in req:
    l.append(r['VolunteerPosition'])

for v in volunteers:
    if len(v['Volunteer']) == 1:
        try:
            l.remove(next(r for r in v['Volunteer'] if r in l))
        except StopIteration:
            pass

removed = True
while removed:
    removed = False
    for r in l:
        for v in volunteers:
            if r in v['Volunteer']:
                print("Assigning {} to {}".format(r, v['AthleteID']))
                v['Volunteer'] = {r: v['Volunteer'][r]}
                removed = True
                l.remove(r)
                break
                
#Search for duplicate positions that are not allowed
req = c.execute("SELECT * FROM VolunteerPositions WHERE AllowMultiple = 0")
l = []
for r in req:
    l.append(r['VolunteerPosition'])

for v in volunteers:
    if any(name in v['Volunteer'] for name in l):
        try:
            p = next(r for r in v['Volunteer'] if r in l and len(v['Volunteer']) > 1)
            print('Deleting {} from {}'.format(p, v['AthleteID']))
            del v['Volunteer'][p]
        except StopIteration:
            pass

# Delete volunteers with empty volunteer lists
volunteers = [x for x in volunteers if len(x['Volunteer']) > 0]

# Any athlete with more than one possible volunteer role: just pick the first one
for v in volunteers:
    if len(v['Volunteer']) > 1:
        v['Volunteer'] = {list(v['Volunteer'].keys())[0] : v['Volunteer'][list(v['Volunteer'].keys())[0]]}
        print("Set {} {} to {}".format(v['FirstName'], v['LastName'], list(v['Volunteer'].keys())[0]))

added = True
while added:
    added = False
    for v in volunteers:
        if len(v['Volunteer']) == 1:
            if len(c.execute("SELECT * FROM VolunteerPositions WHERE VolunteerPosition = '{}'".format(list(v['Volunteer'].keys())[0]))) == 0:
                print("INSERT INTO VolunteerPositions (VolunteerPosition) VALUES ('{}')".format(list(v['Volunteer'].keys())[0]))
                c.execute("INSERT INTO VolunteerPositions (VolunteerPosition) VALUES ('{}')".format(list(v['Volunteer'].keys())[0]))
            if len(c.execute("SELECT * FROM EventVolunteers WHERE EventID = dbo.getEventID('{}', {}) AND AthleteID = {} AND VolunteerPositionID = dbo.getVolunteerID('{}')".format(parkrun, eventnumber, v['AthleteID'], list(v['Volunteer'].keys())[0]))) == 0:
                print("INSERT INTO EventVolunteers (EventID, AthleteID, VolunteerPositionID) VALUES (dbo.getEventID('{}', {}), {}, dbo.getVolunteerID('{}'))".format(parkrun, eventnumber, v['AthleteID'], list(v['Volunteer'].keys())[0]))
                c.execute("INSERT INTO EventVolunteers (EventID, AthleteID, VolunteerPositionID) VALUES (dbo.getEventID('{}', {}), {}, dbo.getVolunteerID('{}'))".format(parkrun, eventnumber, v['AthleteID'], list(v['Volunteer'].keys())[0]))
            added = True
            volunteers = [x for x in volunteers if x['AthleteID'] != v['AthleteID']]
            break

if len(volunteers) > 0:
    print("{} Volunteers remain unassigned".format(len(volunteers)))
    for v in volunteers:
        print(v)
                
    
    

