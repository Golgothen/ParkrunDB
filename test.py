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
    
    table = root.xpath('//*[@id="content"]/div[1]/table')[0]
    
    headings = ['Pos','parkrunner','Gender','Age Cat','Club','Time']#,'Age Grade','Gender Pos','Note',] #'Strava']
    
    rows = table.xpath('//tbody/tr')
    
    # 30/10/19 - Changes to parkrun website has required an overhaul of this code...
    
    # 30/10/19 - Tables now only have 6 cells.  Untested on international sites
    #if len(rows) > 0:
    #    if len(rows[0].getchildren()) < 11:  # France results have no position or Gender position columns
    #        headings = ['parkrunner','Time','Age Cat','Age Grade','Gender','Club','Note']#,'Strava']
    #else:
    #    headings = ['Pos','parkrunner','Time','Age Cat','Age Grade','Gender','Gender Pos','Club','Note'] #,'Strava']
    results = []
    
    for row in rows:
        d = {}
        for h, v in zip(headings, row.getchildren()):
            # 30/10/19 - Remained unchanged
            if h == 'Pos':
                d['Pos'] = int(v.text)
                print(d)
            
            # 30/10/19 - Age Grade is now included in Age Category cell.  Pull it out there instead.
            #if h == 'Age Grade':
            #    if v.text is not None:
            #        d[h]=float(v.text.split()[0])
            #    else:
            #        d[h]=None
            
            if h == 'parkrunner':
                if len(v.getchildren()[0].getchildren())>0:
                    data = v.getchildren()[0].getchildren()[0].text
                    d['FirstName'] = data.split()[0].replace("'","''")
                    lastName = ''
                    l = data.split()
                    l.pop(0)
                    for i in l:
                        lastName += i.capitalize().replace("'","''") + ' '
                    if lastName != '':
                        d['LastName'] = lastName.strip()
                    else:
                        d['LastName'] = ''
                    d['AthleteID'] = int(v.getchildren()[0].getchildren()[0].get('href').split('=')[1])
                else:
                    # Unknown Athlete
                    # 30/10/19 - Untested!
                    d['FirstName'] = 'Unknown'
                    d['LastName'] = None
                    d['AthleteID'] = 0
                    d['Time'] = None
                    d['Age Cat'] = None
                    d['Age Grade'] = None
                    d['Club'] = None
                    d['Note'] = None
                    break
                print(d)
            if h == 'Gender':
                #30/10/19 - Gender also holds Gender Pos.
                if v.getchildren()[0].text.strip() is not None:
                    d['Gender'] = v.getchildren()[0].text.strip()[0]
                else:
                    d['Gender']='M'
                print(d)
            if h == 'Age Cat':
                if len(v.getchildren())>0:
                    # 30/10/19 - Age Category and Age Grade are now in the same cell
                    d['Age Cat'] = v.getchildren()[0].getchildren()[0].text
                    if len(v.getchildren()) > 1:
                        d['Age Grade'] = float(v.getchildren()[1].text.split('%')[0])
                    else:
                        d['Age Grade'] = None
                else:
                    d['Age Cat'] = None
                    d['Age Grade'] = None
                print(d)
            if h == 'Club':
                if len(v.getchildren())>0:
                    if v.getchildren()[0].getchildren()[0].text is not None:
                        d[h]=v.getchildren()[0].getchildren()[0].text.replace("'","''")
                    else:
                        d['Club'] = None
                else:
                    d['Club'] = None
                print(d)
            if h == 'Time':
                data = v.getchildren()[0].text
                if data is not None:
                    if len(data)<6:
                        data = '0:' + data
                d['Time'] = data
                
                # 30/11/19 - Note is now inside the Name cell
                d['Note'] = v.getchildren()[1].getchildren()[0].text
                print(d)
        results.append(d)
    if len(results) > 0:
        if 'Pos' not in results[0].keys():
            results = sorted(results, key=lambda k: '0:00:00' if k['Time'] is None else k['Time'])
            for i in range(len(results)):
                results[i]['Pos'] = i + 1
        return results
    else:
        return None

c = Connection(sender_config)

def printv():
    for v in volunteers:
        print(v)
    print(len(volunteers))

root = getURL('https://www.parkrun.com.au/railwaypark/results/weeklyresults/?runSeqNumber=1')
eventURL = 'railwaypark'

#def getVolunteers(root):
    
#c = Connection(config)


        
if root is None:
    return

    
volunteerNames = root.xpath('//*[@id="content"]/div[2]/p[1]')[0].getchildren()
volunteers = []
try:
                          
    parkrun = root.xpath('//*[@id="content"]/div[1]/div[1]/h1')[0].text.strip().split(' parkrun')[0]
    eventnumber = int(root.xpath('//*[@id="content"]/div[1]/div[1]/h3/span[2]')[0].text.strip().split('#')[1].strip().split()[0])
    date = datetime.strptime(root.xpath('//*[@id="content"]/div[1]/div[1]/h3')[0].text,'%d/%m/%Y')
except ValueError:
    print("Page error for event {}. Skipping.".format(eventURL))
    return

results = getEventTable(root)

if len(volunteerNames) == 0:
    #No volunteer information
    print('{} event {} has no volunteer information'.format(eventURL, eventnumber))
    return

for v in volunteerNames:
    fn = ''
    ln = ''
    n = v.text.split()
    fn  = n[0]
    del n[0]
    for l in n:
        ln += l + ' ' 
    fn = fn.replace("'","''").strip()
    ln = ln.replace("'","''").strip()
    volunteers.append({'AthleteID': int(v.get('href').split('=')[1]), 'FirstName': fn, 'LastName' : ln})

# Remove athletes that already have volunteered for this event
vl = c.execute("SELECT Athletes.AthleteID, FirstName, LastName FROM EventVolunteers  INNER JOIN Athletes on Athletes.AthleteID = EventVolunteers.AthleteID WHERE EventID = dbo.getEventID('{}', {})".format(eventURL, eventnumber))
for v in vl:
    volunteers = [x for x in volunteers if x['AthleteID'] != v['AthleteID']]
    print('Deleting {} {} ({})'.format(v['FirstName'], v['LastName'], v['AthleteID']))

# Retrieve all remaining volunteer stats and remove accounted stats.
for v in volunteers:
    v['Volunteer'] = {}
    table = None
    if v['AthleteID'] != 0:
        found = False
        retry = 0
        athletepage = None
        while not found and retry < 3:
            athletepage = getURL('https://www.parkrun.com.au/results/athleteresultshistory/?athleteNumber={}'.format(v['AthleteID']))
            if athletepage is not None:
                found = True
            else:
                retry += 1
                print("URL Error for athlete {} on attempt {}".format(v['AthleteID'], retry))
                print('Sleeping {} seconds...'.format(2))
                sleep(2)
        if athletepage is None:
            print("Failed to retrieve athlete stats for {} {} ({}), Skipping".format(v['FirstName'], v['LastName'], v['AthleteID']))
            continue
        if len(athletepage.xpath('//*[@id="results"]/tbody')) > 2:
            if len(athletepage.xpath('//*[@id="content"]/div[3]')[0].getchildren()) > 0:
                if athletepage.xpath('//*[@id="content"]/div[3]')[0].getchildren()[0].text == 'Volunteer Summary':
                    table = athletepage.xpath('//*[@id="results"]/tbody')[2]
                else:
                    print("Athlete {} {} ({}) has no volunteer history.  Possibly identified incorrect athlete for {} event {}".format(v['FirstName'], v['LastName'], v['AthleteID'], eventURL, eventnumber))
            else:
                print("Athlete {} {} ({}) has no volunteer history.  Possibly identified incorrect athlete for {} event {}".format(v['FirstName'], v['LastName'], v['AthleteID'], eventURL, eventnumber))
        if v != volunteers[-1]:
            print('Sleeping {} seconds...'.format(2))
            sleep(2)
    if table is not None:
        for r in table.getchildren():
            if int(r.getchildren()[0].text) == date.year:
                v['Volunteer'][r.getchildren()[1].text] = int(r.getchildren()[2].text)
        athletevol = c.execute("SELECT * FROM qryAthleteVolunteerSummaryByYear WHERE AthleteID = {} AND Year = {}".format(v['AthleteID'], date.year))
        if len(athletevol) > 0:
            for al in athletevol:
                if al['VolunteerPosition'] in v['Volunteer']:
                    v['Volunteer'][al['VolunteerPosition']] -= al['Count']
                    if v['Volunteer'][al['VolunteerPosition']] == 0:
                        del v['Volunteer'][al['VolunteerPosition']]

if results is not None:
    #Search results for volunteer athlete ID's that appear in the results
    req = c.execute("SELECT * FROM VolunteerPositions WHERE CanRun = 1")
    l = []
    for r in req:
        l.append(r['VolunteerPosition'])
    
    for v in volunteers:
        try:
            a = next(r for r in results if r['AthleteID'] == v['AthleteID'])
            if v['AthleteID'] != 0:
                v['Volunteer'] = {k: v['Volunteer'][k] for k in l if k in v['Volunteer']}
        except StopIteration:
            pass
    #Remove volunteer positions from people who don't appear in the results
    req = c.execute("SELECT * FROM VolunteerPositions WHERE MustRun = 1")
    l = []
    for r in req:
        l.append(r['VolunteerPosition'])
    
    for v in volunteers:
        try:
            a = next(r for r in results if r['AthleteID'] == v['AthleteID'])
            #self.logger.debug(a)
        except StopIteration:
            v['Volunteer'] = {k: v['Volunteer'][k] for k in v['Volunteer'] if k not in l}
    # Locate tail walkers correctly from the rear of the field
    found = True
    while found:
        found = False
        for v in volunteers:
            try:
                a = next(r for r in results if r['AthleteID'] == v['AthleteID'] and r['Pos'] == len(results))
                print(a)
                if 'Tail Walker' in v['Volunteer']:
                    v['Volunteer'] = {'Tail Walker': v['Volunteer']['Tail Walker']}
                    results = results[:-1]
                    print('Setting {} {} to Tail Walker'.format(v['FirstName'], v['LastName']))
                    found = True
            except StopIteration:
                if 'Tail Walker' in v['Volunteer'] and len(v['Volunteer']) > 1:
                    del v['Volunteer']['Tail Walker']
                    print('Deleting Tail Walker from {} {}'.format(v['FirstName'], v['LastName']))

#Search for vital roles
req = c.execute("SELECT VolunteerPosition FROM VolunteerPositions WHERE VolunteerPositions.Required = 1 AND VolunteerPositionID NOT IN (SELECT VolunteerPositionID FROM EventVolunteers WHERE EventID = dbo.getEventID('{}', {}))".format(eventURL, eventnumber))
l = []
for r in req:
    l.append(r['VolunteerPosition'])

for v in volunteers:
    if len(v['Volunteer']) == 1:
        try:
            l.remove(next(r for r in v['Volunteer'] if r in l))
        except StopIteration:
            pass

found = True
while found:
    found = False
    for r in l:
        for v in volunteers:
            if r in v['Volunteer']:
                print("Setting {} to {} {}".format(r, v['FirstName'], v['LastName']))
                v['Volunteer'] = {r: v['Volunteer'][r]}
                found = True
                l.remove(r)
                for y in volunteers:
                    if y['AthleteID'] != v['AthleteID']:
                        if r in y['Volunteer']:
                            del y['Volunteer'][r]
                break

while len(l) > 0:
    for x in l:
        found = False
        for v in volunteers:
            if len(v['Volunteer']) == 0:# and v['AthleteID'] != 0:
                v['Volunteer'] = {x : 1}
                print("Position {} at {} event {} has been filled by Unknown Athlete {} {}.".format(x, eventURL, eventnumber, v['FirstName'], v['LastName']))
                found = True
                l.remove(x)
                break
        if found:
            break
        else:
            print("Position {} at {} event {} has not been filled. Investigate".format(x, eventURL, eventnumber))
            l.remove(x)
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
            print('Deleting {} from {} {}'.format(p, v['FirstName'], v['LastName']))
            del v['Volunteer'][p]
        except StopIteration:
            pass

# Delete volunteers with empty volunteer lists
for v in volunteers:
    if len(v['Volunteer']) == 0:
        print("Athlete {} {} ({}) did not get a volunteer position for {} event {}. Investigate".format(v['FirstName'], v['LastName'], v['AthleteID'], eventURL, eventnumber))
        v['Volunteer']['Unknown'] = 1

# Any athlete with more than one possible volunteer role: just pick the first one
for v in volunteers:
    if len(v['Volunteer']) > 1:
        v['Volunteer'] = {list(v['Volunteer'].keys())[0] : v['Volunteer'][list(v['Volunteer'].keys())[0]]}
        print("Setting {} {} to {}".format(v['FirstName'], v['LastName'], list(v['Volunteer'].keys())[0]))

# Append the results
added = True
while added:
    added = False
    for v in volunteers:
        if len(c.execute("SELECT * FROM Athletes WHERE AthleteID = {}".format(v['AthleteID']))) == 0:
            v['Gender'] = 'M'
            v['Age Cat'] = None
            v['Club'] = None
            c.addAthlete(v)
        if len(v['Volunteer']) == 1:
            if len(c.execute("SELECT * FROM VolunteerPositions WHERE VolunteerPosition = '{}'".format(list(v['Volunteer'].keys())[0]))) == 0:
                print("Adding volunteer position {}".format(list(v['Volunteer'].keys())[0]))
                c.execute("INSERT INTO VolunteerPositions (VolunteerPosition) VALUES ('{}')".format(list(v['Volunteer'].keys())[0]))
            if len(c.execute("SELECT * FROM EventVolunteers WHERE EventID = dbo.getEventID('{}', {}) AND AthleteID = {} AND VolunteerPositionID = dbo.getVolunteerID('{}')".format(eventURL, eventnumber, v['AthleteID'], list(v['Volunteer'].keys())[0]))) == 0:
                print("Adding {} {} as {}".format(v['FirstName'],v['LastName'], list(v['Volunteer'].keys())[0]))
                c.execute("INSERT INTO EventVolunteers (EventID, AthleteID, VolunteerPositionID) VALUES (dbo.getEventID('{}', {}), {}, dbo.getVolunteerID('{}'))".format(eventURL, eventnumber, v['AthleteID'], list(v['Volunteer'].keys())[0]))
            added = True
            volunteers = [x for x in volunteers if x['AthleteID'] != v['AthleteID']]
            break

if len(volunteers) > 0:
    print("{} Volunteers remain unassigned:".format(len(volunteers)))
    for v in volunteers:
        print(v)
