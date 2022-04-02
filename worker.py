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
    def __init__(self, q, m, i, mode, config, delay, year, volunteer, juniors):
        super(Worker, self).__init__()
        self.inQ = q  #input Queue
        self.msgQ = m  #message queue
        self.id = i
        self.mode = mode
        self.config = config
        self.delay = delay
        self.year = year
        self.volunteer = volunteer
        self.juniors = juniors
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
            #self.logger.debug(parkrun)
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
                    self.logger.debug('Event {} got {} runners'.format(parkrun['EventURL'], len(data)))
                    parkrun['Runners'] = len(data)
                    # Add the event if it's a new event
                    # Check the event has the correct number of runners
                    if not c.checkParkrunEvent(parkrun):
                        self.logger.info('Parkrun {} event {}: runners did not match - reimporting.'.format(parkrun['Name'], parkrun['EventNumber']))
                        #if not, delete the old event record and re-import the data
                        self.msgQ.put(Message('Process', self.id, 'Updating ' + parkrun['Name'] + ' event ' + xstr(parkrun['EventNumber'])))
                        eventID = c.addParkrunEvent(parkrun)
                        self.logger.debug('getLastEvent found {} runners'.format(len(data)))
                        for row in data:
                            row['EventID'] = eventID
                            c.addParkrunEventPosition(row)
                        if self.volunteer:
                            self.logger.info('Parkrun {} event {}: volunteers did not match - downloading.'.format(parkrun['EventURL'], parkrun['EventNumber']))
                            self.msgQ.put(Message('Process', self.id, 'Updating volunteers for ' + parkrun['Name'] + ' event ' + xstr(parkrun['EventNumber'])))
                            self.getVolunteers(self.getURL(parkrun['URL'] + parkrun['LatestResultsURL']), parkrun['EventURL'], parkrun['URL'])
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
                        #self.logger.debug(row)
                        self.logger.debug('Process {} Checking {} event {}'.format(self.id, row['EventURL'], xstr(row['EventNumber'])))
                        # Check the event has the correct number of runners
                        if not c.checkParkrunEvent(row):
                            #if not, delete the old event record and re-import the data
                            self.logger.info('Parkrun {} event {}: runners did not match - reimporting.'.format(parkrun['EventURL'], row['EventNumber']))
                            self.msgQ.put(Message('Process', self.id, 'Updating ' + row['Name'] + ' event ' + xstr(row['EventNumber'])))
                            eventID = c.replaceParkrunEvent(row)
                            self.logger.debug(f'EventID is {eventID}')
                            eData = self.getEvent(parkrun['URL'] + parkrun['EventNumberURL'], row['EventNumber'])
                            if eData is not None:
                                runnersAdded = True
                                self.logger.debug('getEvent found {} runners'.format(len(eData)))
                                for eRow in eData:
                                    eRow['EventID'] = eventID
                                    c.addParkrunEventPosition(eRow)
                                self.logger.debug('Sleeping for {} seconds'.format(self.delay))
                                sleep(self.delay)
                            else:
                                self.logger.debug('getEvent found no runners')
                        if self.volunteer:
                            if not c.checkParkrunVolunteers(row):
                                if row['EventDate'].year == self.year or self.year == 0:
                                    self.logger.info('Parkrun {} event {}: volunteers did not match - downloading.'.format(parkrun['EventURL'], row['EventNumber']))
                                    self.msgQ.put(Message('Process', self.id, 'Updating volunteers for ' + row['Name'] + ' event ' + xstr(row['EventNumber'])))
                                    self.getVolunteers(self.getURL(parkrun['URL'] + parkrun['EventNumberURL'] + str(row['EventNumber'])), parkrun['EventURL'], parkrun['URL'])
                                    self.logger.debug('Sleeping for {} seconds'.format(self.delay))
                                    sleep(self.delay)
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
                if e.code == 500:
                    self.msgQ.put(Message('Error',self.id, 'Server Error ' + url))
                    return None
                self.msgQ.put(Message('Error', self.id, 'Got response {}. retrying in 1 second'.format(e.code)))
                sleep(self.delay)
            except:
                self.logger.warning('Unexpected network error. URL: ' + url)
                self.msgQ.put(Message('Error', self.id, 'Bad URL ' + url))
                return None
        temp = f.read().decode('utf-8', errors='ignore')
        self.logger.debug('URL returned string of length {}'.format(len(temp)))
        return lxml.html.fromstring(temp) 
    
    def getEventTable(self, root):
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
            for h, v in zip(headings, row):
                # 30/10/19 - Remained unchanged
                if h == 'Pos':
                    d['Pos'] = int(v.text)
                
                # 30/10/19 - Age Grade is now included in Age Category cell.  Pull it out there instead.
                #if h == 'Age Grade':
                #    if v.text is not None:
                #        d[h]=float(v.text.split()[0])
                #    else:
                #        d[h]=None
                
                if h == 'parkrunner':
                    if len(v[0].getchildren())>0:
                        data = v[0][0].text
                        if len(data.split()) == 0:
                            # Unnamed athlete
                            d['FirstName'] = 'Unknown'
                            d['LastName'] = None
                            d['AthleteID'] = 0
                            d['Time'] = None
                            d['Age Cat'] = None
                            d['Age Grade'] = None
                            d['Club'] = None
                            d['Note'] = None
                            break
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
                        d['AthleteID'] = int(v[0][0].get('href').split('/')[5])
                    else:
                        # Unknown Athlete
                        d['FirstName'] = 'Unknown'
                        d['LastName'] = None
                        d['AthleteID'] = 0
                        d['Time'] = None
                        d['Age Cat'] = None
                        d['Age Grade'] = None
                        d['Club'] = None
                        d['Note'] = None
                        break
                if h == 'Gender':
                    #30/10/19 - Gender also holds Gender Pos.
                    if len(v[0].text.strip()) > 0:
                        d['Gender'] = v[0].text.strip()[0]
                    else:
                        d['Gender'] = 'M'
                if h == 'Age Cat':
                    if len(v.getchildren())>0:
                        # 30/10/19 - Age Category and Age Grade are now in the same cell
                        d['Age Cat'] = v[0][0].text
                        if len(v.getchildren()) > 1:
                            try:
                                d['Age Grade'] = float(v[1].text.split('%')[0])
                            except:
                                d['Age Grade'] = None
                        else:
                            d['Age Grade'] = None
                    else:
                        d['Age Cat'] = None
                        d['Age Grade'] = None
                if h == 'Club':
                    if len(v.getchildren())>0:
                        if v[0][0].text is not None:
                            d[h] = v[0][0].text.replace("'","''")
                        else:
                            d[h] = None
                    else:
                        d[h] = None
                if h == 'Time':
                    data = v[0].text
                    if data is not None:
                        if len(data)<6:
                            data = '0:' + data
                    d['Time'] = data
                    
                    # 30/11/19 - Note is now inside the Time cell
                    d['Note'] = v[1][0].text
                    #self.logger.debug(len(v[1]))
                    #if len(v) > 1:
                    #    d['Note'] += ' ' + v[1][1].text
            results.append(d)
        if len(results) > 0:
            if 'Pos' not in results[0].keys():
                results = sorted(results, key=lambda k: '0:00:00' if k['Time'] is None else k['Time'])
                for i in range(len(results)):
                    results[i]['Pos'] = i + 1
            return results
        else:
            return None
    
    def getEvent(self, url, parkrunEvent):
        self.logger.debug('Hitting {}'.format(url + str(parkrunEvent)))
        root = self.getURL(url + str(parkrunEvent))
        #self.logger.debug(root)
        #Test if we got a valid response'
        if root is None:  #most likely a 404 error
            self.logger.warning('Error retrieving event')
            self.msgQ.put(Message('Error', self.id, 'Error getting event. Check url ' + url + str(parkrunEvent)))
            return None
        self.logger.debug('GetURL did not return None')
        if len(root.xpath('//*[@id="content"]/h1')) > 0:
            self.logger.warning('Error retrieving event')
            self.msgQ.put(Message('Error', self.id, 'Possible URL error getting event. Check url ' + url + str(parkrunEvent)))
            return None
        self.logger.debug('GetURL did not return an error page')
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
            eventElement = root.xpath('//*[@id="content"]/div[1]/div[1]/h3')[0]
        except IndexError:
            self.logger.warning('Error retrieving event')
            self.msgQ.put(Message('Error', self.id, 'Possible page error retrieving url ' + url))
            return 0, None, None

        #if len(eventHTML.split('#')[1].split('-')[0].strip()) == 0:
        #    return 0, None, None
        eventNumber =  int(eventElement[2].text.split('#')[1].strip())
        eventDate = datetime.strptime(eventElement[0].text,'%d/%m/%Y')
        
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

        table = root.xpath('//*[@id="content"]/div[1]/table')[0]
        headings = ['EventNumber','EventDate','Runners','Volunteers']    
        rows = table.xpath('//tbody/tr')
                
        data = []
        for row in rows:
            if len(row.getchildren()) != 8:
                continue
            else:
                d = {}
                for h, v in zip(headings, row.getchildren()):
                    #print(h)
                    if h == 'EventNumber':
                        d[h] = int(v[0].text)
                    if h in ['Runners','Volunteers']:
                        try:
                            d[h] = int(v.text)
                        except:
                            d[h] = None
                    if h == 'EventDate':
                        d[h] = datetime.strptime(v[0][0][0].text,"%d/%m/%Y")
                data.insert(0,d)
        return data
    
    def getVolunteers(self, root, eventURL, parkrunURL):
        c = Connection(self.config)
        
        if root is None:
            return
        volunteerNames = root.xpath('//*[@id="content"]/div[2]/p[1]')[0].getchildren()
        volunteers = []
        try:
                                  
            parkrun = root.xpath('//*[@id="content"]/div[1]/div[1]/h1')[0].text.strip().split(' parkrun')[0]
            eventnumber = int(root.xpath('//*[@id="content"]/div[1]/div[1]/h3/span[3]')[0].text.strip().split('#')[1].strip().split()[0])
            date = datetime.strptime(root.xpath('//*[@id="content"]/div[1]/div[1]/h3/span[1]')[0].text,'%d/%m/%Y')
        except ValueError:
            self.logger.error("Page error for event {}. Skipping.".format(eventURL))
            return
        
        results = self.getEventTable(root)
        
        if len(volunteerNames) == 0:
            #No volunteer information
            self.logger.warning('{} event {} has no volunteer information'.format(eventURL, eventnumber))
            return
        
        for v in volunteerNames:
            fn = ''
            ln = ''
            nameString = v.text
            n = nameString.split()
            fn  = n[0].replace("'","''").strip()
            del n[0]
            for l in n:
                ln += l + ' ' 
            ln = ln.replace("'","''").strip()
            volunteers.append({'AthleteID': int(v.get('href').split('=')[1]), 'FirstName': fn, 'LastName' : ln, 'NameString':nameString, 'Volunteer':{}})
        
        
        # Check for deleted volunteer credits
        vl = c.execute("SELECT Athletes.AthleteID, FirstName, LastName FROM EventVolunteers  INNER JOIN Athletes on Athletes.AthleteID = EventVolunteers.AthleteID WHERE EventID = dbo.getEventID('{}', {})".format(eventURL, eventnumber))
        for v in vl:
            found = False
            for vn in volunteers:
                if v['AthleteID'] == vn['AthleteID']:
                    found = True
                    break
            if not found:
                self.logger.info('Deleting volunteer credit for {} {} ({})'.format(v['FirstName'], v['LastName'], v['AthleteID']))
                c.execute("DELETE FROM EventVolunteers where AthleteID = {} AND EventID = dbo.getEventID('{}', {})".format(v['AthleteID'],eventURL, eventnumber))

        # Remove athletes that already have volunteered for this event
        vl = c.execute("SELECT Athletes.AthleteID, FirstName, LastName FROM EventVolunteers  INNER JOIN Athletes on Athletes.AthleteID = EventVolunteers.AthleteID WHERE EventID = dbo.getEventID('{}', {})".format(eventURL, eventnumber))
        for v in vl:
            volunteers = [x for x in volunteers if x['AthleteID'] != v['AthleteID']]
            self.logger.info('Already credited {} {} ({})'.format(v['FirstName'], v['LastName'], v['AthleteID']))
        for v in volunteers:
            self.logger.info('Remaining {} {} ({})'.format(v['FirstName'], v['LastName'], v['AthleteID']))
            
        #See if the volunteer roster is still available
        eventRoster = self.getURL(f'{parkrunURL}/futureroster/')
        if eventRoster is not None:
            roster = eventRoster.xpath('//*[@id="rosterTable"]')
            FirstRosterDate = None
            try:
                FirstRosterDate = datetime.strptime(roster[0][0][0][1].text,'%d %B %Y')
            except:
                pass
            if FirstRosterDate == date:
                #Roster for today is available
                self.logger.info('Downloading event roster')
                for i in range(len(roster[0][1])):
                    #Some positions on the roster are empty.  Ignore these.
                    if roster[0][1][i][1].text is not None:
                        for v in volunteers:
                            if v['NameString'] == roster[0][1][i][1].text:
                                v['Volunteer'][roster[0][1][i][0][0].text] = 1
                                  
            else:
                #Roster is not available, do it the old way
                # Retrieve all remaining volunteer stats and remove accounted stats.
                for v in volunteers:
                    v['Volunteer'] = {}
                    table = None
                    if v['AthleteID'] != 0:
                        found = False
                        retry = 0
                        athletepage = None
                        while not found and retry < 3:
                            athletepage = self.getURL('https://www.parkrun.com.au/parkrunner/{}/'.format(v['AthleteID']))
                            if athletepage is not None:
                                found = True
                            else:
                                retry += 1
                                self.logger.warning("URL Error for athlete {} on attempt {}".format(v['AthleteID'], retry))
                                self.logger.debug('Sleeping {} seconds...'.format(self.delay))
                                sleep(self.delay)
                        if athletepage is None:
                            self.logger.error("Failed to retrieve athlete stats for {} {} ({}), Skipping".format(v['FirstName'], v['LastName'], v['AthleteID']))
                            continue
                        if len(athletepage.xpath('//*[@id="results"]/tbody')) > 2:
                            if len(athletepage.xpath('//*[@id="content"]/div[3]')[0].getchildren()) > 0:
                                if athletepage.xpath('//*[@id="content"]/div[3]')[0].getchildren()[0].text == 'Volunteer Summary':
                                    table = athletepage.xpath('//*[@id="results"]/tbody')[2]
                                else:
                                    self.logger.warning("Athlete {} {} ({}) has no volunteer history.  Possibly identified incorrect athlete for {} event {}".format(v['FirstName'], v['LastName'], v['AthleteID'], eventURL, eventnumber))
                            else:
                                self.logger.warning("Athlete {} {} ({}) has no volunteer history.  Possibly identified incorrect athlete for {} event {}".format(v['FirstName'], v['LastName'], v['AthleteID'], eventURL, eventnumber))
                        if v != volunteers[-1]:
                            self.logger.debug('Sleeping {} seconds...'.format(self.delay))
                            sleep(self.delay)
                    if table is not None:
                        for r in table:
                            #if int(r.getchildren()[0].text) == date.year:
                                v['Volunteer'][r[0].text.strip()] = int(r[1].text)
                        athletevol = c.execute("SELECT * FROM qryAthleteVolunteerSummary WHERE AthleteID = {}".format(v['AthleteID']))
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
                                self.logger.debug(a)
                                if 'Tail Walker' in v['Volunteer']:
                                    v['Volunteer'] = {'Tail Walker': v['Volunteer']['Tail Walker']}
                                    results = results[:-1]
                                    self.logger.info('Setting {} {} to Tail Walker'.format(v['FirstName'], v['LastName']))
                                    found = True
                            except StopIteration:
                                if 'Tail Walker' in v['Volunteer'] and len(v['Volunteer']) > 1:
                                    del v['Volunteer']['Tail Walker']
                                    self.logger.debug('Deleting Tail Walker from {} {}'.format(v['FirstName'], v['LastName']))
                
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
                                self.logger.info("Setting {} to {} {}".format(r, v['FirstName'], v['LastName']))
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
                                self.logger.info("Position {} at {} event {} has been filled by Unknown Athlete {} {}.".format(x, eventURL, eventnumber, v['FirstName'], v['LastName']))
                                found = True
                                l.remove(x)
                                break
                        if found:
                            break
                        else:
                            self.logger.warning("Position {} at {} event {} has not been filled. Investigate".format(x, eventURL, eventnumber))
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
                            self.logger.debug('Deleting {} from {} {}'.format(p, v['FirstName'], v['LastName']))
                            del v['Volunteer'][p]
                        except StopIteration:
                            pass
                
                # Delete volunteers with empty volunteer lists
                for v in volunteers:
                    if len(v['Volunteer']) == 0:
                        self.logger.warning("Athlete {} {} ({}) did not get a volunteer position for {} event {}. Investigate".format(v['FirstName'], v['LastName'], v['AthleteID'], eventURL, eventnumber))
                        v['Volunteer']['Unknown'] = 1
                
            # Any athlete with more than one possible volunteer role: just pick the first one
            for v in volunteers:
                if len(v['Volunteer']) > 1:
                    v['Volunteer'] = {list(v['Volunteer'].keys())[0] : v['Volunteer'][list(v['Volunteer'].keys())[0]]}
                    self.logger.info("Setting {} {} to {}".format(v['FirstName'], v['LastName'], list(v['Volunteer'].keys())[0]))
            
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
                            self.logger.info("Adding volunteer position {}".format(list(v['Volunteer'].keys())[0]))
                            c.execute("INSERT INTO VolunteerPositions (VolunteerPosition) VALUES ('{}')".format(list(v['Volunteer'].keys())[0]))
                        if len(c.execute("SELECT * FROM EventVolunteers WHERE EventID = dbo.getEventID('{}', {}) AND AthleteID = {} AND VolunteerPositionID = dbo.getVolunteerID('{}')".format(eventURL, eventnumber, v['AthleteID'], list(v['Volunteer'].keys())[0]))) == 0:
                            self.logger.info("Adding {} {} as {}".format(v['FirstName'],v['LastName'], list(v['Volunteer'].keys())[0]))
                            c.execute("INSERT INTO EventVolunteers (EventID, AthleteID, VolunteerPositionID) VALUES (dbo.getEventID('{}', {}), {}, dbo.getVolunteerID('{}'))".format(eventURL, eventnumber, v['AthleteID'], list(v['Volunteer'].keys())[0]))
                        added = True
                        volunteers = [x for x in volunteers if x['AthleteID'] != v['AthleteID']]
                        break
            
            if len(volunteers) > 0:
                self.logger.warning("{} Volunteers remain unassigned:".format(len(volunteers)))
                for v in volunteers:
                    self.logger.warning(v)
            
            



