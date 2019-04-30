from mplogger import *

from dbconnection import Connection
from urllib.request import urlopen, Request #, localhost
from urllib.error import HTTPError
from time import sleep

#from parkrunlist import ParkrunList
#from worker import *

import logging, logging.config, multiprocessing, lxml.html


def getURL(url):
    completed = False
    while not completed:
        try:
            #logger.debug('Hitting {}'.format(url))
            f = urlopen(Request(url, data=None, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'}))
            completed = True
        except HTTPError as e:
            #logger.warning('Got HTTP Error {}'.format(e.code))
            if e.code == 404:
                #self.msgQ.put(Message('Error',self.id, 'Bad URL ' + url))
                return None
            if e.code == 403:
                #self.msgQ.put(Message('Error',self.id, 'Forbidden ' + url))
                return None
            #self.msgQ.put(Message('Error', self.id, 'Got response {}. retrying in 1 second'.format(e.code)))
            sleep(1)
        except:
            #logger.warning('Unexpected network error. URL: ' + url)
            #self.msgQ.put(Message('Error', self.id, 'Bad URL ' + url))
            return None
    temp = f.read().decode('utf-8', errors='ignore')
    #logger.debug('URL returned string of length {}'.format(len(temp)))
    return temp 

def getEvent(url, parkrunEvent):
    #self.logger.debug('Hitting {}'.format(url + str(parkrunEvent)))
    html = getURL(url + str(parkrunEvent))
    #Test if we got a valid response'
    if html is None:  #most likely a 404 error
        #self.logger.warning('Error retrieving event')
        #self.msgQ.put(Message('Error', self.id, 'Error getting event. Check url ' + url))
        return None
    if '<h1>Something odd has happened, so here are the most first finishers</h1>' in html:  
        #self.logger.warning('Error retrieving event')
        #self.msgQ.put(Message('Error', self.id, 'Possible URL error getting event. Check url ' + url))
        return None
    html = '<table' + html.split('<table')[1].split('</p>')[0]
    table = lxml.html.fromstring(html)
    return getEventTable(table)

def getEventTable(tableHTML):
    headings = ['Pos','parkrunner','Time','Age Cat','Age Grade','Gender','Gender Pos','Club','Note','Strava']
    
    rows = tableHTML.xpath('//tbody/tr')
    
    if len(rows) > 0:
        if len(rows[0].getchildren()) < 11:  # France results have no position or Gender position columns
            headings = ['parkrunner','Time','Age Cat','Age Grade','Gender','Club','Note','Strava']
    else:
        headings = ['Pos','parkrunner','Time','Age Cat','Age Grade','Gender','Gender Pos','Club','Note','Strava']
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


if __name__ == '__main__':
    
    config = sender_config
    c = Connection(config)
    data = c.execute("select * from getAthleteCheckHistoryList(40) ORDER BY EventCount DESC")
    baseURL = "http://www.parkrun.com.au/results/athleteeventresultshistory/?athleteNumber={}&eventNumber=0"

    for athlete in data:
        athlete['EventCount'] = c.execute("SELECT dbo.getAthleteEventCountWithoutJuniors({})".format(athlete['AthleteID']))
        print("Checking ID {}, {} {} ({})".format(athlete['AthleteID'], athlete['FirstName'], athlete['LastName'], athlete['EventCount']), end='', flush=True)
        html = getURL(baseURL.format(athlete['AthleteID']))
        runcount = int(html.split('<h2>')[1].split('<br/>')[1].split(' parkruns')[0])
        if athlete['EventCount'] != runcount:
            eventsMissing = runcount - athlete['EventCount']
            print("\nMissing {} runs".format(eventsMissing))
            rows = rows = lxml.html.fromstring('<table' + html.split('<table')[3].split('</table>')[0] + '</table>').xpath('//tbody/tr')
            hist_data = c.execute("SELECT * FROM getAthleteEventHistory({})".format(athlete['AthleteID']))
            if eventsMissing > 0:
                for row in rows:  # Iterate through the events in the summary table
                    currentParkrun = row[0][0].get('href').split('/')[3]
                    currentParkrunName = row[0][0].text.split(' parkrun')[0]
                    currentDate = datetime.strptime(row[1][0].text,"%d/%m/%Y")
                    eventNumber = int(row[2][0].get('href').split('=')[1])
                    found = False
                    for d in hist_data:
                        if d['URL'] == currentParkrun and d['EventNumber'] == eventNumber:
                            found = True
                            break
                    if not found:
                        print("Missed event {} for parkrun {}".format(eventNumber, currentParkrunName))
                        eventURL = c.execute("SELECT dbo.getEventURL('{}')".format(currentParkrun))
                        if eventURL is not None:
                            event_data = getEvent(eventURL, eventNumber)
                            eventID = c.replaceParkrunEvent({'Name': currentParkrun, 'EventNumber': eventNumber, 'EventDate': currentDate})
                            if event_data is not None:
                                for edata in event_data:
                                    edata['EventID'] = eventID
                                    c.addParkrunEventPosition(edata)
                                print("Reloaded event {} for parkrun {}".format(eventNumber, currentParkrunName))
                                eventsMissing -= 1 
                        else:
                            print("Possible new event {} - URL {}. Investigate and retry.".format(currentParkrunName, row[0][0].get('href')))
                if eventsMissing == 0:
                    c.execute("UPDATE Athletes SET HistoryLastChecked = GETDATE() WHERE AthleteID = " + str(athlete['AthleteID']))
                    print("Athlete {} {}, {} run count OK.".format(athlete['FirstName'], athlete['LastName'], athlete['AthleteID']))
            else:
                # Event has been deleted from athlete history.  Find out which one
                for d in hist_data:
                    found = False
                    for row in rows:
                        if d['URL'] == row.getchildren()[0].getchildren()[0].get('href').split('/')[3] and \
                           d['EventNumber'] == int(row.getchildren()[2].getchildren()[0].get('href').split('=')[1]):
                            found = True
                            break
                    if not found:
                        print("\nDeleted event {} for parkrun {}".format(d['EventNumber'], d['ParkrunName']))
                        event_data = getEvent(c.execute("SELECT dbo.getEventURL('{}')".format(d['URL'])),d['EventNumber'])
                        eventID = c.replaceParkrunEvent({'Name': d['URL'], 'EventNumber': d['EventNumber'], 'EventDate': d['EventDate']})
                        if event_data is not None:
                            for edata in event_data:
                                edata['EventID'] = eventID
                                c.addParkrunEventPosition(edata)
                            print("Reloaded event {} for parkrun {}".format(d['EventNumber'], d['ParkrunName']))
                            eventsMissing += 1
                if eventsMissing == 0:
                    c.execute("UPDATE Athletes SET HistoryLastChecked = GETDATE() WHERE AthleteID = " + str(athlete['AthleteID']))
                    print("Athlete {} {}, {} run count OK.".format(athlete['FirstName'], athlete['LastName'], athlete['AthleteID']))
        else:
            c.execute("UPDATE Athletes SET HistoryLastChecked = GETDATE() WHERE AthleteID = " + str(athlete['AthleteID']))
            print(" run count OK.")
        sleep(3)
