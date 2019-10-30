from mplogger import *

from dbconnection import Connection
from urllib.request import urlopen, Request #, localhost
from urllib.error import HTTPError
from time import sleep
from timeit import default_timer as timer
from parkrunlist import ParkrunList
from worker import *

import logging, logging.config, multiprocessing, lxml.html, argparse

intervals = (
    ('weeks', 604800),  # 60 * 60 * 24 * 7
    ('days', 86400),    # 60 * 60 * 24
    ('hours', 3600),    # 60 * 60
    ('minutes', 60),
    ('seconds', 1),
    )

def display_time(seconds, granularity=4):
    result = []

    for name, count in intervals:
        value = seconds // count
        if value:
            seconds -= value * count
            if value == 1:
                name = name.rstrip('s')
            result.append("{} {}".format(value, name))
    return ', '.join(result[:granularity])

def getURL(url):
    completed = False
    while not completed:
        try:
            logger.debug('Hitting {}'.format(url))
            f = urlopen(Request(url, data=None, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'}))
            completed = True
        except HTTPError as e:
            logger.warning('Got HTTP Error {}'.format(e.code))
            if e.code == 404:
                #self.msgQ.put(Message('Error',self.id, 'Bad URL ' + url))
                return None
            if e.code == 403:
                #self.msgQ.put(Message('Error',self.id, 'Forbidden ' + url))
                raise
                #return None
            #self.msgQ.put(Message('Error', self.id, 'Got response {}. retrying in 1 second'.format(e.code)))
            sleep(1)
        except:
            logger.warning('Unexpected network error. URL: ' + url)
            #self.msgQ.put(Message('Error', self.id, 'Bad URL ' + url))
            return None
    temp = f.read().decode('utf-8', errors='ignore')
    logger.debug('URL returned string of length {}'.format(len(temp)))
    return temp 

def getEvent(url, parkrunEvent):
    logger.debug('Hitting {}'.format(url + str(parkrunEvent)))
    html = getURL(url + str(parkrunEvent))
    #Test if we got a valid response'
    if html is None:  #most likely a 404 error
        logger.warning('Error retrieving event')
        #self.msgQ.put(Message('Error', self.id, 'Error getting event. Check url ' + url))
        return None
    if '<h1>Something odd has happened, so here are the most first finishers</h1>' in html:  
        logger.warning('Error retrieving event')
        #self.msgQ.put(Message('Error', self.id, 'Possible URL error getting event. Check url ' + url))
        return None
    html = '<table' + html.split('<table')[1].split('</p>')[0]
    table = lxml.html.fromstring(html)
    return getEventTable(table)

def getEventTable(root):

    table = root.xpath('//*[@id="content"]/div[2]/table')[0]
    
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
                d[h] = int(v.text)
            
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
                    break
            if h == 'Gender':
                #30/10/19 - Gender also holds Gender Pos.
                if v.getchildren()[0].text.strip() is not None:
                    d[h]=v.getchildren()[0].text.strip()[0]
                else:
                    d[h]='M'
            if h == 'Age Cat':
                if len(v.getchildren())>0:
                    # 30/10/19 - Age Category and Age Grade are now in the same cell
                    d['Age Cat'] = v.getchildren()[0].getchildren()[0].text
                    d['Age Grade'] = float(v.getchildren()[1].text.split('%')[0])
                else:
                    d['Age Cat'] = None
                    d['Age Grade'] = None
            if h == 'Club':
                if len(v.getchildren())>0:
                    if v.getchildren()[0].getchildren()[0].text is not None:
                        d[h]=v.getchildren()[0].getchildren()[0].text.replace("'","''")
                    else:
                        d[h]=None
                else:
                    d[h]=None
            if h == ['Time']:
                data = v.getchildren()[0].text
                if data is not None:
                    if len(data)<6:
                        data = '0:' + data
                d[h] = data
                
                # 30/11/19 - Note is now inside the Name cell
                d['Note'] = v.getchildren()[1].getchildren()[0].text
        results.append(d)
    if len(results) > 0:
        if 'Pos' not in results[0].keys():
            results = sorted(results, key=lambda k: '0:00:00' if k['Time'] is None else k['Time'])
            for i in range(len(results)):
                results[i]['Pos'] = i + 1
        return results
    else:
        return None


if __name__ == '__main__':
    
    loggingQueue = multiprocessing.Queue()

    listener = LogListener(loggingQueue)
    listener.start()
    
    config = sender_config
    config['handlers']['queue']['queue'] = loggingQueue
    logging.config.dictConfig(config)
    logger = logging.getLogger('checkhistory')

    c = Connection(config)

    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type = int, default = 40, help = 'Specify number of events and athlete needs to have completed to be selected.')
    parser.add_argument('--delay', type = int, default = 5, help = 'Wait n seconds before processing the next athlete')
    
    args = parser.parse_args()
    
    logger.debug(args)
    
    limit = args.limit
    delay = args.delay
    
    
    
    data = c.execute("select * from getAthleteCheckHistoryList({}) ORDER BY EventCount DESC, NextCheckDate, HistoryLastChecked DESC, AthleteID".format(limit))
    baseURL = "http://www.parkrun.com.au/results/athleteeventresultshistory/?athleteNumber={}&eventNumber=0"
    counter = 0
    start = timer()
    
    inQ = multiprocessing.Queue()
    outQ = multiprocessing.Queue()
    
    worker = Worker(inQ, outQ, 0, Mode.NORMAL, config, 10, 0, False, False)
    worker.start()
    
    try:
        for athlete in data:
            tick = timer()
            while not outQ.empty():
                logger.debug(outQ.get(False))
            athlete['EventCount'] = c.execute("SELECT dbo.getAthleteEventCount({})".format(athlete['AthleteID']))
            logger.debug("Checking ID {}, {} {} ({})".format(athlete['AthleteID'], athlete['FirstName'], athlete['LastName'], athlete['EventCount']))
            html = getURL(baseURL.format(athlete['AthleteID']))
            try:
                runcount = int(html.split('<h2>')[1].split('<br/>')[0].split(' runs at All Events')[0].split(' ')[-1])
                logger.debug("Runcount = {}".format(runcount))
            except (ValueError, IndexError, AttributeError):
                print("Error reading run count for Athlete {}".format(athlete['AthleteID']))
                logger.warning("Error reading run count for Athlete {}".format(athlete['AthleteID']))
                continue
            if athlete['EventCount'] != runcount:
                eventsMissing = runcount - athlete['EventCount']
                rows = lxml.html.fromstring('<table' + html.split('<table')[3].split('</table>')[0] + '</table>').xpath('//tbody/tr')
                hist_data = c.execute("SELECT * FROM getAthleteEventHistory({})".format(athlete['AthleteID']))
                if eventsMissing > 0:
                    logger.debug("Athlete {} Missing {} runs".format(athlete['AthleteID'], eventsMissing))
                    for row in rows:  # Iterate through the events in the summary table
                        parkrun = {}
                        position = {}
                        try:
                            parkrun['EventURL'] = row[0][0].get('href').split('/')[3]
                            parkrun['Name'] = row[0][0].text.split(' parkrun')[0]
                            parkrun['CountryURL'] = 'http://' + row[0][0].get('href').split('/')[2] + '/'
                            parkrun['EventDate'] = datetime.strptime(row[1][0].text,"%d/%m/%Y")
                            parkrun['EventNumber'] = int(row[2][0].get('href').split('=')[1])
                            
                            position['AthleteID'] = athlete['AthleteID']
                            position['Pos'] = int(row[3].text)
                            position['Time'] = row[4].text
                            if len(position['Time'])<6:
                                position['Time'] = '00:' + position['Time'] 
                            position['Age Cat'] = None
                            if row[5].text is not None:
                                position['Age Grade'] = row[5].text[:-1]
                            else:
                                position['Age Grade'] = None
                            position['Note'] = None
                        except TypeError:
                            print("Error reading parkrun data {}, {} for Athlete {}".format(parkrun['EventURL'], parkrun['EventNumber'], athlete['AthleteID']))
                            logger.warning("Error reading parkrun data {}, {} for Athlete {}".format(parkrun['EventURL'], parkrun['EventNumber'], athlete['AthleteID']))
                            continue
                        logger.debug(parkrun)
                        logger.debug(position)
                        found = False
                        for d in hist_data:
                            if d['URL'] == parkrun['EventURL'] and d['EventNumber'] == parkrun['EventNumber']:
                                found = True
                                break
                        if not found:
                            logger.debug("Missed event {} for parkrun {}".format(parkrun['EventNumber'], parkrun['EventURL']))
                            parkrunType = c.execute("SELECT dbo.getParkrunType('{}')".format(parkrun['EventURL']))
                            if parkrunType == 'Special':
                                logger.debug("Special Event detected")
                                position['EventID'] = c.execute("SELECT dbo.getEventID('{}',{})".format(parkrun['EventURL'], parkrun['EventNumber']))
                                if position['EventID'] is None:
                                    position['EventID'] = c.addParkrunEvent(parkrun)
                                c.addParkrunEventPosition(position, False)
                            else:
                                eventURL = c.execute("SELECT dbo.getEventURL('{}')".format(parkrun['EventURL']))
                                if eventURL is not None:
                                    event_data = getEvent(eventURL, parkrun['EventNumber'])
                                    eventID = c.replaceParkrunEvent({'EventURL': parkrun['EventURL'], 'EventNumber': parkrun['EventNumber'], 'EventDate': parkrun['EventDate']})
                                    if event_data is not None:
                                        for edata in event_data:
                                            edata['EventID'] = eventID
                                            c.addParkrunEventPosition(edata)
                                        logger.debug("Reloaded event {} for parkrun {}".format(parkrun['EventNumber'], parkrun['EventURL']))
                                        sleep(10)
                                        eventsMissing -= 1 
                                else:
                                    parkrun['RegionID'] = c.execute("SELECT dbo.getDefaultRegionID('{}')".format(parkrun['CountryURL']))
                                    parkrun['Juniors'] = False
                                    if 'juniors' in parkrun['Name']:
                                        parkrun['Juniors'] = True
                                        #parkrun['Name'] = parkrun['Name'].split(' junior')[0]
                                    
                                    p = c.execute("select dbo.getParkrunID('{}')".format(parkrun['EventURL']))
                                    if p is None:    
                                        c.execute("INSERT INTO Parkruns (RegionID, ParkrunName, URL, LaunchDate, ParkrunTypeID) VALUES ({}, '{}', '{}', '19000101', {})".format(parkrun['RegionID'], parkrun['Name'], parkrun['EventURL'], (lambda x: 2 if x else 1)(parkrun['Juniors'])))
                                        logger.info("Added new event {}.".format(row[0][0].get('href')))
                                        l = ParkrunList(config, Mode.NORMAL)
                                        l.events(parkrun['EventURL'], True)
                                        for p in l:
                                            inQ.put(p)
                                        print("Added new event {}.".format(row[0][0].get('href')))
                                    else:
                                        continue
                    if eventsMissing == 0:
                        #c.execute("UPDATE Athletes SET HistoryLastChecked = GETDATE() WHERE AthleteID = " + str(athlete['AthleteID']))
                        logger.info("Athlete {} {}, {} run count OK.".format(athlete['FirstName'], athlete['LastName'], athlete['AthleteID']))
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
                            logger.debug("Deleted event {} for parkrun {} for athlete {} {} ({})".format(d['EventNumber'], d['ParkrunName'],athlete['FirstName'], athlete['LastName'], athlete['AthleteID']))
                            event_data = getEvent(c.execute("SELECT dbo.getEventURL('{}')".format(d['URL'])),d['EventNumber'])
                            eventID = c.replaceParkrunEvent({'EventURL': d['URL'], 'EventNumber': d['EventNumber'], 'EventDate': d['EventDate']})
                            if event_data is not None:
                                for edata in event_data:
                                    edata['EventID'] = eventID
                                    c.addParkrunEventPosition(edata)
                                logger.debug("Reloaded event {} for parkrun {}".format(d['EventNumber'], d['URL']))
                                eventsMissing += 1
                            sleep(10)
                    if eventsMissing == 0:
                        #c.execute("UPDATE Athletes SET HistoryLastChecked = GETDATE() WHERE AthleteID = " + str(athlete['AthleteID']))
                        logger.info("Athlete {} {}, {} run count OK.".format(athlete['FirstName'], athlete['LastName'], athlete['AthleteID']))
            else:
                c.execute("UPDATE Athletes SET HistoryLastChecked = GETDATE() WHERE AthleteID = " + str(athlete['AthleteID']))
                logger.info("Athlete {} {} ({}), {} run count OK.".format(athlete['FirstName'], athlete['LastName'], athlete['EventCount'], athlete['AthleteID']))
            counter += 1
            if counter % 10 == 0:
                rate = (counter / ((timer() - start)/60))
                stats = c.execute("SELECT * FROM getAthleteCheckProgress({})".format(limit))[0]
                print('{:8,.0f} athletes meet criteria, {:8,.0f} athletes checked, {:8,.0f} athletes remain, {:7,.4f}% complete, {:4.1f} athletes/minute, ETC in {} '.format(stats['AthleteCount'],stats['CheckedAthlete'],stats['AthleteCount']-stats['CheckedAthlete'],stats['PercentComplete'],rate, display_time(int((stats['AthleteCount']-stats['CheckedAthlete'])/rate*60))))
                #logger.debug('{:8,.0f} athletes meet criteria, {:8,.0f} athletes checked, {:8,.0f} athletes remain, {:7,.4f}% complete.'.format(stats['AthleteCount'],stats['CheckedAthlete'],stats['AthleteCount']-stats['CheckedAthlete'],stats['PercentComplete']))
                #print('{} events pending download.'.format(inQ.qsize()))
                logger.debug('{} events pending download.'.format(inQ.qsize()))
                counter = 0
                start = timer()
            t = delay - (timer() - tick)
            if t > 0:
                sleep(t)
        inQ.put(None)
        worker.join()
        while not outQ.empty():
            outQ.get(False)
        listener.stop()
    except (KeyboardInterrupt, SystemExit):
        inQ.put(None)
        worker.join()
        while not outQ.empty():
            outQ.get(False)
        listener.stop()
    