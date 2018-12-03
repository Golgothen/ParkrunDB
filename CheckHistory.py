from mplogger import *

from dbconnection import Connection
from urllib.request import urlopen, Request #, localhost
from urllib.error import HTTPError
from time import sleep

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



if __name__ == '__main__':
    
    #loggingQueue = multiprocessing.Queue()

    #listener = LogListener(loggingQueue)
    #listener.start()
    
    config = sender_config
    #config['handlers']['queue']['queue'] = loggingQueue
    #logging.config.dictConfig(config)
    #logger = logging.getLogger('application')

    c = Connection(config)
    data = c.execute("select * from getAthleteCheckHistoryList(500) order by EventCount DESC")
    
    baseURL = "http://www.parkrun.com.au/results/athleteresultshistory/?athleteNumber="
    
    for athlete in data:
        print("Checking ID {}, {} {} ({})".format(athlete['AthleteID'], athlete['FirstName'], athlete['LastName'], athlete['EventCount']))
        html = getURL(baseURL + str(athlete['AthleteID']))
        # Take everything after the H2 tag
        html = html.split('<h2>')[1]
        #separate the run count
        runcount = int(html.split('</h2>')[0].split(' parkruns')[0].split('(')[-1])
        if athlete['EventCount'] != runcount:
            print("Athlete {} {}, {} run count does not match.".format(athlete['FirstName'], athlete['LastName'], athlete['AthleteID']))
            eventsMissing = runcount - athlete['EventCount']
            print("Missing {} runs".format(eventsMissing))
            
            #tablehtml = html.split('<h1>Event Summaries</h1>')[1].split('</table>')[0] + '</table>'
            rows = lxml.html.fromstring(html.split('<h1>Event Summaries</h1>')[1].split('</table>')[0] + '</table>').xpath('//tbody/tr')
            
            for row in rows:  # Iterate through the events in the summary table
                currentParkrun = row[0][0].get('href').split('/')[3]
                currentParkrunName = row[0][0].text.split(' parkrun')[0]
                sql = "SELECT dbo.getAthleteParkrunCount({},'{}')".format(athlete['AthleteID'],currentParkrun)
                # Check each event in the summary to see if the count matches
                if c.execute(sql) != int(row.getchildren()[1].text):
                    print("Count mismatch for {}.  Checking history".format(currentParkrunName))
                    # Missing run is one of the events listed in the summary
                    # Retrieve the event summary to find out which event number is missing
                    eventHistoryhtml = getURL(row.getchildren()[5].getchildren()[0].get('href'))
                    #hist_rows = lxml.html.fromstring('<table>' + eventHistoryhtml.split('<caption>All Results</caption>')[1].split('</div>')[0]).xpath('//tbody/tr')
                    hist_rows = lxml.html.fromstring('<table>' + eventHistoryhtml.split('<table')[3].split('</div>')[0]).xpath('//tbody/tr')
                    if eventsMissing > 0:
                        for hist_row in hist_rows:
                            eventNumber = int(hist_row.getchildren()[1].getchildren()[0].text)
                            position =  int(hist_row.getchildren()[2].text)
                            found = False
                            hist_data = c.execute("SELECT * FROM getAthleteParkrunEventPositions({},'{}')".format(athlete['AthleteID'],currentParkrun))
                            #if len(hist_data) == 0:
                            #    print("No event data for event {}, URL: {}.  Rectify and rerun.".format(currentParkrun, row.getchildren()[0].getchildren()[0].get('href')))
                            #    continue
                            for d in hist_data:
                                if d['EventNumber'] == eventNumber:
                                    found = True
                                    break
                            #if not any(d['EventNumber'] ==  eventNumber for d in hist_data):
                            if not found:
                                print("Missing event {} for parkrun {}, position {}".format(eventNumber, currentParkrunName, position))
                                if c.execute("SELECT dbo.getAthleteIDParkrunEventPosition('{}',{},{})".format(currentParkrun, eventNumber, position)) == 0:
                                    print("Deleting due to unknown athlete being reassigned to position {}".format(position))
                                else:
                                    print("Deleting due to finish order mismatch")
                                c.execute("DELETE FROM Events WHERE EventID = dbo.getEventURLID('{}', {})".format(currentParkrun, eventNumber))
                    else:
                        # Event has been deleted from athlete history.  Find out which one
                        hist_data = c.execute("SELECT * FROM getAthleteParkrunEventPositions({},'{}')".format(athlete['AthleteID'],currentParkrun))
                        for d in hist_data:
                            eventNumber = d['EventNumber']
                            for hist_row in hist_rows:
                                found = False
                                if eventNumber == int(hist_row.getchildren()[1].getchildren()[0].text):
                                #position =  int(hist_row.getchildren()[2].text)
                                #hist_data = c.execute("SELECT * FROM getAthleteParkrunEventPositions({},'{}')".format(athlete['AthleteID'],currentParkrun))
                                #if len(hist_data) == 0:
                                #    print("No event data for event {}, URL: {}.  Rectify and rerun.".format(currentParkrun, row.getchildren()[0].getchildren()[0].get('href')))
                                #    continue
                                    found = True
                                    break
                            #if not any(d['EventNumber'] ==  eventNumber for d in hist_data):
                            if not found:
                                print("Deleted event {} for parkrun {}".format(eventNumber, currentParkrunName))
                                c.execute("DELETE FROM Events WHERE EventID = dbo.getEventURLID('{}', {})".format(currentParkrun, eventNumber))
                    sleep(5)    
        else:
            c.execute("UPDATE Athletes SET HistoryLastChecked = GETDATE() WHERE AthleteID = " + str(athlete['AthleteID']))
            #print("Athlete {} {}, {} run count OK.".format(athlete['FirstName'], athlete['LastName'], athlete['AthleteID']))
        sleep(5)
        