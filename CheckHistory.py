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
    
    loggingQueue = multiprocessing.Queue()

    listener = LogListener(loggingQueue)
    #listener.start()
    
    config = sender_config
    config['handlers']['queue']['queue'] = loggingQueue
    logging.config.dictConfig(config)
    logger = logging.getLogger('application')

    c = Connection(config)
    data = c.execute("select * from getAthleteCheckHistoryList(500) order by EventCount DESC")
    
    baseURL = "http://www.parkrun.com.au/results/athleteresultshistory/?athleteNumber="
    
    for athlete in data:
        print("Checking ID {}".format(athlete['AthleteID']))
        html = getURL(baseURL + str(athlete['AthleteID']))
        # Take everything after the H2 tag
        html = html.split('<h2>')[1]
        #separate the run count
        runcount = int(html.split('</h2>')[0].split(' parkruns')[0].split('(')[-1])
        if athlete['EventCount'] != runcount:
            print("Athlete {} {}, {} run count does not match.".format(athlete['FirstName'], athlete['LastName'], athlete['AthleteID']))
            print("Missing {} runs".format(runcount - athlete['EventCount']))
            eventsMissing = runcount - athlete['EventCount']
            
            tablehtml = html.split('<h1>Event Summaries</h1>')[1].split('</table>')[0] + '</table>'
            rows = lxml.html.fromstring(tbl).xpath('//tbody/tr')
            
            eventsFound = 0
            for row in rows:
                sql = "SELECT dbo.getAthleteParkrunCount({},{})".format(athlete['AthleteID'],row[0][0].text.split(' parkrun')[0])
                if eventsFound = EventsMissing:
                    break
        else:
            c.execute("UPDATE Athletes SET HistoryLastChecked = GETDATE() WHERE AthleteID = " + str(athlete['AthleteID']))
            print("Athlete {} {}, {} run count OK.".format(athlete['FirstName'], athlete['LastName'], athlete['AthleteID']))
        sleep(5)
        