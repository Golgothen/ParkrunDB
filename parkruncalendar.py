import lxml.html
from urllib.request import urlopen, Request #, localhost
from urllib.error import HTTPError
from dbconnection import Connection
from mplogger import *
import multiprocessing
from datetime import datetime
import pyodbc


def getURL(url):
    completed = False
    while not completed:
        try:
            logger.debug('Hitting {}'.format(url))
            f = urlopen(Request(url, data=None, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'}))
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

if __name__ == '__main__':

    loggingQueue = multiprocessing.Queue()
    
    listener = LogListener(loggingQueue)
    listener.start()
    
    config = sender_config
    config['handlers']['queue']['queue'] = loggingQueue
    logging.config.dictConfig(config)
    logger = logging.getLogger('parkruncalendar')
    
    c = Connection(config)
    logger.debug("Reading Parkrun Calendar")
    table = lxml.html.fromstring(getURL('https://email.parkrun.com/t/i-l-pmttz-ntrktsur-m/')).xpath('/html/body/section[1]/div/div[1]/div[1]/div/div[4]')[0]
    logger.debug("Parkrun Calendar read")
    lists = {}
    
    for e in table:
        if e.tag == 'h3':
            currentlist = e.getchildren()[0].text
            if currentlist == 'Cancellations': currentlist = 'Cancellation'
            if currentlist == 'Anniversaries': currentlist = 'Anniversary'
            if currentlist == 'Launches': currentlist = 'Launch'
            lists[currentlist] = []
            if currentlist == 'Cancellation':
                logger.debug("Launches and Anniversaries processed")
                break
        if e.tag == 'p':
            if len(e.getchildren()) > 0:
                if e.getchildren()[0].tag == 'span':
                    currentmonth = e.getchildren()[0].getchildren()[0].text
                if e.getchildren()[0].tag == 'strong':
                    if len(e.getchildren()[0].getchildren()) > 0:
                        currentmonth = e.getchildren()[0].getchildren()[0].text
                    else:
                        currentdate = e.getchildren()[0].text[:-2]
                if e.getchildren()[0].tag == 'a':
                    event = e.getchildren()[0].text.split(' (')[0]
                    lists[currentlist].append(((datetime.strptime('{} {} {}'.format(currentmonth, currentdate, datetime.now().year),'%B %d %Y')),event))
            else:
                #print( list(map(str.strip, e.text.replace('\xa0','').split(','))))
                for x in [x.split('(')[0].strip() for x in list(map(str.strip, e.text.replace('\xa0','').split(',')))]:
                    lists[currentlist].append(( (datetime.strptime('{} {} {}'.format(currentmonth, currentdate, datetime.now().year),'%B %d %Y')), (x)))
    
    logger.debug("Reading Parkrun Cancellations")
    table = lxml.html.fromstring(getURL('https://www.parkrun.com.au/cancellations/')).xpath('//*[@id="content"]/div[1]')[0]
    logger.debug("Parkrun Cancellations read")
    for e in table:
        if e.tag == 'h1':
            if len(e.getchildren()) == 0:
                currentdate = datetime.strptime(e.text,'%Y-%m-%d')
        if e.tag == 'ul':
            for li in e.getchildren():
                lists['Cancellation'].append((currentdate,li.getchildren()[0].text[:-8]))#,li.text.split(':').strip())
    
    logger.debug("Parkrun Cancellations processed")
    count = 0
    for l in lists:
        for i in lists[l]:
            if len(c.execute("SELECT * FROM ParkrunCalendar WHERE ParkrunID = dbo.getParkrunID('{}') AND CalendarID = dbo.getCalendarID('{}') AND CalendarDate = '{}'".format(i[1], l, i[0].strftime('%Y-%m-%d')))) == 0:
                count += 1
                c.execute("INSERT INTO ParkrunCalendar (ParkrunID, CalendarID, CalendarDate) VALUES (dbo.getParkrunID('{}'), dbo.getCalendarID('{}'), '{}')".format(i[1], l, i[0].strftime('%Y-%m-%d')))
    logger.info("Database updated. {} records added.".format(count))
    listener.stop()
