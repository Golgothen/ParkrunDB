import lxml.html
from urllib.request import urlopen, Request #, localhost
from urllib.error import HTTPError
from dbconnection import Connection
from mplogger import *
import multiprocessing
from datetime import datetime
import pyodbc

calendarURL = 'https://email.parkrun.com/t/i-l-pthlkg-ntrktsur-m/'

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

def readCancellations(URL):
    table = lxml.html.fromstring(getURL()).xpath('//*[@id="content"]/div[1]')[0]
    logger.debug("Parkrun Cancellations read")
    
    c = Connection(config)
    lists = {}
    ulcount = 0
    licount = 0
    lists['Cancellation'] = []
    for e in table:
        if e.tag == 'h1':
            if len(e.getchildren()) == 0:
                currentdate = datetime.strptime(e.text,'%Y-%m-%d')
        if e.tag == 'ul':
            ulcount += 1
            licount = 0
            if len(e.getchildren()) > 1:
                for li in e.getchildren():
                    licount += 1
                    lists['Cancellation'].append((currentdate,li.getchildren()[0].text[:-8],table.xpath('//*[@id="content"]/div[1]/ul[{}]/li[{}]/text()'.format(ulcount,licount))[0].split(':')[1].strip().replace("'","''")))
            else:
                lists['Cancellation'].append((currentdate,e.getchildren()[0].getchildren()[0].text[:-8],table.xpath('//*[@id="content"]/div[1]/ul[{}]/li/text()'.format(ulcount))[0].split(':')[1].strip().replace("'","''")))
            
    
    logger.debug("Parkrun Cancellations processed")
    count = 0
    for l in lists:
        for i in lists[l]:
            if len(c.execute("SELECT * FROM ParkrunCalendar WHERE ParkrunID = dbo.getParkrunID('{}') AND CalendarID = dbo.getCalendarID('{}') AND CalendarDate = '{}'".format(i[1], l, i[0].strftime('%Y-%m-%d')))) == 0:
                try:
                    c.execute("INSERT INTO ParkrunCalendar (ParkrunID, CalendarID, CalendarDate, Notes) VALUES (dbo.getParkrunID('{}'), dbo.getCalendarID('{}'), '{}', '{}')".format(i[1], l, i[0].strftime('%Y-%m-%d'),i[2]))
                    count += 1
                    logger.info("Added {} for event {} on date {} : {}".format(i[1],l,i[0].strftime('%Y-%m-%d'),i[2]))
                except pyodbc.IntegrityError:
                    logger.warning('Error writing calendar record {} for event {} on date {}. Investigate.'.format(l, i[1], i[0].strftime('%Y-%m-%d')))
                    
    logger.info("Database updated. {} records added.".format(count))
    print("Database updated. {} records added.".format(count))
    listener.stop()

if __name__ == '__main__':

    loggingQueue = multiprocessing.Queue()
    
    listener = LogListener(loggingQueue)
    listener.start()
    
    config = sender_config
    config['handlers']['queue']['queue'] = loggingQueue
    logging.config.dictConfig(config)
    logger = logging.getLogger('parkruncalendar')
    
    
    logger.debug("Reading Australian Parkrun Cancellations")
    readCancellations('https://www.parkrun.com.au/cancellations/')
    logger.debug("Reading Australian Parkrun Cancellations")
    readCancellations('https://www.parkrun.co.nz/cancellations/')
    
