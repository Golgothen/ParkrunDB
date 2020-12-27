
    
logger = logging.getLogger(__name__)
    
    
def getURL(url, proxy):
    completed = False
    while not completed:
        logger.debug('Hitting {}'.format(url))
        try:
            response = requests.get(url, proxies={"http": proxy, "https": proxy}, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'}, timeout = 10)
            if response.status_code != 200:
                logger.warning('Got HTTP Error {}'.format(e.code))
            if response.status_code in [404, 403]:
                return None
        except:
            return None
        completed = True
        """
        except HTTPError as e:
            logger.warning('Got HTTP Error {}'.format(e.code))
            if e.code == 404:
                #self.msgQ.put(Message('Error',self.id, 'Bad URL ' + url))
                return None
            if e.code == 403:
                #self.msgQ.put(Message('Error',self.id, 'Forbidden ' + url))
                return None
            #self.msgQ.put(Message('Error', self.id, 'Got response {}. retrying in 1 second'.format(e.code)))
            sleep(5)
        except:
            logger.warning('Unexpected network error. URL: ' + url)
            #self.msgQ.put(Message('Error', self.id, 'Bad URL ' + url))
            return None
        """
    temp = response.text#.decode('utf-8', errors='ignore')
    #self.logger.debug('URL returned string of length {}'.format(len(temp)))
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
            #if h == 'Strava':
            #    if len(v.getchildren())>0:
            #        added = False
            #        for c in v.getchildren():
            #            if 'strava' in c.get('href'):
            #                d['StravaID']=c.get('href').split('/')[4]
            #                added = True
            #        if not added:
            #            d['StravaID']=None
            #    else:
            #        d['StravaID']=None
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

def getEvent(self, url, parkrunEvent):
    logger.debug('Hitting {}'.format(url + str(parkrunEvent)))
    root = self.getURL(url + str(parkrunEvent))
    logger.debug(root)
    #Test if we got a valid response'
    if root is None:  #most likely a 404 error
        logger.warning('Error retrieving event')
        self.msgQ.put(Message('Error', self.id, 'Error getting event. Check url ' + url + str(parkrunEvent)))
        return None
    logger.debug('GetURL did not return None')
    if len(root.xpath('//*[@id="content"]/h1')) > 0:
        logger.warning('Error retrieving event')
        self.msgQ.put(Message('Error', self.id, 'Possible URL error getting event. Check url ' + url + str(parkrunEvent)))
        return None
    logger.debug('GetURL did not return an error page')
    return self.getEventTable(root)

def getLatestEvent(self, url):
    logger.debug('Hitting {}'.format(url))
    root = self.getURL(url)
    
    #Test if we got a valid response
    if root is None:  #most likely a 404 error
        logger.warning('Error retrieving event')
        self.msgQ.put(Message('Error', self.id, 'Error getting event. Check url ' + url))
        return 0, None, None
    if len(root.xpath('//*[@id="content"]/h1')) > 0:
        logger.warning('Error retrieving event')
        self.msgQ.put(Message('Error', self.id, 'Possible URL error getting event. Check url ' + url))
        return 0, None, None
    
    try:
        eventHTML = root.xpath('//*[@id="content"]/h2')[0].text
    except IndexError:
        logger.warning('Error retrieving event')
        self.msgQ.put(Message('Error', self.id, 'Possible page error retrieving url ' + url))
        return 0, None, None

    if len(eventHTML.split('#')[1].split('-')[0].strip()) == 0:
        return 0, None, None
    eventNumber =  int(eventHTML.split('#')[1].split('-')[0].strip())
    eventDate = datetime.strptime(eventHTML[len(eventHTML)-10:],'%d/%m/%Y')
    
    return eventNumber, eventDate, self.getEventTable(root)

def getEventHistory(self, url):
    logger.debug('Hitting {}'.format(url))
    root = self.getURL(url)
    
    #Test if we got a valid response
    if root is None:  #most likely a 404 error
        logger.warning('Error retrieving event. URL: ' + url)
        self.msgQ.put(Message('Error', self.id, 'Possible 404 error getting event history. Check url ' + url))
        return None

    if len(root.xpath('//*[@id="content"]/h1')) > 0:
        logger.warning('Error retrieving event')
        self.msgQ.put(Message('Error', self.id, 'URL error in event history. Check ' + url))
        return None

    table = root.xpath('//*[@id="results"]')[0]
    headings = ['EventNumber','EventDate','Runners','Volunteers']    
    rows = table.xpath('//tbody/tr')
    
    data = []
    for row in rows:
        d = {}
        for h, v in zip(headings, row.getchildren()):
            if h == 'EventNumber':
                d[h] = int(v.getchildren()[0].text)
            if h in ['Runners','Volunteers']:
                d[h] = int(v.text)
            if h == 'EventDate':
                d[h] = datetime.strptime(v.getchildren()[0].text,"%d/%m/%Y")
        data.insert(0,d)
    return data

def getVolunteers(self, root):
    volunteerNames = [x.strip() for x in root.xpath('//*[@id="content"]/div[2]/p[1]')[0].text.split(':')[1].split(',')]
    volunteers = []
    parkrun = root.xpath('//*[@id="content"]/h2')[0].text.strip().split(' parkrun')[0]
    results = self.getEventTable(root)
    if self.c.execute("SELECT dbo.getParkrunType('{}')".format(parkrun)) == 'Standard':
        for v in volunteerNames:
            volunteers.append(self.c.execute("SELECT * FROM getAthleteParkrunVolunteerBestMatch('{}','{}','{}')".format(v.split()[0],v.split()[1],parkrun))[0])
        
        # Locate the tail walker
        for v in volunteers:
            try:
                tailwalker = next(r for r in results if r['AthleteID'] == v['AthleteID'])
            except StopIteration:
                continue
