import multiprocessing, multiprocessing.queues, threading, requests
from bs4 import BeautifulSoup as soup
import lxml.html
from urllib.request import urlopen, Request #, localhost
from urllib.error import HTTPError

class Crawler():
    
    """
    Crawler is a super class that ProxyManager will interact with.
    Subclasses of this class will have the specific code needed for interpreting each web page table
    """
    
    def __init__(self, name, proxyQue):
        self.name = name
        self.proxyQue = proxyQue
    
    def getProxies(self):
        # Subclasses will provide this code and put their results into the self.proxyQue
        print('Crawler {} scanning for proxies'.format(self.name))
    
    def testProxy(self, proxy):
        print("Testing: {}".format(proxy))
        try:
            response = requests.get('https://httpbin.org/ip', proxies={"http": proxy, "https": proxy}, timeout = 5)
            print('Valid')
            return True
        except:
            print('Invalid')
            return False
    
    def getURL(self, url):
        completed = False
        while not completed:
            try:
                #print('Hitting {}'.format(url))
                f = urlopen(Request(url, data=None, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'}))
                completed = True
            except HTTPError as e:
                print('Got HTTP Error {}'.format(e.code))
                if e.code == 404:
                    print('Error Bad URL ' + url)
                    return None
                if e.code == 403:
                    print('Error Forbidden ' + url)
                    return None
                print('Error Got response {}. retrying in 1 second'.format(e.code))
            except:
                print('Unexpected network error. URL: ' + url)
                print('Error Bad URL ' + url)
                return None
        temp = f.read().decode('utf-8', errors='ignore')
        #self.logger.debug('URL returned string of length {}'.format(len(temp)))
        return lxml.html.fromstring(temp) 

        


class FreeProxyList(Crawler):
    
    """
    FreeProxyList scrapes proxy information from https://free-proxy-list.net
    """
    
    def __init__(self, proxyQue):
        super().__init__('Free_Proxy_List', proxyQue)
    
    def getProxies(self):
        super().getProxies()
        response = requests.get('https://free-proxy-list.net/')
        page_soup = soup(response.text, "html.parser")
        containers = page_soup.find_all("div", {"class": "table-responsive"})[0]
        ip_index = [8*k for k in range(80)]
        for i in ip_index:
            ip = containers.find_all("td")[i].text
            port = containers.find_all("td")[i+1].text
            https = containers.find_all("td")[i+6].text
            #print("ip address : {:<15}   port : {:<5}   https : {:<3} ".format(ip, port, https))
            if https == 'yes':
                if self.testProxy(ip + ':' + port):
                    self.proxyQue.put(ip + ':' + port)
        print('Crawler {} complete'.format(self.name))
        


class SpyOne(Crawler):
    
    """
    SpyOne scrapes proxies from http://spys.one/en/https-ssl-proxy/
    """
    def __init__(self, proxyQue):
        super().__init__('SpyOne', proxyQue)
    
    def getProxies(self):
        super().getProxies()
        lx = self.getURL('http://spys.one/en/https-ssl-proxy/')
        # Isolate the table we want
        t = lx[1][2][4][0][0]
        
        for i in range(2,len(t)-1):
            ip = t[i][0][0].text
            port = '8080'
            if self.testProxy(ip + ':' + port):
                self.proxyQue.put(ip + ':' + port)
        
        print('Crawler {} complete'.format(self.name))
    
class ProxyScrape(Crawler):
    
    """
    ProxyScrape scrapes proxies from https://api.proxyscrape.com
    """
    
    def __init__(self, proxyQue):
        super().__init__('ProxyScrape', proxyQue)
    
    def getProxies(self):
        super().getProxies()
        lx = self.getURL('https://api.proxyscrape.com/?request=getproxies&proxytype=http&timeout=10000&country=all&ssl=all&anonymity=all')
        l = lx.text.split('\r\n')
        for i in l:
            if self.testProxy(i):
                self.proxyQue.put(i)
        print('Crawler {} complete'.format(self.name))
    

class NonDupeQueue(multiprocessing.queues.Queue):
    
    def __init__(self, *args, **kwargs): 
        ctx = multiprocessing.get_context()
        super(NonDupeQueue, self).__init__(*args, **kwargs, ctx = ctx)
        self.__items = []
    
    def put(self, item): #, *args, **kwargs):
        if item not in self.__items:
            self.__items.append(item)
            super().put(item) #, *args, **kwargs)
    
    def get(self, *args, **kwargs):
        x = super().get(*args, **kwargs)
        self.__items = [i for i in self.__items if i != x]
        return x

           
class ProxyManager(multiprocessing.Process):
    
    """
    ProxyManager will be responsible for maintaining a list of available and valid proxies.
    Once a proxy is requested it will be removes from the que of available proxies.
    If the que drops below a pre-defined threshold, all subclasses of crawlers will be invoked to collect more proxies.
    """
    
    def __init__(self, exitEvent):
        super(ProxyManager, self).__init__()
        self.__crawlers = []
        self.__crawler_threads = []
        self.__proxies = None
        self.__min_proxy_count = 3
        self.__exitEvent = exitEvent
        self.__proxyList = []
    
    def addCrawler(self, newCrawler):
        # Add the new crawler to the list of crawlers
        self.__crawlers.append(newCrawler)
    
    def getProxyCount(self):
        return self.__proxies.qsize()
    
    """
    def put(self, newProxy):
        if newProxy not in self.__proxyList:
            self.__proxyList.append(newProxy)
            self.__proxies.put(newProxy)
        else:
            print('Dupe {} skipped.'.format(newProxy))
    """
    
    def getProxy(self):
        # Check if there are sufficient proxies in the 
        if self.__proxies.qsize() <= self.__min_proxy_count:
            # Iterate through the available crawlers
            for i in self.__crawlers:
                # see if there is a thread with the same name
                running = False
                for j in self.__crawler_threads:
                    if j.name == i.name:
                        # a thread by the same name was found, so don't start it again
                        running = True
                if not running:
                    # start any crawlers that didn't have matching threads (by name)
                    x = threading.Thread(target = i.getProxies, daemon = True, name = i.name)
                    self.__crawler_threads.append(x)
                    x.start()
        
        # If there are no proxies left, execution should block here until one of the crawlers returns a new value into the queue
        x = self.__proxies.get()
        #self.__proxyList = [i for i in self.__proxyList if i != x]
        return x
    
    def run(self):
        self.__proxies = NonDupeQueue()
        self.addCrawler(FreeProxyList(self.__proxies))
        self.addCrawler(ProxyScrape(self.__proxies))
        for i in self.__crawlers:
            x = threading.Thread(target = i.getProxies, daemon = True, name = i.name)
            self.__crawler_threads.append(x)
            x.start()
        self.__exitEvent.wait()
        print('ProxyManager exiting')
        