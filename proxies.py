from bs4 import BeautifulSoup as soup
import requests
import random
import base64


def get_proxies():
    proxy_web_site = 'https://free-proxy-list.net/'
    response = requests.get(proxy_web_site)
    page_html = response.text
    page_soup = soup(page_html, "html.parser")
    containers = page_soup.find_all("div", {"class": "table-responsive"})[0]
    ip_index = [8*k for k in range(80)]
    proxies = set()
    for i in ip_index:
        ip = containers.find_all("td")[i].text
        port = containers.find_all("td")[i+1].text
        https = containers.find_all("td")[i+6].text
        print("ip address : {:<15}   port : {:<5}   https : {:<3} ".format(ip, port, https))
        if https == 'yes':
            proxy = ip + ':' + port
            proxies.add(proxy)
    """
    proxy_web_site = 'http://free-proxy.cz/en/proxylist/country/all/https/ping/all/'
    response = requests.get(proxy_web_site)
    page_html = response.text
    page_soup = soup(page_html, "html.parser")
    containers = page_soup.find_all("table", {"id": "proxy_list"})[0]
    ip_index = [11*k for k in range(37)]
    proxies = set()
    for i in ip_index:
        try:
            ip = base64.decodestring(bytes(containers.find_all("td")[i].text.split('"')[1],'utf8')).decode('utf8')
            port = containers.find_all("td")[i+1].text
            https = containers.find_all("td")[i+2].text
            print("ip address : {:<15}   port : {:<5}   https : {:<3} ".format(ip, port, https))
            if https == 'HTTPS':
                proxy = ip + ':' + port
                proxies.add(proxy)
        except:
            i+=1
    """    
    return proxies

def check_proxies():
    working_proxies = set()
    proxies = get_proxies()            
    test_url = 'https://httpbin.org/ip'    
    for i in proxies:
        print("Testing: {}".format(i))
        try:
            response = requests.get(test_url, proxies={"http": i, "https": i}, timeout = 5)
            #print(response.json())
            print("Valid.")
            working_proxies.add(i)
        except:
            print("Invalid")
    return working_proxies



url = 'write your website url here'
working_proxies = check_proxies()


def scrape_website(url):
    global working_proxies
    for prox in list(working_proxies):
        
        try:
             html_code = requests.get(url, proxies={"http": prox, "https": prox}, timeout = 5).text
             page_soup = soup(html_code, "html.parser")
             print(page_soup)

             ''' add a module here to target specific data points from the website
           and record it '''
          
             time.sleep(randint(3,6))
             
        except:
            
            working_proxies.remove(prox)
            print("{} proxy removed from the list".format(prox))
                        
            # update proxy list if you run out of proxies
            if len(working_proxies) < 3:
                working_proxies = check_proxies()