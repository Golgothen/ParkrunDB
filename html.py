import logging, logging.config, multiprocessing, lxml.html

from lxml import etree as e
from mplogger import *
from dbconnection import Connection

loggingQueue = multiprocessing.Queue()

#listener = LogListener(loggingQueue)
#listener.start()

config = sender_config
config['handlers']['queue']['queue'] = loggingQueue
#logging.config.dictConfig(config)
#logger = logging.getLogger('checkhistory')

c = Connection(config)

data = c.execute("select * from getParkrunReportDetail('Wyndham Vale') order by [Total Runs] desc")

root = e.Element('html', version = '5.0')

body = e.SubElement(root, 'body')

t = e.SubElement(body,'table')
th = e.SubElement(t,'tr')
for i in list(data[0].keys()):
    td = e.SubElement(th,'td')
    td.text = i

for row in data:
    tr = e.SubElement(t, 'tr')
    for k, v in row.items():
        td = e.SubElement(tr, 'td')
        td.text = str(v)

s = e.SubElement(body,'style')
s.text = """ 
            body {
                font-family: sans-serif;
            }
            table, td, td{
                border: 1px solid black;
            }
        """

lxml.html.open_in_browser(root)
