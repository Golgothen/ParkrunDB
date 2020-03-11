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

#fstr = lambda s: '' if s is None or s == 'NULL' else str(s)
parkrun = 'Wyndham Vale'
data = c.execute(f"select * from getParkrunReportDetail('{parkrun}') order by [Total Runs] desc")

root = e.Element('html', version = '5.0')

body = e.SubElement(root, 'body')
h = e.SubElement(body,'h3')
h.text = f'Upcomming milestones for {parkrun} parkrun'
t = e.SubElement(body,'table')
t.attrib['class'] = 'sortable'
thead = e.SubElement(t,'thead')
tr = e.SubElement(thead,'tr')
for i in list(data[0].keys()):
    td = e.SubElement(tr,'th')
    td.text = i
    td.attrib['class'] = 'header'

rowcount = 0
for row in data:
    cls = ''
    if 47 <= row['Total Runs'] <= 49 or \
       97 <= row['Total Runs'] <= 99 or \
       247 <= row['Total Runs'] <= 249 or \
       497 <= row['Total Runs'] <= 499 or \
       97 <= row['Runs at Home'] <= 99:
        cls = 'milestone'
    else:
        if rowcount == 0:
            cls = 'altrow1'
        else:
            cls = 'altrow2'
    tr = e.SubElement(t, 'tr', CLASS=cls)
    rowcount += 1
    if rowcount> 1:
        rowcount = 0
    else:
        cls = ''
    for k, v in row.items():
        td = e.SubElement(tr, 'td')
        td.text = str(v)
        if k != 'Barcode':
            cls = ' notfirstcol' 
        if row == data[-1] and 'lastrow' not in cls:
            cls += ' lastrow'

        if len(cls) > 0:
            td.attrib['class'] = cls

s = e.SubElement(body,'style')
s.text = """ 
            body {
                font-family: 'Montserrat', sans-serif;
                background-color: Azure
            }
            table{
                border-spacing : 0;
                padding : 20px;
                cellspacing : 4;
                cellpadding : 0;
            }
            .milestone {
                background-color: Yellow
            }
            .altrow1 {
                background-color: PaleGreen
            }
            .altrow2 {
                background-color: DarkSeaGreen
            }
            .lastrow {
                border-bottom : 1px solid;
            }
            .notfirstcol {
                border-left : 0;
            }
            tr, td {
                font-size : 14px;
                vertical-align : middle;
                padding : 5px;
                border: 1px solid black;
                border-bottom : 0;
            }
            th {
                font-size : 16px';
                font-weight : normal;
            }
        """

lxml.html.open_in_browser(root)
