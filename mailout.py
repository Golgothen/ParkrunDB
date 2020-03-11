import logging, logging.config, multiprocessing, lxml.html, gMail

from lxml import etree as e
from mplogger import *
from dbconnection import Connection

fstr = lambda s: '' if s is None else str(s)

loggingQueue = multiprocessing.Queue()
config = sender_config
config['handlers']['queue']['queue'] = loggingQueue

def buildDetailParkrunReport(parkrun):
    c = Connection(config)
    
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
            cls = ''
            td = e.SubElement(tr, 'td')
            if type(v).__name__ == 'int':
                cls = 'number'
            else:
                cls='text'
            if k in ['Gender', 'Age Group']:
                cls = 'centered'
            if len(fstr(v)) > 0:
                td.text = fstr(v)
            if k == 'Barcode':
                cls = 'firstcol text' 
            if row == data[-1]:
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
                    background-color: SpringGreen
                }
                .altrow2 {
                    background-color: LightGreen
                }
                .lastrow {
                    border-bottom : 1px solid black;
                }
                .firstcol {
                    border-left : 1px solid black;
                }
                .text {
                    text-align : left;
                }
                .number {
                    text-align : right;
                }
                .centered {
                    text-align : center;
                }
                tr, td {
                    font-size : 14px;
                    vertical-align : middle;
                    padding : 5px;
                    border-top: 1px solid black;
                    border-right: 1px solid black;
                    border-bottom : 0;
                    border-left : 0;
                }
                th {
                    font-size : 16px';
                    font-weight : normal;
                    text-align : center;
                    padding : 4px;
                }
                h3 {
                    padding : 20px;
                }
            """
    return root #lxml.html.tostring()

def buildSummaryParkrunReport(parkrun):
    c = Connection(config)
    data = c.execute(f"select * from getParkrunReportSummary('{parkrun}') order by [Total Runs] desc")
    
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
            cls = ''
            td = e.SubElement(tr, 'td')
            if type(v).__name__ == 'int':
                cls = 'number'
            else:
                cls='text'
            if k in ['Gender', 'Age Group']:
                cls = 'centered'
            if len(fstr(v)) > 0:
                td.text = fstr(v)
            if k == 'Barcode':
                cls = 'firstcol text' 
            if row == data[-1]:
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
                    align : center;
                    border-spacing : 0;
                    padding : 20px;
                    cellspacing : 4;
                    cellpadding : 0;
                }
                .milestone {
                    background-color: Yellow
                }
                .altrow1 {
                    background-color: SpringGreen
                }
                .altrow2 {
                    background-color: LightGreen
                }
                .lastrow {
                    border-bottom : 1px solid black;
                }
                .firstcol {
                    border-left : 1px solid black;
                }
                .text {
                    text-align : left;
                }
                .number {
                    text-align : right;
                }
                .centered {
                    text-align : center;
                }
                tr, td {
                    font-size : 14px;
                    vertical-align : middle;
                    padding : 5px;
                    border-top: 1px solid black;
                    border-right: 1px solid black;
                    border-bottom : 0;
                    border-left : 0;
                }
                th {
                    font-size : 16px';
                    font-weight : normal;
                    text-align : center;
                    padding : 4px;
                }
                h3 {
                    padding : 20px;
                }
            """
    return root #lxml.html.tostring()

def parkrunMilestoneMailout():
    
    listener = LogListener(loggingQueue)
    listener.start()
    
    logging.config.dictConfig(config)
    logger = logging.getLogger('mailout')
    
    c = Connection(config)

    service = gMail.auth()
    
    maillist = c.execute('SELECT Email, ParkrunName, Detailed from Parkruns WHERE Subscribed = 1')
    for m in maillist:
        if m['Detailed']:
            text = lxml.html.tostring(buildDetailParkrunReport(m['ParkrunName'])).decode('utf-8')
        else:
            text = lxml.html.tostring(buildSummaryParkrunReport(m['ParkrunName'])).decode('utf-8')
        r = gMail.SendMessage(service, 'me', gMail.CreateMessage('me',m['Email'], 'Weekly Upcoming Milestone Report', text))
        
        logger.info(f"{r} sent to {m['Email']} for parkrun {m['ParkrunName']}")
    
    listener.stop()
    
