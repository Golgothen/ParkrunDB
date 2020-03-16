import logging, logging.config, multiprocessing, lxml.html, gMail

import xml.etree.ElementTree as e
from mplogger import *
from dbconnection import Connection

fstr = lambda s: '' if s is None else str(s)

loggingQueue = multiprocessing.Queue()
config = sender_config
config['handlers']['queue']['queue'] = loggingQueue

StyleSheet = """ 
                body {
                    font-family: 'Montserrat', sans-serif;
                    background-color: Azure
                }
                table{
                    border-spacing : 0;
                    padding : 20px;
                    cellspacing : 4;
                    cellpadding : 0;
                    border : 1px solid black;
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
                    border-bottom : 1px solid white;
                }
                .firstcol {
                    border-left : 1px solid white;
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
                .newevent {
                    color : ForestGreen;
                    font-weight : bold;
                }
                .volunteer {
                    color : BlueViolet;
                    font-weight : bold;
                }
                .arrowUp {
                    color : Green;
                    font-weight : bold;
                    font-size : 15px;
                }
                .arrowDown {
                    color : Red;
                    font-weight : bold;
                    font-size : 15px;
                }
                .arrowRight {
                    color : Blue;
                    font-weight : bold;
                    font-size : 15px;
                }
                tr, td {
                    font-size : 14px;
                    vertical-align : middle;
                    padding : 5px;
                    border-top: 1px solid white;
                    border-right: 1px solid white;
                    border-bottom : 0;
                    border-left : 0;
                }
                th {
                    font-size : 16px';
                    font-weight : normal;
                    text-align : center;
                    padding : 4px;
                    padding-left : 12px;
                    padding-right : 12px;
                }
                h3 {
                    padding : 20px;
                }
                
            """


def buildDetailParkrunReport(parkrun, node):
    c = Connection(config)
    
    data = c.execute(f"select * from getParkrunReportDetail('{parkrun}') order by [Total Runs] desc")
    t = e.SubElement(node,'table')
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
    #return #root #lxml.html.tostring()

def buildSummaryParkrunReport(parkrun, node):
    c = Connection(config)
    data = c.execute(f"select * from getParkrunReportSummary('{parkrun}') order by [Total Runs] desc")
    
    t = e.SubElement(node,'table')
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
    #return root #lxml.html.tostring()

def buildWeeklyParkrunReport():

    root = e.Element('html', version = '5.0')
    body = e.SubElement(root, 'body')

    p = e.SubElement(body, 'p')
    p.text = 'Good morning fellow parkrunners!'

    c = Connection(config)
    data = c.execute(f"SELECT * FROM getStatesmanReport(100, 'Victoria') ORDER BY Rank, TQTY DESC")
    
    colgroups = {
        'Rank'                  : ['Rank', 'RankArrow', 'AbsRankChange'],
        'Weeks<br>Held'         : ['Weeks'],
        'Athlete'               : ['AthleteName'],
        'Parkrun'               : ['LastRunParkrun'],
        'Events<br>Local/Total' : ['Events', 'DifferentEvents'],
        'Total<br>Runs'         : ['EventCount'],
        'Tourist<br>Quotient'   : ['TQ', 'TQTY'],
        'This Year<br>Run/New'  : ['TYEventsDone', 'TYNewEvents'],
        'Last Year<br>Run/New'  : ['LYEventsDone', 'LYNewEvents'],
        'P<br>Index'            : ['pIndex'],
        'Wilson<br>Index'       : ['wIndex', 'WIndexArrow', 'wIndexChange']
        }
    
    t = e.SubElement(body,'table')
    thead = e.SubElement(t,'thead')
    tr1 = e.SubElement(thead,'tr')
    tr2 = e.SubElement(thead,'tr')
    
    for i in colgroups:
        h = str(i).split('<br>')
        td = e.SubElement(tr1,'th')
        td.text = h[0]
        td.attrib['colspan'] = str(len(colgroups[i]))
        
        if len(h) > 1:
            if len(colgroups[i]) > len(h[1].split('/')):
                td = e.SubElement(tr2,'th')
                td.attrib['colspan'] = str(len(colgroups[i]))
                td.text = str(h[1])
            else:
                for x in h[1].split('/'):
                    td = e.SubElement(tr2,'th')
                    td.text = str(x)
        else:
            td = e.SubElement(tr2,'th')
            td.attrib['colspan'] = str(len(colgroups[i]))
    
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
        for i in colgroups:
            for j in colgroups[i]:
                td = e.SubElement(tr, 'td')
                cls = ''
                if type(row[j]).__name__ in ['int', 'float']:
                    cls = 'number'
                else:
                    cls='text'
                if j in ['Weeks']:
                    cls = 'centered'
                if j == 'Rank':
                    cls += ' firstcol' 
                if row == data[-1]:
                    cls += ' lastrow'
                if 'Arrow' in j:
                    if j == 'RankArrow':
                        switch = row['RankChange']
                    else:
                        switch = row['wIndexChange']
                    if switch < 0:
                        arr = ' &darr; '
                        scls = ' arrowDown'
                    if switch == 0:
                        arr = ' &rarr; '
                        scls = ' arrowRight'
                    if switch > 0:
                        arr = ' &uarr; '
                        scls = ' arrowUp'
                    s = e.SubElement(td,'span')
                    s.attrib['class'] = scls
                    s.text = arr
                    row[j] = s
                if i == 'Parkrun':
                    if row['LastRunParkrunThisWeek'] > 0:
                        s = e.SubElement(e.SubElement(td,'p'), 'span')
                        s.text = fstr(row['LastRunParkrun'])
                        if row['EventChange'] > 0:
                            s.attrib['class'] = 'newevent'
                    if row['VolunteerThisWeek'] is not None:
                        if row['VolunteerThisWeek'] > 0:
                            s = e.SubElement(td, 'span')
                            s.text = f"({fstr(row['LastVolParkrun'])})"
                            s.attrib['class'] = 'volunteer'
                    row[j] = None
                if len(cls) > 0:
                    td.attrib['class'] = cls
                if type(row[j]).__name__ in ['int', 'float', 'str']:
                    td.text = fstr(row[j])
    
    s = e.SubElement(body,'style')
    s.text = StyleSheet
    x = e.tostring(root).decode('utf-8').replace('&amp;','&')
    with open('output.html','w') as f:
        f.write(x)
        #lxml.html.open_in_browser(root)    


def subRegionStatsReport():
    c = Connection(config)

    service = gMail.auth()
    
    
    
    maillist = c.execute('SELECT Email, AddressTo, SubRegionName from SubRegions WHERE Email IS NOT NULL')
    for m in maillist:
        root = e.Element('html', version = '5.0')
        body = e.SubElement(root, 'body')
        p = e.SubElement(body,'p')
        p.text = f"Hi {m['AddressTo']}. Below is this weeks {m['SubRegionName']} stats"
        p = e.SubElement(e.SubElement(body,'p'),'br')
        
        stats = c.execute(f"select ParkrunName as Parkrun, TotalRunners as Finishers, TotalVolunteers as Volunteers, Total, LastYear as [Last Year], LastYearP as [Last Year %], CalendarType from qrySubRegionStats where SubRegionName = '{m['SubRegionName']}'")
        
        t = e.SubElement(body,'table')
        thead = e.SubElement(t,'thead')
        tr = e.SubElement(thead,'tr')
        for i in list(stats[0].keys()):
            if i in ['CalendarType']:
                continue
            td = e.SubElement(tr,'th')
            td.text = i
            td.attrib['class'] = 'header'
        
        rowcount = 0
        for row in stats:
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
            if row['CalendarType'] == 'Cancellation':
                row['Finishers'] = 'Cancelled'
                row['Volunteers'] = ''
                row['Total'] = ''
                row['Last Year %'] = ''
                row['Last Year'] = ''
            for k, v in row.items():
                if k in ['CalendarType']:
                    continue
                cls = ''
                td = e.SubElement(tr, 'td')
                if type(v).__name__ in ['int', 'float', 'decimal.Decimal']:
                    cls = 'number'
                else:
                    cls='text'
                if k in ['Gender', 'Age Group']:
                    cls = 'centered'
                if k == 'Parkrun':
                    cls = 'firstcol text' 
                if row == stats[-1]:
                    cls += ' lastrow'
                if len(cls) > 0:
                    td.attrib['class'] = cls
                if len(fstr(v)) > 0:
                    td.text = fstr(v)
        
        p = e.SubElement(e.SubElement(body,'p'),'br')
        p = e.SubElement(body,'p')
        p.text = 'This email is automatically generated.  I have tested the process of generating it as much as I could, however there may be issues in the future that arise that I have not yet tested for.'
        p = e.SubElement(body,'p')
        p.text = "If there are any errors in the data, or the email it's self, then please let me know so I can correct them."
        p = e.SubElement(e.SubElement(body,'p'),'br')
        p = e.SubElement(body,'p')
        p.text = 'Cheers, Paul Ellis.'

        s = e.SubElement(body,'style')
        s.text = StyleSheet
        r = gMail.SendMessage(service, 'me', gMail.CreateMessage('me',m['Email'], f"{m['SubRegionName']} stats for this weekend", lxml.html.tostring(root).decode('utf-8')))

def parkrunMilestoneMailout():
    
    listener = LogListener(loggingQueue)
    listener.start()
    
    logging.config.dictConfig(config)
    logger = logging.getLogger('mailout')
    
    c = Connection(config)

    service = gMail.auth()
    
    
    
    maillist = c.execute('SELECT Email, ParkrunName, Detailed from Parkruns WHERE Subscribed = 1')
    for m in maillist:
        root = e.Element('html', version = '5.0')
        body = e.SubElement(root, 'body')
        h = e.SubElement(body,'h3')
        h.text = f"Upcomming milestones for {m['ParkrunName']} parkrun"
        
        if m['Detailed']:
            buildDetailParkrunReport(m['ParkrunName'], body)
        else:
            buildSummaryParkrunReport(m['ParkrunName'], body)
        s = e.SubElement(body,'style')
        s.text = StyleSheet
        r = gMail.SendMessage(service, 'me', gMail.CreateMessage('me',m['Email'], f"Weekly Upcoming Milestone Report for {m['ParkrunName']}", lxml.html.tostring(root).decode('utf-8')))
        
        logger.info(f"{r['id']} sent to {m['Email']} for parkrun {m['ParkrunName']}")
    
    listener.stop()
    
