import logging, logging.config, multiprocessing, lxml.html, gMail, math

#import xml.etree.ElementTree as e
import lxml.etree as e
from mplogger import *
from dbconnection import Connection
from datetime import date
from time import sleep
from os import path

fstr = lambda s: '' if s is None else str(s)

direction = lambda s: 'down' if s < 0 else ('steady at' if s == 0 else 'up')
concat = lambda r, d: ' and ' if r == d[-2] else ('' if r == d[-1] else ', ')
ordinal = lambda n: "%d%s" % (n,"tsnrhtdd"[(math.floor(n/10)%10!=1)*(n%10<4)*n%10::4])
formattime = lambda s: s.strftime('%M:%S') if s.hour == 0 else s.strftime('%H:%M:%S')
guntimedelta = lambda o, n: datetime.combine(date.min, o) - datetime.combine(date.min, n)
genderPosessive = lambda s: 'his' if s == 'M' else 'her'
genderObjective = lambda s: 'him' if s == 'M' else 'her'
daySalutation = lambda: 'morning' if datetime.now().hour < 13 else 'afternoon'

 
loggingQueue = multiprocessing.Queue()
config = sender_config
config['handlers']['queue']['queue'] = loggingQueue


StyleSheet = """ 
                body {
                    font-family: 'Montserrat', sans-serif;
                    background-color: White
                }
                table{
                    border-collapse:collapse;
                    border-spacing : 0;
                    padding : 20px;
                    cellspacing : 4;
                    cellpadding : 0;
                }
                .milestone {
                    background-color: Yellow
                }
                .altrow1 {
                    background-color: #EEEEEE
                }
                .altrow2 {
                    background-color: #DDDDDD
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
                    font-style : italic;
                }
                .volunteer {
                    color : BlueViolet;
                    font-weight : bold;
                    font-style : italic;
                }
                .arrowUp {
                    color : Green;
                    font-weight : bold;
                    font-size : 15px;
                    text-align: center;
                }
                .arrowDown {
                    color : Red;
                    font-weight : bold;
                    font-size : 15px;
                    text-align: center;
                }
                .arrowRight {
                    color : Blue;
                    font-weight : bold;
                    font-size : 15px;
                }
                a.parkrunname {
                    font-weight : bold;
                    color : black;
                }
                a.athlete {
                    color : black;
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
                    padding-left : 80px;
                    font-size : 20px;
                    font-style : italic;
                    color : Blue;
                }
                .section {
                    margin-top : 50px;
                    margin-bottom : 10px;
                }
                .parkrunname {
                    font-weight: bold;
                }
                .approachingmilestone {
                    font-weight: bold;
                    color: Orange;
                }
                .milestone50 {
                    font-weight: bold;
                    color: Red;
                }
                .milestone100 {
                    font-weight: bold;
                    color: Black;
                }
                .milestone250 {
                    font-weight: bold;
                    color: Green;
                }
                .milestone500 {
                    font-weight: bold;
                    color: Blue;
                }
                tr, td {
                    font-size : 14px;
                    vertical-align : middle;
                    padding : 5px;
                }
                colgroup {
                    border-left: 1px solid black;
                    border-right: 1px solid black;
                }
                .lastrow {
                    border-bottom : 1px solid black;
                }
                .firstrow {
                    border-top : 1px solid black;
                }
                .name {
                    font-size: 3em;
                    font-style: italic;
                    color: red;
                    background-color: #EEE;
                    border-top: thin solid;
                    border-top-color: red;
                    padding: 0px 30px;
                    margin-top: 15px;
                }
                .business {
                    font-size: 1.75em;
                    font-style: italic;
                    color: black;
                    padding: 0px 60px;
                }
                .title {
                    float: left;
                    font-size: 1.5em;
                    color: blue;
                    margin-left: 60px;
                }
                .details {
                    float: right;
                }
                .details p{
                    color: #888;
                    margin: 0px 15px;
                }
                .footer {
                    border-bottom: thin solid;
                    border-bottom-color: red;
                    height: 15px;
                    background-color: #EEE;
                    margin-top: 15px;
                    margin-bottom: 15px;
                }
                .container {
                    width: 450px;
                }
                .container:before,
                .container:after {
                    content: " ";
                    display: table;
                }
                .container:after {
                    clear: both;
                }
            """

def buildDetailParkrunReport(parkrun, node):
    c = Connection(config)
    
    data = c.execute(f"select * from getParkrunReportDetail('{parkrun}') order by EventCount desc")

    colgroups = {
        'Barcode<br>Name<br>Club<br>Last Run' : ['AthleteID', 'Name', 'ClubName', 'LastEventDate'],
        'Total<br>Home'                       : ['EventCount', 'RunCount'],
        'Gender<br>Age Cat<br>Age Grp'        : ['Gender', 'AgeCategory', 'AgeGroup']
        }
    
    t = e.SubElement(node,'table')
    for i in colgroups:
        col = e.SubElement(t, 'colgroup', {'span': f"{len(colgroups[i])}"})
    tr1 = e.SubElement(t,'tr', {'class' : 'firstrow'})
    
    for i in colgroups:
        h = str(i).split('<br>')
        td = e.SubElement(tr1,'th', {'scope' : 'colgroup'})
        td.text = h[0]
        for i in range(1,len(h)):
            td = e.SubElement(tr1,'th', {'scope': 'col'})
            td.text = str(h[i])
    
    
    rowcount = 0
    for row in data:
        cls = ''
        if 47 <= row['EventCount'] <= 49 or \
           97 <= row['EventCount'] <= 99 or \
           247 <= row['EventCount'] <= 249 or \
           497 <= row['EventCount'] <= 499 or \
           (7 <= row['EventCount'] <= 9 and row['AgeGroup'].lower() == 'junior') or \
           97 <= row['RunCount'] <= 99:
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
        for k in colgroups:
            for i in colgroups[k]:
                cls = ''
                td = e.SubElement(tr, 'td')
                if type(row[i]).__name__ == 'int':
                    cls = 'number'
                else:
                    cls='text'
                if i in ['Gender', 'AgeGroup']:
                    cls = 'centered'
                if len(fstr(row[i])) > 0:
                    td.text = fstr(row[i])
                if k == 'AthleteID':
                    cls = 'text' 
                if row == data[-1]:
                    cls += ' lastrow'
                if len(cls) > 0:
                    td.attrib['class'] = cls
    #return #root #lxml.html.tostring()

def buildSummaryParkrunReport(parkrun, node):
    c = Connection(config)
    data = c.execute(f"select * from getParkrunReportSummary('{parkrun}') order by EventCount desc")
    
    colgroups = {
        'Barcode<br>Name<br>Club<br>Last Run' : ['AthleteID', 'Name', 'ClubName', 'LastEventDate'],
        'Total<br>Home'                       : ['EventCount', 'RunCount'],
        'Gender<br>Age Cat<br>Age Grp'        : ['Gender', 'AgeCategory', 'AgeGroup']
        }
    
    
    rowcount = 0
    if len(data) == 0:
        s = e.SubElement(node,'Span')
        s.text = 'There are no upcoming milestones this week.'
    else:
        t = e.SubElement(node,'table')
        for i in colgroups:
            col = e.SubElement(t, 'colgroup', {'span': f"{len(colgroups[i])}"})
        tr1 = e.SubElement(t,'tr', {'class' : 'firstrow'})
        
        for i in colgroups:
            h = str(i).split('<br>')
            td = e.SubElement(tr1,'th', {'scope' : 'colgroup'})
            td.text = h[0]
            for i in range(1,len(h)):
                td = e.SubElement(tr1,'th', {'scope': 'col'})
                td.text = str(h[i])
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
            for k in colgroups:
                for i in colgroups[k]:
                    cls = ''
                    td = e.SubElement(tr, 'td')
                    if type(row[i]).__name__ == 'int':
                        cls = 'number'
                    else:
                        cls='text'
                    if i in ['Gender', 'AgeGroup']:
                        cls = 'centered'
                    if len(fstr(row[i])) > 0:
                        td.text = fstr(row[i])
                    if i == 'AthleteID':
                        cls = 'text' 
                    if row == data[-1]:
                        cls += ' lastrow'
                    if len(cls) > 0:
                        td.attrib['class'] = cls
    #return root #lxml.html.tostring()

def buildVolunteerMilestoneReport(parkrun, node):
    c = Connection(config)
    data = c.execute(f"select * from qryParkrunVolunteerMilestone where Parkrun = '{parkrun}' order by TotalVolunteerCount desc")
    
    colgroups = {
        'Barcode<br>Name<br>Club'             : ['AthleteID', 'Name','ClubName'],
        'Total<br>Home'                       : ['TotalVolunteerCount', 'EventVolunteerCount'],
        'Gender<br>Age Cat<br>Age Grp'        : ['Gender', 'AgeCategory', 'AgeGroup']
        }
    
    
    rowcount = 0
    if len(data) == 0:
        s = e.SubElement(node,'Span')
        s.text = 'There are no upcoming volunteer milestones this week.'
    else:
        t = e.SubElement(node,'table')
        for i in colgroups:
            col = e.SubElement(t, 'colgroup', {'span': f"{len(colgroups[i])}"})
        tr1 = e.SubElement(t,'tr', {'class' : 'firstrow'})
        
        for i in colgroups:
            h = str(i).split('<br>')
            td = e.SubElement(tr1,'th', {'scope' : 'colgroup'})
            td.text = h[0]
            for i in range(1,len(h)):
                td = e.SubElement(tr1,'th', {'scope': 'col'})
                td.text = str(h[i])
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
            for k in colgroups:
                for i in colgroups[k]:
                    cls = ''
                    td = e.SubElement(tr, 'td')
                    if type(row[i]).__name__ == 'int':
                        cls = 'number'
                    else:
                        cls='text'
                    if i in ['Gender', 'AgeGroup']:
                        cls = 'centered'
                    if len(fstr(row[i])) > 0:
                        td.text = fstr(row[i])
                    if i == 'AthleteID':
                        cls = 'text' 
                    if row == data[-1]:
                        cls += ' lastrow'
                    if len(cls) > 0:
                        td.attrib['class'] = cls
    #return root #lxml.html.tostring()

def buildWeeklyParkrunReport(region):
    #region = 'Queensland'
    root = e.Element('html', version = '5.0')
    head = e.SubElement(root, 'head')
    
    body = e.SubElement(root, 'body')
    p = e.SubElement(body, 'h4')
    p.text = f'Good {daySalutation()} fellow parkrunners!'
    p = e.SubElement(body, 'p')
    p.text = f"Welcome to the weekly parkrun report for {date.today().strftime('%A')}, the {ordinal(date.today().day)} of {date.today().strftime('%B, %Y')}." 
    p = e.SubElement(body, 'p')
    c = Connection(config)
    
    if path.exists("comments.txt"):
        sec = e.SubElement(body, 'div', {'class' : 'section'})
        p = e.SubElement(sec, 'h3')
        p.text = f'From the Tech Department'
        p = e.SubElement(sec, 'p')
        with open('comments.txt','r') as f:
            lines = f.readlines()
        for l in lines:
            s = e.SubElement(p,'span')
            s.text = l
            b = e.SubElement(p,'br')

    sec = e.SubElement(body, 'div', {'class' : 'section'})
    p = e.SubElement(sec, 'h3')
    p.text = f'Weekend in brief'
    data = c.execute(f"select ParkrunName from getParkrunCancellations('{region}') order by ParkrunName")
    p = e.SubElement(sec, 'p')
    s = e.SubElement(p, 'span')
    s.text = "As of writing "
    s = e.SubElement(p, 'span')
    if len(data) == 0:
        s.text = "there were no "
        a = e.SubElement(s, 'a', {'href' : 'https://www.parkrun.com.au/cancellations/', 'target' : '_blank', 'rel' : 'noopener noreferrer', 'class' : 'parkrunname'})
        a.text = 'cancellations'
    elif len(data) == 1:
        s.text = f"{data[0]['ParkrunName']} was the only "
        a = e.SubElement(p, 'a', {'href' : 'https://www.parkrun.com.au/cancellations/', 'target' : '_blank', 'rel' : 'noopener noreferrer', 'class' : 'parkrunname'})
        a.text = 'cancellations'
    else:
        s = e.SubElement(p, 'span')
        s.text = ''
        for row in data:
            s.text += f"{row['ParkrunName']}{concat(row,data)}"
        s.text +=  " were the only "
        a = e.SubElement(p, 'a', {'href' : 'https://www.parkrun.com.au/cancellations/', 'target' : '_blank', 'rel' : 'noopener noreferrer', 'class' : 'parkrunname'})
        a.text = 'cancellations'

    data = c.execute(f"select ParkrunName, URL from getParkrunNoResults('{region}') order by ParkrunName")
    s = e.SubElement(p, 'span')
    s.text = ", with "
    if len(data) == 0:
        s.text += "all other events posting results"
    elif len(data) == 1:
        a = e.SubElement(s, 'a', {'href' : f"https://www.parkrun.com.au/{data[0]['URL']}/results/latestresults/", 'target' : '_blank', 'rel' : 'noopener noreferrer', 'class' : 'parkrunname'})
        a.text = data[0]['ParkrunName']
        s = e.SubElement(p, 'span')
        s.text = " the only event yet to post results"
    else:
        for row in data:
            a = e.SubElement(s, 'a', {'href' : f"https://www.parkrun.com.au/{row['URL']}/results/latestresults/", 'target' : '_blank', 'rel' : 'noopener noreferrer', 'class' : 'parkrunname'})
            a.text = row['ParkrunName']
            s = e.SubElement(p, 'span')
            s.text = f"{concat(row,data)}"
        s.text +=  " yet to post results"

    data = c.execute(f"select ParkrunName, URL from getParkrunNoVolunteers('{region}') order by ParkrunName")
    s = e.SubElement(p, 'span')
    s.text = ", and "
    if len(data) == 0:
        s.text += "all volunteer information recorded."
    elif len(data) == 1:
        a = e.SubElement(s, 'a', {'href' : f"https://www.parkrun.com.au/{data[0]['URL']}/results/latestresults/", 'target' : '_blank', 'rel' : 'noopener noreferrer', 'class' : 'parkrunname'})
        a.text = data[0]['ParkrunName']
        s = e.SubElement(p, 'span')
        s.text = " the only event yet to post volunteer information."
    else:
        for row in data:
            a = e.SubElement(s, 'a', {'href' : f"https://www.parkrun.com.au/{row['URL']}/results/latestresults/", 'target' : '_blank', 'rel' : 'noopener noreferrer', 'class' : 'parkrunname'})
            a.text = row['ParkrunName']
            s = e.SubElement(p, 'span')
            s.text = f"{concat(row,data)}"
        s.text +=  " yet to post volunteer information."
    
    
    FirstTimers = c.execute(f"select dbo.getWeeklyFirstTimers('{region}')")
    Tourists = c.execute(f"select dbo.getWeeklyTourists('{region}')")
    Volunteers = c.execute(f"select dbo.getWeeklyVolunteers('{region}')")
    TotalPBs = c.execute(f"select dbo.getWeeklyPBs('{region}')")
    TotalRunners = c.execute(f"select dbo.getWeeklyTotalRunners('{region}')")
    TotalRunnersLastWeek = c.execute(f"select dbo.getWeeklyTotalRunnersLastWeek('{region}')")
    if FirstTimers is None: FirstTimers = 0
    if Tourists is None: Tourists = 0
    if Volunteers is None: Volunteers = 0
    if TotalPBs is None: TotalPBs = 0
    if TotalRunners is None: TotalRunners = 0
    if TotalRunnersLastWeek is None: TotalRunnersLastWeek = 0
    
    
    p = e.SubElement(sec, 'p')
    p.text = f"We had {TotalRunners:,.0f} runners ({direction(TotalRunners - TotalRunnersLastWeek)} by {abs(TotalRunners - TotalRunnersLastWeek):,.0f}"
    if TotalRunners > 0:
        p.text += f" or {abs(TotalRunnersLastWeek - TotalRunners) / TotalRunners:.2%})"
    p.text += f", with {TotalPBs:,.0f} PB's"
    if TotalRunners > 0:
        p.text += f" ({TotalPBs / TotalRunners:.2%})"
    p.text += f", {Tourists:,.0f} tourists visiting new events for the first time, and {FirstTimers:,.0f} first timers, supported by {Volunteers} volunteers."
    
    data = c.execute(f"select top(5) ParkrunName, URL, ThisWeek, RunnersChange from qryWeeklyParkrunEventSize where Region='{region}' order by ThisWeek desc")
    p = e.SubElement(sec, 'p')
    s = e.SubElement(p, 'span')
    s.text = "The largest events were "
    for row in data:
        a = e.SubElement(p, 'a', {'href' : f"https://www.parkrun.com.au/{row['URL']}/results/latestresults/", 'target' : '_blank', 'rel' : 'noopener noreferrer', 'class' : 'parkrunname'})
        a.text = row['ParkrunName']
        s = e.SubElement(p, 'span')
        if row['RunnersChange'] is not None:
            s.text = f" ({row['ThisWeek']}, {direction(row['RunnersChange'])} {abs(row['RunnersChange'])}){concat(row,data)}"
        else:
            s.text = f" ({row['ThisWeek']}, returning){concat(row,data)}"
    s.text += '.'
    
    data = c.execute(f"select top(5) ParkrunName, URL, ThisWeek, LastWeekP from qryWeeklyParkrunEventSize where Region='{region}' and ThisWeek IS NOT NULL order by LastWeekP desc")
    if len(data)>0:
        p = e.SubElement(sec, 'p')
        s = e.SubElement(p, 'span')
        s.text = "The largest increase by percentage was "
        for row in data:
            a = e.SubElement(p, 'a', {'href' : f"https://www.parkrun.com.au/{row['URL']}/results/latestresults/", 'target' : '_blank', 'rel' : 'noopener noreferrer', 'class' : 'parkrunname'})
            a.text = row['ParkrunName']
            if row['LastWeekP'] is not None:
                s = e.SubElement(p, 'span')
                s.text = f" ({row['ThisWeek']}, {direction(row['LastWeekP'])} {abs(row['LastWeekP']):.0f}%){concat(row,data)}"
        s.text += '.'
    
    data = c.execute(f"select ParkrunName, URL, PBs from getTop5PBs('{region}') order by PBs desc")
    if len(data)>0:
        p = e.SubElement(sec, 'p')
        s = e.SubElement(p, 'span')
        s.text = "The most PBs were at "
        for row in data:
            a = e.SubElement(p, 'a', {'href' : f"https://www.parkrun.com.au/{row['URL']}/results/latestresults/", 'target' : '_blank', 'rel' : 'noopener noreferrer', 'class' : 'parkrunname'})
            a.text = row['ParkrunName']
            s = e.SubElement(p, 'span')
            s.text = f" ({row['PBs']}){concat(row,data)}"
        s.text += '.'
    
    data = c.execute(f"select ParkrunName, URL, PBs, Percentage from getTop5PBsByPercent('{region}') order by Percentage desc")
    if len(data)>0:
        p = e.SubElement(sec, 'p')
        s = e.SubElement(p, 'span')
        s.text = "The most PBs by percentage of field was "
        for row in data:
            a = e.SubElement(p, 'a', {'href' : f"https://www.parkrun.com.au/{row['URL']}/results/latestresults/", 'target' : '_blank', 'rel' : 'noopener noreferrer', 'class' : 'parkrunname'})
            a.text = row['ParkrunName']
            s = e.SubElement(p, 'span')
            s.text = f" ({row['PBs']} or {row['Percentage']:.0f}%){concat(row,data)}"
        s.text += '.'
    
    data = c.execute(f"select ParkrunName, URL, FirstTimers, Percentage from getTop5FirstTimers('{region}') order by FirstTimers desc")
    if len(data)>0:
        p = e.SubElement(sec, 'p')
        s = e.SubElement(p, 'span')
        s.text = "The most first timers were at "
        for row in data:
            a = e.SubElement(p, 'a', {'href' : f"https://www.parkrun.com.au/{row['URL']}/results/latestresults/", 'target' : '_blank', 'rel' : 'noopener noreferrer', 'class' : 'parkrunname'})
            a.text = row['ParkrunName']
            s = e.SubElement(p, 'span')
            s.text = f" ({row['FirstTimers']}){concat(row,data)}"
        s.text += '.'
    
    data = c.execute(f"select ParkrunName, URL, FirstTimers, Percentage from getTop5FirstTimersByPercent('{region}') order by Percentage desc")
    if len(data)>0:
        p = e.SubElement(sec, 'p')
        s = e.SubElement(p, 'span')
        s.text = "The most first timers by percentage of field was at "
        for row in data:
            a = e.SubElement(p, 'a', {'href' : f"https://www.parkrun.com.au/{row['URL']}/results/latestresults/", 'target' : '_blank', 'rel' : 'noopener noreferrer', 'class' : 'parkrunname'})
            a.text = row['ParkrunName']
            s = e.SubElement(p, 'span')
            s.text = f" ({row['FirstTimers']} or {row['Percentage']:.0f}%){concat(row,data)}"
        s.text += '.'
    
    sec = e.SubElement(body, 'div', {'class' : 'section'})
    data = c.execute(f"select * from (select Rank() OVER (partition by q.AgeCategory order by q.guntime asc) as Rank, q.AthleteID, q.Athlete, q.ParkrunName, q.GunTime, q.AgeCategory, q.Comment from qryParkrunThisWeekFastestAthlete as q where q.Gender = 'F' and q.Region = '{region}') x where x.Rank = 1 order by x.GunTime asc")
    if len(data)>0:
        p = e.SubElement(sec, 'h3')
        p.text = f'The fastest among us'
        d = e.SubElement(sec, 'div')
        p = e.SubElement(d, 'p')
        p.text = f"The {len(data)} fastest females in {region} by age category, in pace order, were:"
        l = e.SubElement(d,'ol')
        for row in data:
            li = e.SubElement(l,'li')
            s = e.SubElement(li, 'a', {'class' : 'athlete', 'href' : f"https://www.parkrun.com.au/results/athleteresultshistory/?athleteNumber={row['AthleteID']}", 'target' : '_blank', 'rel' : 'noopener noreferrer', 'class' : 'parkrunname'})
            s.text = f"{row['Athlete']}"
            s = e.SubElement(li, 'span')
            s.text = f" ({row['AgeCategory']}) running {row['ParkrunName']} in {formattime(row['GunTime'])}"
            if row['Comment'] not in [None, 'PB']:
                if row['Comment'] == 'New PB!':
                    s.text += f" setting herself a new PB"
                else:
                    s.text += ' for the first time'
            s.text += '.'
        
    if len(data)>0:
        data = c.execute(f"select * from (select Rank() OVER (partition by q.AgeCategory order by q.guntime asc) as Rank, q.AthleteID, q.Athlete, q.ParkrunName, q.GunTime, q.AgeCategory, q.Comment from qryParkrunThisWeekFastestAthlete as q where q.Gender = 'M' and q.Region = '{region}') x where x.Rank = 1 order by x.GunTime asc")
        d = e.SubElement(sec, 'div')
        p = e.SubElement(d, 'p')
        p.text = f"The {len(data)} fastest males in {region} by age category, in pace order, were:"
        l = e.SubElement(d,'ol')
        for row in data:
            li = e.SubElement(l,'li')
            s = e.SubElement(li, 'a', {'class' : 'athlete', 'href' : f"https://www.parkrun.com.au/results/athleteresultshistory/?athleteNumber={row['AthleteID']}", 'target' : '_blank', 'rel' : 'noopener noreferrer', 'class' : 'parkrunname'})
            s.text = f"{row['Athlete']}"
            s = e.SubElement(li, 'span')
            s.text = f" ({row['AgeCategory']}) running {row['ParkrunName']} in {formattime(row['GunTime'])}"
            if row['Comment'] not in [None, 'PB']:
                if row['Comment'] == 'New PB!':
                    s.text += f" setting himself a new PB"
                else:
                    s.text += ' for the first time'
            s.text += '.'
    
    sec = e.SubElement(body, 'div', {'class' : 'section'})
    p = e.SubElement(sec, 'h3')
    p.text = f'Reaching Milestones'
    milestones = [500, 250, 100, 50]
    
    for m in milestones:
        data = c.execute(f"select * from getWeeklyMilestones('{region}', {m}) order by ParkrunName, LastName")
        if len(data) > 0:
            d = e.SubElement(sec, 'div')
            p = e.SubElement(d, 'p')
            if len(data) > 1:
                p.text = f"The following {len(data)} athletes join the {m} club this week:"
            else:
                p.text = f"Just one athlete joined the {m} club this week:"
            l = e.SubElement(d, 'ul')
            currentParkrun = data[0]['ParkrunName']
            li = e.SubElement(l, 'li')
            s = e.SubElement(li, 'a', {'class' : 'parkrunname', 'href' : f"https://www.parkrun.com.au/{data[0]['URL']}/results/latestresults/", 'target' : '_blank', 'rel' : 'noopener noreferrer'})
            s.text = currentParkrun
            s = e.SubElement(li, 'span')
            s.text = ': '
            for row in data:
                if row['ParkrunName'] != currentParkrun:
                    s.text = ''
                    currentParkrun = row['ParkrunName']
                    li = e.SubElement(l, 'li')
                    s = e.SubElement(li, 'a', {'class' : 'parkrunname', 'href' : f"https://www.parkrun.com.au/{row['URL']}/results/latestresults/", 'target' : '_blank', 'rel' : 'noopener noreferrer'})
                    s.text = currentParkrun
                    s = e.SubElement(li, 'span')
                    s.text = ': '
                    n = e.SubElement(li, 'a', {'class' : 'athlete', 'href' : f"https://www.parkrun.com.au/results/athleteresultshistory/?athleteNumber={row['AthleteID']}", 'target' : '_blank', 'rel' : 'noopener noreferrer'})
                    n.text = f"{row['FirstName']} {row['LastName']}"
                    s = e.SubElement(li, 'span')
                    s.text = ', '
                else:
                    n = e.SubElement(li, 'a', {'class' : 'athlete', 'href' : f"https://www.parkrun.com.au/results/athleteresultshistory/?athleteNumber={row['AthleteID']}", 'target' : '_blank', 'rel' : 'noopener noreferrer'})
                    n.text = f"{row['FirstName']} {row['LastName']}"
                    s = e.SubElement(li, 'span')
                    s.text = ', '
            s.text = ''
                

    
    
    sec = e.SubElement(body, 'div', {'class' : 'section'})
    p = e.SubElement(sec, 'h3')
    p.text = f'Record Setters'
    data = c.execute(f"select * from qryParkrunRecordsBrokenThisWeek where Region = '{region}' order by ParkrunName")    
    d = e.SubElement(sec, 'div')
    p = e.SubElement(d, 'p')
    if len(data) == 0:
        p.text = 'There were no course records broken this week.'
    else:
        if len(data) == 1:
            p.text = f'There was just one course record broken this week:'
        else:
            p.text = f'There were {len(data)} course records broken this week:'
        l = e.SubElement(d, 'ul')
        for row in data:
            ownrecord = False
            li = e.SubElement(l, 'li')
            s = e.SubElement(li, 'a', {'class' : 'parkrunname', 'href' : f"https://www.parkrun.com.au/{row['URL']}/results/latestresults/", 'target' : '_blank', 'rel' : 'noopener noreferrer'})
            s.text = row['ParkrunName']
            s = e.SubElement(li, 'span')
            s.text = ': '
            n = e.SubElement(li, 'a', {'class' : 'athlete', 'href' : f"https://www.parkrun.com.au/results/athleteresultshistory/?athleteNumber={row['AthleteID']}", 'target' : '_blank', 'rel' : 'noopener noreferrer'})
            n.text = f"{row['FirstName']} {row['LastName']} "
            rd = guntimedelta(row['PreviousRecord'], row['GunTime']).seconds
            s =  e.SubElement(li, 'span')
            if rd > 60:
                s.text = 'smashed'
            elif 20 < rd <= 60:
                s.text = 'broke'
            else:
                s.text = 'took'
            if row['FirstName'] == row['PreviousRecordHolderFirstName'] and row['LastName'] == row['PreviousRecordHolderLastName']:
                s.text += f" {genderPosessive(row['Gender'])} own record"
                ownrecord = True
            else:
                n = e.SubElement(li, 'a', {'class' : 'athlete', 'href' : f"https://www.parkrun.com.au/results/athleteresultshistory/?athleteNumber={row['PreviousRecordHolderAthleteID']}", 'target' : '_blank', 'rel' : 'noopener noreferrer'})
                n.text = f"{row['PreviousRecordHolderFirstName']} {row['PreviousRecordHolderLastName']}"
                s =  e.SubElement(li, 'span')
                s.text = f"'s record "
            s.text += f" by {guntimedelta(row['PreviousRecord'], row['GunTime']).seconds} seconds"
            if row['Comment'] == 'New PB!' and not ownrecord:
                s.text += f", setting {genderObjective(row['Gender'])}self a new PB,"
            s.text += f" running {formattime(row['GunTime'])}."
    
    data = c.execute(f"select * from qryEventRecordsBrokenThisWeek WHERE Region = '{region}'")
    d = e.SubElement(sec, 'div')
    p = e.SubElement(d, 'p')
    if len(data) == 0:
        p.text = 'There were no attendance records broken this week.'
    else:
        if len(data) == 1:
            p.text = f'There was just one attendance record broken this week:'
        else:
            p.text = f'There were {len(data)} attendance records broken this week:'
        l = e.SubElement(d, 'ul')
        for row in data:
            li = e.SubElement(l, 'li')
            s = e.SubElement(li, 'span')
            s.attrib['class'] = 'parkrunname'
            s.text = row['ParkrunName'] + ': '
            n = e.SubElement(li, 'span')
            rd = row['EventRecord'] - row['PreviousRecord']
            if rd > 60:
                margin = 'smashing'
            else:
                margin = 'breaking'
            weeks = (row['EventDate'] - row['PreviousRecordDate']).days/7
            n.text = f"with {row['EventRecord']}, {margin} their previous record of {row['PreviousRecord']} by {rd}, set "
            if row['PreviousRecordEvent'] == row['EventNumber'] - 1:
                n.text += 'last week.'
            else:
                if row['PreviousRecordEvent'] == 1:
                    n.text += f"on their launch "
                n.text += f"back on the {ordinal(row['PreviousRecordDate'].day)} of {row['PreviousRecordDate'].strftime('%B, %Y')}"
    
    sec = e.SubElement(body, 'div', {'class' : 'section'})
    p = e.SubElement(sec, 'h3')
    p.text = f'Kudos to the vollies'
    vollyP = e.SubElement(sec, 'p')
    
    sec = e.SubElement(body, 'div', {'class' : 'section'})
    p = e.SubElement(sec, 'h3')
    p.text = f'Statesmanship Top 100'
    data = c.execute(f"SELECT * FROM getStatesmanReport(100, '{region}') ORDER BY Rank, TQTY DESC")
    #data = c.execute(f"SELECT * FROM temptable ORDER BY Rank, TQTY DESC")
    
    vollies = {}
    
    colgroups = {
        'Rank'                                               : ['Rank', 'RankArrow', 'AbsRankChange'],
        'Weeks<br>Held'                                      : ['Weeks'],
        'Athlete'                                            : ['AthleteName'],
        'parkrun'                                            : ['LastRunParkrun'],
        'Events<br>Local/Total'                              : ['TotalEvents', 'DifferentEvents'],
        'Total<br>Runs'                                      : ['EventCount'],
        f'Tourist Quotient<br>Overall/{datetime.now().year}' : ['TQ', 'TQTY'],
        f'{datetime.now().year}<br>Run/New'                  : ['TYEventsDone', 'TYNewEvents'],
        f'{datetime.now().year - 1}<br>Run/New'              : ['LYEventsDone', 'LYNewEvents'],
        'P<br>Index'                                         : ['pIndex'],
        'Wilson<br>Index'                                    : ['wIndex', 'WIndexArrow', 'wIndexChange'],
        'i&#179;<br>Index'                                   : ['i3','i3Arrow', 'i3Change'],
        'A.M.E.L<br>Rank'                                    : ['AMELRank','AMELArrow', 'AMELChange']
        }
    
    t = e.SubElement(sec,'table')
    for i in colgroups:
        c = e.SubElement(t, 'colgroup', {'span': f"{len(colgroups[i])}"})
    tr1 = e.SubElement(t,'tr', {'class' : 'firstrow'})
    tr2 = e.SubElement(t,'tr')
    msgs = e.SubElement(sec, 'div')
    
    for i in colgroups:
        h = str(i).split('<br>')
        td = e.SubElement(tr1,'th', {'colspan': f"{len(colgroups[i])}", 'scope' : 'colgroup'})
        td.text = h[0]
        if len(h) > 1:
            if len(colgroups[i]) > len(h[1].split('/')):
                td = e.SubElement(tr2,'th', {'scope': 'col', 'colspan': f"{len(colgroups[i])}"})
                td.text = str(h[1])
            else:
                for x in h[1].split('/'):
                    td = e.SubElement(tr2,'th', {'scope': 'col'})
                    td.text = str(x)
        else:
            td = e.SubElement(tr2,'th')
            td.attrib['colspan'] = str(len(colgroups[i]))
            td.attrib['scope'] = 'col'
        
        
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
                if type(row[j]).__name__ in ['int', 'float']:
                    cls = 'number'
                else:
                    cls = 'text'
                if j in ['Weeks', 'pIndex']:
                    cls = 'centered'
                if j == 'Rank':
                    cls += ' firstcol' 
                if row == data[-1]:
                    cls += ' lastrow'
                if 'Arrow' in j:
                    if j == 'RankArrow':
                        switch = row['RankChange']
                    elif j == 'WIndexArrow':
                        switch = row['wIndexChange']
                    elif j == 'AMELArrow':
                        switch = row['AMELChange']
                    else:
                        switch = row['i3Change']
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
                if j == 'DifferentEvents':
                    if row[j] in [49, 99]:
                        cls += ' approachingmilestone'
                        if row[j] == 99:
                            p = e.SubElement(msgs, 'p')
                            a = e.SubElement(p, 'a', {'class' : 'athlete', 'href' : f"https://www.parkrun.com.au/results/athleteresultshistory/?athleteNumber={row['AthleteID']}", 'target' : '_blank', 'rel' : 'noopener noreferrer'})
                            a.text = f"{row['FirstName']} {row['LastName']}" 
                            s = e.SubElement(p, 'span')
                            s.text = 'is set set to join the Cowell club next week.'
                if j == 'DifferentEvents':
                    if row[j] in [50, 100]:
                        cls += ' milestone50'
                        if row[j] == 100 and row['LastRunParkrunThisWeek'] > 0 and row['EventChange'] > 0:
                            p = e.SubElement(msgs, 'p')
                            a = e.SubElement(p, 'a', {'class' : 'athlete', 'href' : f"https://www.parkrun.com.au/results/athleteresultshistory/?athleteNumber={row['AthleteID']}", 'target' : '_blank', 'rel' : 'noopener noreferrer'})
                            a.text = f"{row['FirstName']} {row['LastName']}"
                            s = e.SubElement(p, 'span')
                            s.text = f" achieved {genderPosessive(row['Gender'])} Cowell at "
                            a = e.SubElement(p, 'a', {'class' : 'athlete', 'href' : f"https://www.parkrun.com.au/{row['LastRunParkrunURL']}/results/latestresults/", 'target' : '_blank', 'rel' : 'noopener noreferrer'})
                            a.text = row['tmpParkrun']
                            s = e.SubElement(p, 'span')
                            s.text = "."
                            
                if j == 'EventCount':
                    if row[j] in [49, 99, 249, 499]:
                        cls += ' approachingmilestone'
                        p = e.SubElement(msgs, 'p')
                        a = e.SubElement(p, 'a', {'class' : 'athlete', 'href' : f"https://www.parkrun.com.au/results/athleteresultshistory/?athleteNumber={row['AthleteID']}", 'target' : '_blank', 'rel' : 'noopener noreferrer'})
                        a.text = f"{row['FirstName']} {row['LastName']}"
                        s = e.SubElement(p, 'span')
                        s.text = f" is set to run {genderPosessive(row['Gender'])} {row['EventCount'] + 1}th parkrun next week."
                    if row[j] in [50, 100, 250, 500]:
                        cls += f' milestone{row[j]}'
                        p = e.SubElement(msgs, 'p')
                        a = e.SubElement(p, 'a', {'class' : 'athlete', 'href' : f"https://www.parkrun.com.au/results/athleteresultshistory/?athleteNumber={row['AthleteID']}", 'target' : '_blank', 'rel' : 'noopener noreferrer'})
                        a.text = f"{row['FirstName']} {row['LastName']}"
                        s = e.SubElement(p, 'span')
                        s.text = f" ran {genderPosessive(row['Gender'])} {row['EventCount']}th parkrun at "
                        a = e.SubElement(p, 'a', {'class' : 'athlete', 'href' : f"https://www.parkrun.com.au/{row['LastRunParkrunURL']}/results/latestresults/", 'target' : '_blank', 'rel' : 'noopener noreferrer'})
                        a.text = row['tmpParkrun']
                        s = e.SubElement(p, 'span')
                        s.text = "."
                if j == 'pIndex':
                    if row['pIndexChange'] > 0:
                        cls += ' milestone50'
                        p = e.SubElement(msgs, 'p')
                        a = e.SubElement(p, 'a', {'class' : 'athlete', 'href' : f"https://www.parkrun.com.au/results/athleteresultshistory/?athleteNumber={row['AthleteID']}", 'target' : '_blank', 'rel' : 'noopener noreferrer'})
                        a.text = f"{row['FirstName']} {row['LastName']}"
                        s = e.SubElement(p, 'span')
                        s.text = f" ups {genderPosessive(row['Gender'])} p index to {row['pIndex']} at "
                        a = e.SubElement(p, 'a', {'class' : 'athlete', 'href' : f"https://www.parkrun.com.au/{row['LastRunParkrunURL']}/results/latestresults/", 'target' : '_blank', 'rel' : 'noopener noreferrer'})
                        a.text = row['tmpParkrun']
                        s = e.SubElement(p, 'span')
                        s.text = "."
                if j == 'i3':
                    if row['i3Change'] > 0:
                        cls += ' milestone50'
                        if j == 'i3':
                            p = e.SubElement(msgs, 'p')
                            a = e.SubElement(p, 'a', {'class' : 'athlete', 'href' : f"https://www.parkrun.com.au/results/athleteresultshistory/?athleteNumber={row['AthleteID']}", 'target' : '_blank', 'rel' : 'noopener noreferrer'})
                            a.text = f"{row['FirstName']} {row['LastName']}"
                            s = e.SubElement(p, 'span')
                            s.text = f" ups {genderPosessive(row['Gender'])} i&#179 index by {row['i3Change']} to {row['i3']} at "
                            a = e.SubElement(p, 'a', {'class' : 'athlete', 'href' : f"https://www.parkrun.com.au/{row['LastRunParkrunURL']}/results/latestresults/", 'target' : '_blank', 'rel' : 'noopener noreferrer'})
                            a.text = row['tmpParkrun']
                            s = e.SubElement(p, 'span')
                            s.text = "."
                if j in ['wIndex', 'wIndexChange']:
                    if row['wIndexChange'] > 0:
                        cls += ' milestone50'
                        if j == 'wIndex':
                            p = e.SubElement(msgs, 'p')
                            a = e.SubElement(p, 'a', {'class' : 'athlete', 'href' : f"https://www.parkrun.com.au/results/athleteresultshistory/?athleteNumber={row['AthleteID']}", 'target' : '_blank', 'rel' : 'noopener noreferrer'})
                            a.text = f"{row['FirstName']} {row['LastName']}"
                            s = e.SubElement(p, 'span')
                            s.text = f" ups {genderPosessive(row['Gender'])} Wilson index by {row['wIndexChange']} to {row['wIndex']} at "
                            a = e.SubElement(p, 'a', {'class' : 'athlete', 'href' : f"https://www.parkrun.com.au/{row['LastRunParkrunURL']}/results/latestresults/", 'target' : '_blank', 'rel' : 'noopener noreferrer'})
                            a.text = row['tmp']
                            s = e.SubElement(p, 'span')
                            s.text = "."
                if j in ['AMELRank', 'AMELChange']:
                    if row['AMELChange'] > 0:
                        cls += ' milestone50'
                        if j == 'AMELRank':
                            p = e.SubElement(msgs, 'p')
                            a = e.SubElement(p, 'a', {'class' : 'athlete', 'href' : f"https://www.parkrun.com.au/results/athleteresultshistory/?athleteNumber={row['AthleteID']}", 'target' : '_blank', 'rel' : 'noopener noreferrer'})
                            a.text = f"{row['FirstName']} {row['LastName']}"
                            s = e.SubElement(p, 'span')
                            s.text = f" ups {genderPosessive(row['Gender'])} Australian Most Events List ranking by {row['AMELChange']} to {row['AMELRank']} at "
                            a = e.SubElement(p, 'a', {'class' : 'athlete', 'href' : f"https://www.parkrun.com.au/{row['LastRunParkrunURL']}/results/latestresults/", 'target' : '_blank', 'rel' : 'noopener noreferrer'})
                            a.text = row['tmpParkrun']
                            s = e.SubElement(p, 'span')
                            s.text = "."
                if i == 'parkrun':
                    if row['LastRunParkrunThisWeek'] > 0:
                        s = e.SubElement(td, 'a', {'href' : f"https://www.parkrun.com.au/{row['LastRunParkrunURL']}/results/latestresults/", 'target' : '_blank', 'rel' : 'noopener noreferrer'})
                        s.text = fstr(row['LastRunParkrun'])
                        if row['EventChange'] > 0:
                            s.attrib['class'] = 'newevent'
                        else:
                            s.attrib['class'] = 'athlete'
                    if row['VolunteerThisWeek'] is not None:
                        if row['VolunteerThisWeek'] > 0:
                            if row['LastRunParkrunThisWeek'] > 0:
                                b = e.SubElement(td, 'br')
                            if row['LastVolParkrun'] not in vollies:
                                vollies[row['LastVolParkrun']] = {'URL' : row['LastVolParkrunURL'], 'names' : []}
                            vollies[row['LastVolParkrun']]['names'].append({'FirstName' : row['FirstName'], 'AthleteID' : row['AthleteID']})
                            s = e.SubElement(td, 'a', {'href' : f"https://www.parkrun.com.au/{row['LastVolParkrunURL']}/results/latestresults/", 'target' : '_blank', 'rel' : 'noopener noreferrer'})
                            s.text = f"({fstr(row['LastVolParkrun'])})"
                            s.attrib['class'] = 'volunteer'
                    if row['JVolunteerThisWeek'] is not None:
                        if row['JVolunteerThisWeek'] > 0:
                            if row['LastRunParkrunThisWeek'] > 0 or row['VolunteerThisWeek'] > 0:
                                b = e.SubElement(td, 'br')
                            if row['LastVolJParkrun'] not in vollies:
                                vollies[row['LastVolJParkrun']] = {'URL' : row['LastVolJParkrunURL'], 'names' : []}
                            vollies[row['LastVolJParkrun']]['names'].append({'FirstName' : row['FirstName'], 'AthleteID' : row['AthleteID']})
                            s = e.SubElement(td, 'a', {'href' : f"https://www.parkrun.com.au/{row['LastVolJParkrunURL']}/results/latestresults/", 'target' : '_blank', 'rel' : 'noopener noreferrer'})
                            s.text = f"({fstr(row['LastVolJParkrun'])})"
                            s.attrib['class'] = 'volunteer'
                    row['tmp'] = row[j]
                    row[j] = None
                if i == 'Athlete':
                    s = e.SubElement(td, 'a', {'href' : f"https://www.parkrun.com.au/results/athleteresultshistory/?athleteNumber={row['AthleteID']}", 'target' : '_blank', 'rel' : 'noopener noreferrer', 'class' : 'athlete'})
                    s.text = row['AthleteName']
                    row['AthleteName'] = None
                if len(cls) > 0:
                    td.attrib['class'] = cls
                if type(row[j]).__name__ in ['int', 'float', 'str']:
                    td.text = fstr(row[j])
    
    s = e.SubElement(vollyP, 'span')
    s.text = 'A shout out to our top 100 statespeople who chose to volunteer this weekend: '
    for p in vollies:
        for v in vollies[p]['names']:
            s = e.SubElement(vollyP, 'a', {'class' : 'athlete', 'href' : f"https://www.parkrun.com.au/results/athleteresultshistory/?athleteNumber={v['AthleteID']}", 'target' : '_blank', 'rel' : 'noopener noreferrer'})
            s.text = v['FirstName']
            s = e.SubElement(vollyP, 'span')
            if len(vollies[p]['names']) > 1:
                s.text = f"{concat(v, vollies[p]['names'])}"
        s = e.SubElement(vollyP, 'span')
        s.text = ' at '
        s = e.SubElement(vollyP, 'a', {'class' : 'athlete', 'href' : f"https://www.parkrun.com.au/{vollies[p]['URL']}/results/latestresults/", 'target' : '_blank', 'rel' : 'noopener noreferrer'})
        s.text = p
        s = e.SubElement(vollyP, 'span')
        if len(vollies)>1:
            if p == list(vollies.keys())[-2]:
                s.text = ', and '
            else:
                s.text = ', '
    s.text = ''
    
    s = e.SubElement(head,'style')
    s.text = StyleSheet

    sec = e.SubElement(body, 'div', {'class' : 'section'})
    p = e.SubElement(sec, 'p')
    p.text = "That's it from me for another week."
    p = e.SubElement(sec, 'p')
    p.text = "Until next week, keep parkrunning."

    sleep(2)
    sec = e.SubElement(body, 'div', {'class' : 'section'})
    p = e.SubElement(sec, 'h3')
    p.text = f'Column Names and Descriptions'
    tbl = e.SubElement(sec,'table')
    c = e.SubElement(tbl,'colgroup', {'span': '1'})
    c = e.SubElement(tbl,'colgroup', {'span': '1'})
 
    th = e.SubElement(tbl,'tr', {'class' : 'firstrow'})
    tr = e.SubElement(th,'th')
    tr.text = 'Column'
    tr = e.SubElement(th,'th')
    tr.text = 'Description'
    
    tr = e.SubElement(tbl,'tr', {'class' : 'altrow1'})
    td = e.SubElement(tr,'td')
    td.text = 'Rank'
    td = e.SubElement(tr,'td')
    td.text = 'Statesmanshp ranking for this state.  Note: Non public and junior events will not count toward your ranking.'
    tr = e.SubElement(tbl,'tr', {'class' : 'altrow2'})
    td = e.SubElement(tr,'td')
    td.text = 'Weeks Held'
    td = e.SubElement(tr,'td')
    td.text = 'Number of consecutive weeks statesmanship has been held.  On weeks of a double launch, this gets reset to Zero. Note: Athletes with the same rank are secondly sorted by this column.'
    tr = e.SubElement(tbl,'tr', {'class' : 'altrow1'})
    td = e.SubElement(tr,'td')
    td.text = 'Athlete'
    td = e.SubElement(tr,'td')
    td.text = "Athlete's name, linking to their profile page"
    tr = e.SubElement(tbl,'tr', {'class' : 'altrow2'})
    td = e.SubElement(tr,'td')
    td.text = 'parkrun'
    td = e.SubElement(tr,'td')
    td.text = "Last parkrun this athlete ran at, linking to that parkrun's latest result page. Note: If the athlete does not run this week, this will be blank. New parkruns in this state are shown in green, and volunteer parkruns (in any state) are shown in purple."
    tr = e.SubElement(tbl,'tr', {'class' : 'altrow1'})
    td = e.SubElement(tr,'td')
    td.text = 'Events Local/Total'
    td = e.SubElement(tr,'td')
    td.text = 'Number of different events this athlete has completed.  Local will be in this state, Total will be globally.'
    tr = e.SubElement(tbl,'tr', {'class' : 'altrow2'})
    td = e.SubElement(tr,'td')
    td.text = 'Total Runs'
    td = e.SubElement(tr,'td')
    td.text = 'Total number of parkruns completed'
    tr = e.SubElement(tbl,'tr', {'class' : 'altrow1'})
    td = e.SubElement(tr,'td')
    td.text = 'Tourist Quotient Overall/20xx'
    td = e.SubElement(tr,'td')
    td.text = 'Tourist Quotient is a percentage of parkruns completed that were new events. Interstate/international events contribute to this measure.  Total is for the entire history of the athlete, 20xx is for the current year.  Note: Athletes with the same rank and weeks are thirdly sorted by this column'
    tr = e.SubElement(tbl,'tr', {'class' : 'altrow2'})
    td = e.SubElement(tr,'td')
    td.text = '20xx Run/New'
    td = e.SubElement(tr,'td')
    td.text = 'These two sets of columns count the number of events and number of new events completed by this athlete for this year and last year.'
    tr = e.SubElement(tbl,'tr', {'class' : 'altrow1'})
    td = e.SubElement(tr,'td')
    td.text = 'P Index'
    td = e.SubElement(tr,'td')
    td.text = 'P index measures the number of different events run a number of times. EG: 4 different events run 4 times each = 4, 6 different events run 6 times each = 6. etc.'
    tr = e.SubElement(tbl,'tr', {'class' : 'altrow2'})
    td = e.SubElement(tr,'td')
    td.text = 'Wilson Index'
    td = e.SubElement(tr,'td')
    td.text = 'Highest event number completed in order starting at event 1 (a launch).  Completing an event number at any parkrun contributes to this index.'
    tr = e.SubElement(tbl,'tr', {'class' : 'altrow1'})
    td = e.SubElement(tr,'td')
    td.text = 'i&#179 index'
    td = e.SubElement(tr,'td')
    td.text = 'Highest number of parkruns completed in Australia in launch date order, starting at Main Beach.'
    tr = e.SubElement(tbl,'tr', {'class' : 'altrow2 lastrow'})
    td = e.SubElement(tr,'td')
    td.text = 'A.M.E.L Rank'
    td = e.SubElement(tr,'td')
    td.text = 'Current ranking, and movement of rank from last week, of the Australian Most Events List.'
    
    
    
    

    sec = e.SubElement(body, 'div', {'class' : 'section'})
    addSig(sec)
    
    x = e.tostring(root, pretty_print=True).decode('utf-8').replace('&amp;','&')
    with open('output.html','w') as f:
        f.write(x)
        #lxml.html.open_in_browser(root)    


def subRegionStatsReport():
    c = Connection(config)

    service = gMail.auth()
    

    colgroups = {
        'Parkrun'                                                   : ['Parkrun'],
        'Finishers<br>Volunteers'                                   : ['Finishers', 'Volunteers'],
        'Total'                                                     : ['Total'],
        f'{datetime.now().year - 1}<br>{datetime.now().year - 1} %' : ['Last Year', 'Last Year %'],
        }
    
    maillist = c.execute('SELECT Email, AddressTo, SubRegionName from SubRegions WHERE Email IS NOT NULL')
    for m in maillist:
        root = e.Element('html', version = '5.0')
        head = e.SubElement(root, 'head')
        body = e.SubElement(root, 'body')
        p = e.SubElement(body,'p')
        p.text = f"Hi {m['AddressTo']}. Below is this weeks {m['SubRegionName']} stats"
        
        stats = c.execute(f"select ParkrunName as Parkrun, TotalRunners as Finishers, TotalVolunteers as Volunteers, Total, LastYear as [Last Year], LastYearP as [Last Year %], CalendarType from qrySubRegionStats where SubRegionName = '{m['SubRegionName']}'")
        
        sec = e.SubElement(body, 'div', {'class' : 'section'})
        t = e.SubElement(sec,'table')
        for i in colgroups:
            col = e.SubElement(t, 'colgroup', {'span': f"{len(colgroups[i])}"})
        tr1 = e.SubElement(t,'tr', {'class' : 'firstrow'})
        
        for i in colgroups:
            h = str(i).split('<br>')
            if len(h) == 1:
                td = e.SubElement(tr1,'th', {'scope' : 'colgroup'})
                td.text = h[0]
            else:
                td = e.SubElement(tr1,'th', {'scope' : 'colgroup'})
                td.text = h[0]
                td = e.SubElement(tr1,'th', {'scope': 'col'})
                td.text = str(h[1])
        
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
        

        s = e.SubElement(head,'style')
        s.text = StyleSheet
        sec = e.SubElement(body, 'div', {'class' : 'section'})
        addSig(sec)
        
        with open(f"{m['SubRegionName']}.html", 'w') as f:
            f.write(e.tostring(root, pretty_print=True).decode('utf-8'))
        r = gMail.SendMessage(service, 'me', gMail.CreateMessage('me','golgothen@gmail.com', f"{m['SubRegionName']} stats for this weekend", lxml.html.tostring(root).decode('utf-8')))

def parkrunMilestoneMailout():
    
    listener = LogListener(loggingQueue)
    listener.start()
    
    logging.config.dictConfig(config)
    logger = logging.getLogger('mailout')
    
    c = Connection(config)

    service = gMail.auth()
    
    
    
    maillist = c.execute('SELECT Email, ParkrunName, Detailed from Parkruns WHERE Subscribed <> 0')
    for m in maillist:
        root = e.Element('html', version = '5.0')
        head = e.SubElement(root, 'head')
        body = e.SubElement(root, 'body')
        sec = e.SubElement(body, 'div', {'class' : 'section'})
        h = e.SubElement(sec,'h3')
        h.text = f"Upcomming milestones for {m['ParkrunName']} parkrun"
        sec = e.SubElement(body, 'div', {'class' : 'section'})
        
        if m['Detailed']:
            h.text = f"Detailed upcoming milestones for {m['ParkrunName']} parkrun"
            buildDetailParkrunReport(m['ParkrunName'], sec)
        else:
            h.text = f"Summary upcoming milestones for {m['ParkrunName']} parkrun"
            buildSummaryParkrunReport(m['ParkrunName'], sec)
        sec = e.SubElement(body, 'div', {'class' : 'section'})
        h = e.SubElement(sec,'h3')
        h.text = f"Upcoming volunteer milestones for {m['ParkrunName']} parkrun"
        buildVolunteerMilestoneReport(m['ParkrunName'], sec)
        sec = e.SubElement(body, 'div', {'class' : 'section'})
        p = e.SubElement(sec, 'p')
        p.text = "This email is automatically generated.  If you believe any of this information to be incorrect then please let me know by replying to this email address."
        p = e.SubElement(sec, 'p')
        p.text = "If you no longer wish to receive this email, or would like it sent to a different address, then again, just reply to this email and let me know."
        s = e.SubElement(head,'style')
        s.text = StyleSheet
        sec = e.SubElement(body, 'div', {'class' : 'section'})
        addSig(sec)
        r = gMail.SendMessage(service, 'me', gMail.CreateMessage('me',m['Email'], f"Weekly Upcoming Milestone Report for {m['ParkrunName']}", lxml.html.tostring(root).decode('utf-8')))
        #r = gMail.SendMessage(service, 'me', gMail.CreateMessage('me','golgothen@gmail.com', f"Weekly Upcoming Milestone Report for {m['ParkrunName']}", lxml.html.tostring(root).decode('utf-8')))
        
        logger.info(f"{r['id']} sent to {m['Email']} for parkrun {m['ParkrunName']}")
    
    listener.stop()
    
def addSig(body):
    sig = e.SubElement(body, 'div')
    
    d = e.SubElement(sig, 'div', {'class' : 'name'})
    d.text = 'Paul Ellis'
    d = e.SubElement(sig, 'div', {'class' : 'business'})
    d.text = ''
    d = e.SubElement(sig, 'div', {'class' : 'container'})
    c = e.SubElement(d, 'div', {'class' : 'title'})
    c.text = ''
    c = e.SubElement(d, 'div', {'class' : 'details'})
    p = e.SubElement(c, 'p')
    s = e.SubElement(p, 'span')
    s.text = ' E:'
    s = e.SubElement(p, 'a', {'href' : 'mailto:golgothen@gmail.com'})
    s.text = 'golgothen@gmail.com'
    p = e.SubElement(c, 'p')
    s = e.SubElement(p, 'span')
    s.text = ' M: 0420 413 301'
    p = e.SubElement(c, 'p')
    s = e.SubElement(p, 'span')
    s.text = 'IN:'
    s = e.SubElement(p, 'a', {'href' : 'https://www.linkedin.com/in/paul-ellis-2822a1189/'})
    s.text = 'Paul Ellis'
    d = e.SubElement(sig, 'div', {'class' : 'footer'})
    
def mailoutWeeklyReport():
    
    """
    Weekly parkrun report to all subscribers
    """
    
    listener = LogListener(loggingQueue)
    listener.start()
    
    logging.config.dictConfig(config)
    logger = logging.getLogger('mailout')
    
    c = Connection(config)
    
    service = gMail.auth()
    
    maillist = c.execute('SELECT Email from Subscribers WHERE Subscribed = 1')
    
    with open('output.html','r') as f:
        doc = f.read()
    
    for m in maillist:
        r = gMail.SendMessage(service, 'me', gMail.CreateMessage('me', m['Email'], f"This weeks parkrun report", doc))
        logger.info(f"{r['id']} sent to {m['Email']}")
        print(f"{r['id']} sent to {m['Email']}")
    
    listener.stop()
    
    
def part1():
    buildWeeklyParkrunReport('Victoria')

def part2():
    subRegionStatsReport()
    parkrunMilestoneMailout()
    mailoutWeeklyReport()
    
if __name__ == '__main__':
    part1()
    part2()
     
    