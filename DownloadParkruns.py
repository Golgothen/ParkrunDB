from Worker import *
from TerminalSize import *
import argparse
from sys import stdout
from message import Message
from TerminalSize import *

termWidth = 0
termHeight = 0

class ProcInfo():
    def __init__(self, id, pid, message = ''):
        self.id = id
        self.pid = pid
        self.message = message
        self.error = ''

def getParkrunList(parkruns, q):
    c = Connection()
    for p in parkruns:
        rs = c.execute("SELECT * FROM getLastImportedEventByCountry('" + p + "')")
        for r in rs.fetchall():
            q.put({'Name': r[0], 'url':r[1], 'lastEvent':r[2]})
    c.close()

def printxy(x, y, text):
    stdout.write("\x1b7\x1b[%d;%df%s\x1b8" % (x, y, text))
    stdout.flush()

def paintScreen(procs):
    os.system('cls')
    global termWidth
    global termHeight
    termWidth, termHeight = get_terminal_size()
    stdout.write('-- Processes: --:-- PID --:--  Message  ' + (termWidth - 40) * '-' + '\n')
    for p in procs:
        stdout.write(':   Process {:2.0f}  :  {:5.0f}  :  {:.{w}}'.format(p.id, p.pid, p.message, w = termWidth - 29) + '\n')
    stdout.write('-- Last Error Messages ' + (termWidth - 23) * '-' + '\n')
    for p in procs:
        stdout.write(':   Process {:2.0f}  :  {:.{w}}'.format(p.id, p.error, w = termWidth - 18) + '\n')

def updateScreen(procs):
    width, height = get_terminal_size()
    if width != termWidth or height != termHeight:
        paintScreen(procs)
    else:
        for p in procs:
            printxy(p.id+2, 30, '{:.{w}}'.format(p.message + (' ' * (width - 29)), w = width - 29))
        for p in procs:
            printxy(p.id+len(procs)+3, 19, '{:.{w}}'.format(p.error + (' ' * (width - 18)), w = width - 18))

def updateParkruns(countries, workerCount):
    q = multiprocessing.Queue()     #Work Queue
    r = multiprocessing.Queue()     #Message Queue

    if len(countries)==0:
        print("No country supplied. Exiting.")
        exit()

    if workerCount == -1:
        processes = multiprocessing.cpu_count()
    else:
        processes = int(workerCount)

    p = []
    procs = []
    for i in range(processes):
        p.append(Worker(q, r, i))
        p[i].start()
        procs.append(ProcInfo(i, p[i].pid))
    
    while not r.empty():
        m = r.get()
        if m.type == 'Error':
            procs[m.id].error = m.message
        else:
            procs[m.id].message = m.message

    paintScreen(procs)
        
    getParkrunList(countries, q)            # Fill the queue
    for i in range(processes):
        q.put(None)             # Add a poison pill for each process at the end of the queue.
    
    while not q.empty():
        m = r.get()
        if m.type == 'Error':
            procs[m.id].error = m.message
        else:
            procs[m.id].message = m.message
        updateScreen(procs)
        
    for x in p:
        x.join()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--processes', type = int, default = -1, help = 'Specify number of worker processes. Default is -1 (number of system cores)')
    parser.add_argument('--country', nargs = '+', default = ['Australia'],  help = 'Specify a country to import by name. Surround the country name with double quotes if it contains a space.')
    args = parser.parse_args()
    updateParkruns(args.country, args.processes)
