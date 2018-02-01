from worker import *
from terminal_size import *
import argparse
from sys import stdout
from message import Message
from parkrunlist import ParkrunList
from enum import Enum

termWidth = 0
termHeight = 0

class ProcInfo():
    def __init__(self, id, pid, message = ''):
        self.id = id
        self.pid = pid
        self.message = message
        self.error = ''

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

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--processes', type = int, default = -1, help = 'Specify number of worker processes. Default is number of system cores.')
    parser.add_argument('--country', nargs = '+',  help = 'Specify country/ies to import. Surround the name with double quotes if it contains a space.')
    parser.add_argument('--region', nargs = '+',  help = 'Specify region/s to import. Surround the name with double quotes if it contains a space.')
    parser.add_argument('--event', nargs = '+',  help = 'Specify event/s to import. Surround the name with double quotes if it contains a space.')
    parser.add_argument('--mode', nargs = 1, default = ['Normal'], help = 'Valid modes are Normal, CheckURLs or NewEvents')
    args = parser.parse_args()

    mode = Mode.default()
    
    #First, build a list of events that need to be checked.
    l = ParkrunList()
    if args.country is not None: l.addCountries(args.country)
    if args.region is not None: l.addRegions(args.region)
    if args.event is not None: l.addEvents(args.event)

    mode = Mode[args.mode[0].upper()]
    
    if int(args.processes) == -1:
        processes = multiprocessing.cpu_count()
    else:
        processes = int(args.processes)
    
    # Define and fill the work queue
    workQueue = multiprocessing.Queue()
    for p in l:
        workQueue.put(p)
    
    r = multiprocessing.Queue()     #Message Queue

    p = []
    procs = []
    for i in range(processes):
        p.append(Worker(workQueue, r, i, mode))
        p[i].start()
        procs.append(ProcInfo(i, p[i].pid))
    
    while not r.empty():
        m = r.get()
        if m.type == 'Error':
            procs[m.id].error = m.message
        else:
            procs[m.id].message = m.message

    paintScreen(procs)
        
    for i in range(processes):
        workQueue.put(None)             # Add a poison pill for each process at the end of the queue.
    
    while not WorkQueue.empty():
        m = r.get()
        if m.type == 'Error':
            procs[m.id].error = m.message
        else:
            procs[m.id].message = m.message
        updateScreen(procs)
        
    for x in p:
        x.join()





















