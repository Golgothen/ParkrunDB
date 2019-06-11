from mplogger import *
from worker import *
from terminal_size import *
import argparse, logging, logging.config
from sys import stdout
from message import Message
from parkrunlist import ParkrunList

termWidth = 0
termHeight = 0

class ProcInfo():
    def __init__(self, proc_id, pid, message = ''):
        self.id = proc_id
        self.pid = pid
        self.message = message
        self.error = ''

def printxy(x, y, text):
    stdout.write("\x1b7\x1b[%d;%df%s\x1b8" % (x, y, text))
    stdout.flush()

def paintScreen(procs, qsize):
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
    stdout.write('-' * termWidth + '\n')
    stdout.write('  {:5,} events left in work queue.'.format(qsize))
    stdout.flush()

def updateScreen(procs, qsize):
    width, height = get_terminal_size()
    if width != termWidth or height != termHeight:
        paintScreen(procs, qsize)
    else:
        for p in procs:
            printxy(p.id+2, 30, '{:.{w}}'.format(p.message + (' ' * (width - 29)), w = width - 29))
        for p in procs:
            printxy(p.id+len(procs)+3, 19, '{:.{w}}'.format(p.error + (' ' * (width - 18)), w = width - 18))
        printxy((len(procs)*2)+4, 3, '{:5,}'.format(qsize))

def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--processes', type = int, default = 2, help = 'Specify number of worker processes. Default is number of system cores.')
    parser.add_argument('--country', nargs = '+',  help = 'Specify country/ies to import. Surround the name with double quotes if it contains a space. Seperate multiple countries with spaces.')
    parser.add_argument('--region', nargs = '+',  help = 'Specify region/s to import. Surround the name with double quotes if it contains a space. Seperate multiple regions with spaces.')
    parser.add_argument('--event', nargs = '+',  help = 'Specify event/s to import. Surround the name with double quotes if it contains a space. Seperate multiple events with spaces.')
    parser.add_argument('--exclude_country', nargs = '+',  help = 'Specify country/ies to exclude from import. Surround the name with double quotes if it contains a space. Seperate multiple countries with spaces.')
    parser.add_argument('--exclude_region', nargs = '+',  help = 'Specify region/s to exclude from import. Surround the name with double quotes if it contains a space. Seperate multiple regions with spaces.')
    parser.add_argument('--exclude_event', nargs = '+',  help = 'Specify event/s to exclude from import. Surround the name with double quotes if it contains a space. Seperate multiple events with spaces.')
    parser.add_argument('--mode', nargs = 1, default = ['Normal'], help = 'Valid modes are Normal, CheckURLs or NewEvents')
    parser.add_argument('--delay', type = int, default = 10, help = 'Wait n seconds before processing the next event')
    parser.add_argument('--year', type = int, default = 0, help = 'Download results only for a specific year. Default is all, Has no effect when --MODE is NEWEVENTS')
    parser.add_argument('--inactive', type = str2bool, nargs = '?', default = False, help = 'Specify if inactive parkruns should be included. Default is False.')
    
    args = parser.parse_args()

    loggingQueue = multiprocessing.Queue()

    listener = LogListener(loggingQueue)
    listener.start()
    
    config = sender_config
    config['handlers']['queue']['queue'] = loggingQueue
    logging.config.dictConfig(config)
    logger = logging.getLogger('application')
    
    mode = Mode.default()

    mode = Mode[args.mode[0].upper()]
    
    logger.debug(args)

    #First, build a list of events that need to be checked.
    l = ParkrunList(config, mode)
    l.inactive = args.inactive
    if args.country is not None: l.countries(args.country, True)
    if args.region is not None: l.regions(args.region, True)
    if args.event is not None: l.events(args.event, True)
    
    # if no countries/regions/events were explicitly included, include all events from the database
    if len(l)==0:
        l.addAll()

    # if any exclusions, remove them from the list
    if args.exclude_country is not None: l.countries(args.exclude_country, False)
    if args.exclude_region is not None: l.regions(args.exclude_region, False)
    if args.exclude_event is not None: l.events(args.exclude_event, False)
    
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
        p.append(Worker(workQueue, r, i, mode, config, args.delay, args.year))
        p[i].start()
        procs.append(ProcInfo(i, p[i].pid))
    
    while not r.empty():
        m = r.get()
        if m.type == 'Error':
            procs[m.id].error = m.message
        else:
            procs[m.id].message = m.message

    for i in range(processes):
        workQueue.put(None)             # Add a poison pill for each process at the end of the queue.

    paintScreen(procs, workQueue.qsize())
        
    c = Connection(config)
    
    while not workQueue.empty():
        m = r.get()
        if m.type == 'Error':
            procs[m.id].error = m.message
        else:
            procs[m.id].message = m.message
        updateScreen(procs, workQueue.qsize())
    #c.execute("update p set p.LastUpdated = e.LastEvent from parkruns as p inner join (select ParkrunID, max(EventDate) as LastEvent from events group by ParkrunID) as e on p.ParkrunID = e.ParkrunID")
        
    for x in p:
        x.join()
        
    updateScreen(procs, workQueue.qsize())
    listener.stop()
    





















