from ProxyManager import *
import multiprocessing

loggingQueue = multiprocessing.Queue()

listener = LogListener(loggingQueue)
listener.start()

config = sender_config
config['handlers']['queue']['queue'] = loggingQueue


e = multiprocessing.Event()
pm = ProxyManager(e, config)
pm.start()
