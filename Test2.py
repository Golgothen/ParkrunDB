from ProxyManager import *

e = multiprocessing.Event()
pm = ProxyManager(e)
pm.start()
