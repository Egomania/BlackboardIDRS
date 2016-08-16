import logging
import time
import threading
import random

from multiprocessing import Process, Queue

from helper_functions import query_helper as qh
from helper_functions import dbConnector
from topology import nodes, edges

listenTo = ['bundle']
name = 'ResponseEvaluation'

logger = logging.getLogger("idrs."+name)
logger.setLevel(20)

class prepareEvaluation(threading.Thread):
    def __init__(self, dbs, bundle, listing, openEvals):
        threading.Thread.__init__(self)
        self.dbs = dbs
        dbConnector.connectToDB(self)
        self.bundle = bundle
        self.list = listing
        self.openEvals = openEvals

    def run(self):
        logger.info("Start preparation for bundle: %s", self.bundle)
        self.list.append(self.bundle)
        functionName = "selectSingleValue" + self.dbs.backend.title()
        metrics = getattr(qh, functionName)(self.insert, "metric", "id")
        metricList = []
        functionName = 'getLastSelectedImplementationsWithExecutor' + self.dbs.backend.title()
        result = getattr(qh, functionName)(self.insert, self.bundle)
        functionName = "selectSingleEdgeValue" + self.dbs.backend.title()
        for impl in result:
            for metric in metrics:
                metric_ident = getattr(qh, functionName)(self.insert, "implementationhasmetric", "id", impl[2], metric[0])
                metricList.append(metric_ident[0])
        self.openEvals[self.bundle] = metricList
        functionName = 'updateNode' + self.dbs.backend.title()
        getattr(qh, functionName)(self.DBconnect, self.insert, "bundle", self.bundle, {"_ready": True}, True)
        getattr(qh, functionName)(self.DBconnect, self.insert, "bundle", self.bundle, {"_prepared": False}, True)
        self.list.remove(self.bundle)
        dbConnector.disconnectFromDB(self, True)

class evaluateExecution(threading.Thread):
    def __init__(self, dbs, bundle, listing, openEvals):
        threading.Thread.__init__(self)
        self.dbs = dbs
        dbConnector.connectToDB(self)
        self.bundle = bundle
        self.list = listing
        self.openEvals = openEvals

    def run(self):
        logger.info("Start THREAD evaluation for: %s", self.bundle)
        self.list.append(self.bundle)
        

    def stop(self):
        logger.info("Stop THREAD evaluation for: %s", self.bundle)
        self.list.remove(self.bundle)
        functionName = 'updateNode' + self.dbs.backend.title()
        for elem in self.openEvals[self.bundle]:
            value = random.random()
            getattr(qh, functionName)(self.DBconnect, self.insert, "implementationhasmetric", elem, {"_value": value}, True)
        del self.openEvals[self.bundle]
        dbConnector.disconnectFromDB(self, True)

class PlugIn (Process):

    def __init__(self, q, dbs):
        Process.__init__(self)
        self.subscribe = q[listenTo[0] + '_' + name]
        self.dbs = dbs
        dbConnector.connectToDB(self)
        self.prepared = []
        self.executing = []
        self.openEvals = {}
        self.executorThreads = {}

    def stop(self):
        logger.info( 'Stopped "{0}"'.format(self.__module__) )
        dbConnector.disconnectFromDB(self, True)

    def run(self):

        logger.info( 'Start "{0}"'.format(self.__module__) )
        while (True):
            changed = self.subscribe.get()
            bundle = changed['new']['id']
            prepared = changed['new']['_prepared']
            executing = changed['new']['_executing']
            if (prepared != None) and (prepared) and bundle not in self.prepared:
                prepareThread = prepareEvaluation(self.dbs, bundle, self.prepared, self.openEvals)
                prepareThread.start()
                continue
            if (executing != None) and (executing):
                evalThread = evaluateExecution(self.dbs, bundle, self.executing, self.openEvals)
                evalThread.start()
                self.executorThreads[bundle] = evalThread
                continue
            if (executing != None) and not (executing) and bundle in self.executing:
                self.executorThreads[bundle].stop()
