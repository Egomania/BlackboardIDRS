import logging
import time
import threading
import random
import operator

from multiprocessing import Process, Queue

from helper_functions import query_helper as qh
from helper_functions import dbConnector
from topology import nodes, edges

listenTo = ['bundle']
name = 'ResponseEvaluation'

logger = logging.getLogger("idrs."+name)
logger.setLevel(20)

class prepareEvaluation(threading.Thread):
    def __init__(self, dbs, bundle, listing, openEvals, openImpls):
        threading.Thread.__init__(self)
        self.dbs = dbs
        dbConnector.connectToDB(self)
        self.bundle = bundle
        self.list = listing
        self.openEvals = openEvals
        self.openImpls = openImpls

    def run(self):
        logger.info("Start preparation for bundle: %s", self.bundle)
        self.list.append(self.bundle)
        functionName = "selectSingleValue" + self.dbs.backend.title()
        metrics = getattr(qh, functionName)(self.insert, "metric", "id")
        metricList = []
        implList = []
        functionName = 'getLastSelectedImplementationsWithExecutor' + self.dbs.backend.title()
        result = getattr(qh, functionName)(self.insert, self.bundle)
        functionName = "selectSingleEdgeValue" + self.dbs.backend.title()
        for impl in result:
            implList.append(impl[2])
            for metric in metrics:
                metric_ident = getattr(qh, functionName)(self.insert, "implementationhasmetric", "id", impl[2], metric[0])
                metricList.append(metric_ident[0])
        self.openEvals[self.bundle] = metricList
        self.openImpls[self.bundle] = implList
        functionName = 'updateNode' + self.dbs.backend.title()
        getattr(qh, functionName)(self.DBconnect, self.insert, "bundle", self.bundle, {"_ready": True}, True)
        getattr(qh, functionName)(self.DBconnect, self.insert, "bundle", self.bundle, {"_prepared": False}, True)
        self.list.remove(self.bundle)
        dbConnector.disconnectFromDB(self, True)

    def stopThread(self):
        self.DBconnect.cancel()
        dbConnector.disconnectFromDB(self, False)
        

class evaluateExecution(threading.Thread):
    def __init__(self, dbs, bundle, listing, openEvals, openImpls):
        threading.Thread.__init__(self)
        self.dbs = dbs
        dbConnector.connectToDB(self)
        self.bundle = bundle
        self.list = listing
        self.openEvals = openEvals
        self.openImpls = openImpls

    def run(self):
        logger.info("Start THREAD evaluation for: %s", self.bundle)
        self.list.append(self.bundle)
        # do some measurements here

    def stop(self):
        logger.info("Stop THREAD evaluation for: %s", self.bundle)
        self.list.remove(self.bundle)
        functionName = 'updateNode' + self.dbs.backend.title()
        for elem in self.openEvals[self.bundle]:
            value = random.random()
            getattr(qh, functionName)(self.DBconnect, self.insert, "implementationhasmetric", elem, {"_value": value}, True)
        del self.openEvals[self.bundle]
        updateValues = {'_executed' : True}
        functionName = 'updateEdge' + self.dbs.backend.title()
        for elem in self.openImpls[self.bundle]:
            getattr(qh, functionName)(self.DBconnect, self.insert, 'implementationisinbundle', elem, self.bundle, updateValues, True)
        
        success = random.randint(2, 10)

        #failed response
        if success == 1:
            logger.info("Response Operation Failed for: %s", self.bundle)
            getattr(qh, functionName)(self.DBconnect, self.insert, "bundle", self.bundle, {"_active": True}, True)
        else:
            contextList = []
            logger.info("Response Operation Successful for: %s", self.bundle)
            functionName = 'selectSingleValue' + self.dbs.backend.title()
            contextID = getattr(qh, functionName)(self.insert, "bundlesolvesalertcontext", "tonode", "fromnode", self.bundle , fetchall=True)
            functionName = 'getSubContextOrder' + self.dbs.backend.title()
            subContext = getattr(qh, functionName)(self.insert, contextID[0])
            restart = False
            if len(subContext) != 0:
                maxVal = subContext[0][1]
            for elem in subContext:

                if elem[1] == maxVal:
                    # get infos
                    functionName = 'geteffectedEntities' + self.dbs.backend.title()
                    effectedEntities = getattr(qh, functionName)(self.insert, elem[0])
                    functionName = 'getImplementations' + self.dbs.backend.title()
                    implementationsOnEffected = getattr(qh, functionName)(self.insert, effectedEntities)
                    functionName = 'getImplementationsAttack' + self.dbs.backend.title()
                    implementationsForAttack = getattr(qh, functionName)(self.insert, elem[0])
           
                    # intersection between applicable and helpful responses
                    implementations = list(set(implementationsOnEffected) & set(implementationsForAttack))

                    intersectList = list(set(implementations) & set(self.openImpls[self.bundle]))

                    if len(intersectList) != 0 and elem[0] not in contextList:
                        contextList.append(elem[0])
                    if len(intersectList) == 0:
                        restart = True

                else:
                    if elem[2] in contextList and elem[0] not in contextList:
                        contextList.append(elem[0])
                    if elem[2] not in contextList:
                        restart = True
            
            functionName = 'updateNode' + self.dbs.backend.title()
            for elem in contextList:
                getattr(qh, functionName)(self.DBconnect, self.insert, "alertcontext", elem, {"_solved": True}, True)

            if restart:
                logger.info("Partially unsolved issue, restart Bundle: %s", self.bundle)
                functionName = 'updateNode' + self.dbs.backend.title()
                getattr(qh, functionName)(self.DBconnect, self.insert, "bundle", self.bundle, {"_active": True}, True)

        del self.openImpls[self.bundle]
        dbConnector.disconnectFromDB(self, True)

    def stopThread(self):
        self.DBconnect.cancel()
        dbConnector.disconnectFromDB(self, False)

class PlugIn (Process):

    def __init__(self, q, dbs):
        Process.__init__(self)
        self.subscribe = q[listenTo[0] + '_' + name]
        self.dbs = dbs
        dbConnector.connectToDB(self)
        self.prepared = []
        self.executing = []
        self.openEvals = {}
        self.openImpls = {}
        self.executorThreads = {}

    def stop(self):
        logger.info( 'Stopped "{0}"'.format(self.__module__) )
        for elem in self.executorThreads:
            logger.info("Stop Execution THREAD")
            elem.stopThread()
        dbConnector.disconnectFromDB(self, True)

    def run(self):

        logger.info( 'Start "{0}"'.format(self.__module__) )
        while (True):
            changed = self.subscribe.get()
            bundle = changed['new']['id']
            prepared = changed['new']['_prepared']
            executing = changed['new']['_executing']
            if (prepared != None) and (prepared) and bundle not in self.prepared:
                prepareThread = prepareEvaluation(self.dbs, bundle, self.prepared, self.openEvals, self.openImpls)
                prepareThread.start()
                continue
            if (executing != None) and (executing):
                evalThread = evaluateExecution(self.dbs, bundle, self.executing, self.openEvals, self.openImpls)
                evalThread.start()
                self.executorThreads[bundle] = evalThread
                continue
            if (executing != None) and not (executing) and bundle in self.executing:
                self.executorThreads[bundle].stop()

