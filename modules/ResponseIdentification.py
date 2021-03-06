import logging
import psycopg2
import psycopg2.extensions
import pyorient
import os
import gc
import threading

from operator import itemgetter
from multiprocessing import Process, Queue

from topology import nodes, edges
from classes import alertProcessing as AP

from helper_functions import dbConnector
from helper_functions import query_helper as qh

listenTo = ['alertcontext']
name = 'ResponseIdentification'

logger = logging.getLogger("idrs."+name)
logger.setLevel(20)

class callbackQueueOperator(threading.Thread):
    def __init__(self, q, dbs):
        threading.Thread.__init__(self)
        self.dbs = dbs
        self.q = q
        self.alive = True
        dbConnector.connectToDB(self)

    def stop(self):
        self.alive = False
        dbConnector.disconnectFromDB(self, True)

    def run(self):
        while (self.alive):
            issue = self.q.get()
            if issue == None:
                self.stop()
                break
            logger.info('Timer abgelaufen: %s', issue.ident)
            issue.sheduled = True
            # create issue as alert context node, if no suitable candidate
            functionName = 'getIssueNotSolved' + self.dbs.backend.title()
            issueID = getattr(qh, functionName)(self.insert, issue.ident)
            if issueID == None:
                issueNode = nodes.alertcontext(name=issue.name, rid=None, client=self.insert)
                
            else:
                issueNode = nodes.alertcontext(rid=issueID[0], client=self.insert)
                
            alertContextNode = nodes.alertcontext(rid = issue.ident, client=self.insert)
            contextTocontext = edges.contexttocontext(alertContextNode, issueNode, client=self.insert)


            # get infos
            functionName = 'geteffectedEntities' + self.dbs.backend.title()
            effectedEntities = getattr(qh, functionName)(self.insert, issue.ident)
            functionName = 'getImplementations' + self.dbs.backend.title()
            implementationsOnEffected = getattr(qh, functionName)(self.insert, effectedEntities)
            functionName = 'getImplementationsAttack' + self.dbs.backend.title()
            implementationsForAttack = getattr(qh, functionName)(self.insert, issue.ident)
       
            # intersection between applicable and helpful responses
            implementations = list(set(implementationsOnEffected) & set(implementationsForAttack))
            
            newBundle = nodes.bundle(name = issueNode.name, rid = None, client=self.insert)
            bundlesolvesalertcontext = edges.bundlesolvesalertcontext(newBundle, issueNode, client=self.insert)
            for elem in implementations:
                implNode = nodes.implementation(rid = elem, client=self.insert)
                newedge = edges.implementationisinbundle(implNode, newBundle, client=self.insert)
            
            self.DBconnect.commit()

class PlugIn (Process):

    def __init__(self, q, dbs):
        Process.__init__(self)
        self.subscribe = q[listenTo[0] + '_' + name]
        self.dbs = dbs
        dbConnector.connectToDB(self)
        self.openIssues = {}
        self.callbackQueue = Queue()

    def stop(self, commit=False):
        logger.info( 'Stopped "{0}"'.format(self.__module__) )
        dbConnector.disconnectFromDB(self, commit)
        self.callbackQueue.put(None)

    def callbackFKT (self, issue):
        self.callbackQueue.put(AP.IssueMin(issue))

    def run(self):

        logger.info( 'Start "{0}"'.format(self.__module__) )
        
        callbackOperator = callbackQueueOperator(self.callbackQueue, self.dbs)
        callbackOperator.start()

        functionName = 'getSuperContextWithoutIssue' + self.dbs.backend.title()
        functionNameObs = 'getSubContext' + self.dbs.backend.title()
        while (True):
            changed = self.subscribe.get()
            ident = changed['ident']
            
            if changed['operation'].lower() == "delete":
                continue

            # continue after delete and skip operation
            if  '_solved' in changed['new'].keys():
                if changed['new']['_solved'] != None and changed['new']['_solved'] != 'None':
                    if changed['new']['_solved']:
                        if ident in self.openIssues.keys():
                            del self.openIssues[ident]
                            logger.info( 'Deleted Issue %s -- Remaining Issues: %s', ident, self.openIssues.keys())
                            continue
                

            # own context --> skip operation
            if 'issue' in changed['new']['name']:
                logger.debug ('Issue Alert (OWN) : (%s) %s -- Skip Operation', ident, changed['new']['name'])
                continue
            
            if ident not in self.openIssues:
                # todo : trigger conditions refinement
                #if '_prio' in changed['new'].keys():
                if True:
                    #if changed['new']['_prio'] != None and changed['new']['_prio'] != 'None': 
                    if True:
                        #if int(changed['new']['_prio']) > 0: 
                        if ("sameC" in changed['new']['name']):
                           
                            #get super context = highest alert context in hierarchy
                            superContexts = getattr(qh, functionName)(self.insert, ident)
                            
                            if len(superContexts) > 0:
                                maxVal = max(superContexts,key=itemgetter(1))[1]
                            
                            for elem in superContexts:
                                superContextIden = elem[0]
                                if superContextIden not in self.openIssues:
                                    if elem[1] == maxVal:
                                        self.openIssues[superContextIden] = AP.Issue(os.path.realpath(__file__), self.__module__ , self, "callbackFKT", superContextIden)
                                        logger.info("Schedule (%s) : %s", superContextIden, changed['new']['name'])
                                    else:
                                        self.openIssues[superContextIden] = None
                                        logger.info("Insert Empty (%s) : %s", superContextIden, changed['new']['name'])
                                else:
                                    if self.openIssues[superContextIden] != None:
                                        if not self.openIssues[superContextIden].sheduled:
                                            self.openIssues[superContextIden].restartTimer()
                                            logger.info("Restart Timer (%s) : %s", superContextIden, changed['new']['name'])
                                # Check for obsolete issues 
                                subContexts = getattr(qh, functionNameObs)(self.insert, superContextIden)
                                
                                for entry in subContexts:
                                    subContextIdent = entry[0]
                                    if subContextIdent in self.openIssues and self.openIssues[subContextIdent] != None:
                                        self.openIssues[subContextIdent].t.cancel()
                                        self.openIssues[subContextIdent] = None
                                        logger.info("Obsolete Entry (%s) : %s", superContextIden, changed['new']['name'])
                                        
                
                   
                        



