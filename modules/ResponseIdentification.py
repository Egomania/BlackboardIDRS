import logging
import psycopg2
import psycopg2.extensions
import pyorient
import os

from topology import nodes, edges
from classes import alertProcessing as AP

from multiprocessing import Process, Queue
from helper_functions import dbConnector
from helper_functions import query_helper as qh

logger = logging.getLogger("idrs")

listenTo = ['alertcontext']
name = 'ResponseIdentification'

class PlugIn (Process):

    def __init__(self, q, dbs):
        Process.__init__(self)
        self.subscribe = q[listenTo[0] + '_' + name]
        self.dbs = dbs
        dbConnector.connectToDB(self)
        self.openIssues = {}

    def stop(self, commit=False):
        logger.info( 'Stopped "{0}"'.format(self.__module__) )
        dbConnector.disconnectFromDB(self, commit)

    def callbackFKT (self, issue):

        (DBConnect, insert) = dbConnector.connectToDBTemp(self)

        print ('Timer abgelaufen: ', issue.ident)
        functionName = 'geteffectedEntities' + self.dbs.backend.title()
        effectedEntities = getattr(qh, functionName)(insert, issue.ident)
        functionName = 'getImplementations' + self.dbs.backend.title()
        implementationsOnEffected = getattr(qh, functionName)(insert, effectedEntities)
        functionName = 'getImplementationsAttack' + self.dbs.backend.title()
        implementationsForAttack = getattr(qh, functionName)(insert, issue.ident)
   
        # intersection between applicable and helpful responses
        implementations = list(set(implementationsOnEffected) & set(implementationsForAttack))
        
        newBundle = nodes.bundle(name = issue.name, rid = None, client=insert)
        alertContextNode = nodes.alertcontext(rid = issue.ident, client=insert)
        bundlesolvesalertcontext = edges.bundlesolvesalertcontext(newBundle, alertContextNode, client=insert)
        for elem in implementations:
            implNode = nodes.implementation(rid = elem, client=insert)
            newedge = edges.implementationisinbundle(implNode, newBundle, client=insert)

        dbConnector.disconnectFromDBTemp(self, DBConnect, insert, commit = True)

    def run(self):

        logger.info( 'Start "{0}"'.format(self.__module__) )
        while (True):
            changed = self.subscribe.get()
            
            table = changed['table']
            operation = changed['operation'].lower()
            ident = changed['ident']
            logger.info( '"{0}" got incomming change ("{1}") "{2}" in "{3}"'.format(self.__module__, operation, changed['ident'], table) )
            if ident not in self.openIssues:
                # todo : trigger conditions refinement
                if (changed['new']['_prio'] != None and changed['new']['_prio'] > 0 and "sameClassification_a1" in changed['new']['name']):
                    
                    self.openIssues[ident] = AP.Issue(os.path.realpath(__file__), self.__module__ , self, "callbackFKT", ident)
                    logger.info("Schedule : %s", changed['new']['name'])
                   
                        



