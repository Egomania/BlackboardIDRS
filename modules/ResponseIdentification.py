import logging
import select
import psycopg2
import psycopg2.extensions

import time
import random
import os
from dateutil import parser

from topology import nodes, edges
from classes import alertProcessing as AP

from multiprocessing import Process, Queue

logger = logging.getLogger("idrs")

listenTo = ['alertcontext']
name = 'ResponseIdentification'

class PlugIn (Process):

    def __init__(self, q, dbs):
        Process.__init__(self)
        self.subscribe = q[listenTo[0] + '_' + name]
        self.dbs = dbs
        self.connectToDB()
        self.openIssues = {}

    def connectToDB(self):
        if self.dbs.backend == 'psql':
            self.DBconnect = psycopg2.connect(database=self.dbs.database, user=self.dbs.user, password=self.dbs.pwd, port=self.dbs.port, host=self.dbs.server)
            self.insert = self.DBconnect.cursor()
        elif self.dbs.backend == 'orient':
            self.insert = pyorient.OrientDB(self.dbs.server, self.dbs.port)
            self.DBconnect = self.insert.connect(self.dbs.user, self.dbs.pwd)
            self.insert.db_open(self.dbs.database, self.dbs.user, self.dbs.pwd)
        else:
            logger.error("Unknown Backend: %s", self.dbs.backend)

    def stop(self, commit=False):
        logger.info( 'Stopped "{0}"'.format(self.__module__) )
        self.disconnectFromDB(commit)
        
    def reconnectToDB(self, commit=False):
        self.disconnectFromDB(commit)
        self.connectToDB()

    def disconnectFromDB(self, commit=False):
        if self.dbs.backend == 'psql':
            if commit:
                self.conn.commit()
            self.insert.close()
            self.DBconnect.close()
        elif self.dbs.backend == 'orient':
            self.insert.db_close()
        else:
            pass

    def callbackFKT (self, issue):
        targetIPs = []
        print ('Timer abgelaufen: ', issue.ident)
        if self.dbs.backend == 'psql':
            query = "WITH RECURSIVE contextTree (fromnode, level, tonode) AS ( SELECT id, 0, id FROM alertcontext WHERE id = %s UNION ALL SELECT cTree.tonode, cTree.level + 1, context.fromnode FROM contexttocontext context, contextTree cTree WHERE context.tonode = cTree.fromnode) select ip.name from ip, alertcontexthastarget aht where ip.id = aht.tonode and (aht.fromnode = %s or aht.fromnode in (SELECT distinct tonode FROM contextTree WHERE level > 0));"
            query = self.insert.mogrify(query, (issue.ident, issue.ident, ))
            self.insert.execute(query)
            result = self.insert.fetchall()
            for elem in result:
                targetIPs.append(elem[0])
        elif self.dbs.backend == 'orient':
            pass
        else:
            pass

        print (targetIPs)

    def run(self):

        logger.info( 'Start "{0}"'.format(self.__module__) )
        i = 1
        while (True):
            changed = self.subscribe.get()
            
            table = changed['table']
            operation = changed['operation'].lower()
            ident = changed['ident']
            logger.info( '"{0}" got incomming change ("{1}") "{2}" in "{3}"'.format(self.__module__, operation, changed['ident'], table) )
            if ident not in self.openIssues:
                if (changed['new']['_prio'] != None and changed['new']['_prio'] > 70 and "same" in changed['new']['name']):
                    
                    self.openIssues[ident] = AP.Issue(os.path.realpath(__file__), self.__module__ , self, "callbackFKT", ident)
                    print ("Schedule : ", i, changed['new']['name'])
                    i = i + 1
                   
                        



