import logging
import select
import psycopg2
import psycopg2.extensions
import queue

import time
import random
from dateutil import parser

from topology import nodes, edges

from multiprocessing import Process, Queue

listenTo = ['alert']
name = 'PrioSimAlertPsql'

logger = logging.getLogger("idrs."+name)
#logger.setLevel(20)

class PlugIn (Process):

    def __init__(self, q, dbs):
        Process.__init__(self)
        self.subscribe = q[listenTo[0] + '_' + name]
        self.dbs = dbs
        self.connectToDB()

    def connectToDB(self):
        self.conn = psycopg2.connect(database=self.dbs.database, user=self.dbs.user, password=self.dbs.pwd, port=self.dbs.port, host=self.dbs.server)
        self.cur = self.conn.cursor()

    def stop(self, commit=False):
        logger.info( 'Stopped "{0}"'.format(self.__module__) )
        self.disconnectFromDB(commit)
        
    def reconnectToDB(self, commit=False):
        self.disconnectFromDB(commit)
        self.connectToDB()

    def disconnectFromDB(self, commit=False):
        if commit:
            self.conn.commit()
        self.cur.close()
        self.conn.close()

    def getPrioVal(self, contextRid):

        newContextPrio = 0
        alertCounter = 0

        queries = ["SELECT a.id, a._prio from alerttocontext ac, alert a where ac.tonode = %s AND ac.fromnode = a.id","SELECT a.id, a._prio from contexttocontext ac, alertcontext a where ac.tonode = %s AND ac.fromnode = a.id"]

        for statement in queries:
            statement = self.cur.mogrify(statement, (contextRid, ))
            self.cur.execute(statement)
            result = self.cur.fetchall()

            for entry in result:
                
                try:
                    prioAlert = entry[1]
                except:
                    prioAlert = None
                if prioAlert != None:
                    newContextPrio = newContextPrio + int(prioAlert)
                    alertCounter = alertCounter + 1
        
        try:
        	newPrioValue = newContextPrio / alertCounter
        	ret = int(round(newPrioValue,0))
        except:
            ret = None

        return ret

    def update(self, rid, value, key, table):
        UpdateResult = self.cur.execute("Update " + table + " set " + key + " = " + str(value) + " where id = " + str(rid))
        logger.debug("Update Prio of %s %s with %s. (%s)", table, rid, value, UpdateResult)


    def run(self):

        logger.info( 'Start "{0}"'.format(self.__module__) )

        while (True):
            changed = self.subscribe.get()
            
            table = changed['table']
            operation = changed['operation'].lower()
            ident = changed['ident']
            logger.debug( '"{0}" got incomming change ("{1}") "{2}" in "{3}"'.format(self.__module__, operation, changed['ident'], table) )

            if operation != 'insert':
                logger.debug("Skip Operation. Operation done was: %s", operation)
                self.conn.commit()
                continue


            prio = random.randint(1, 100)
            self.update(ident, prio, "_prio", "alert")
            logger.info("Update Prio of alert %s with %s.", ident, prio)
            statement = "select a.tonode from alerttocontext a where a.fromnode = %s"
            statement = self.cur.mogrify(statement, (ident, ))
            self.cur.execute(statement)
            result = self.cur.fetchall()
            
            for elem in result:
                
                contextRid = elem[0]
                
                # Update directly connected alert Context
                    
                contextPrio = self.getPrioVal(contextRid) 
                if contextPrio != None:
                    self.update(contextRid, contextPrio, "_prio", "alertcontext")

            self.conn.commit()


