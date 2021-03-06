import logging
import select
import pyorient
import time
import random
from dateutil import parser

from topology import nodes, edges

from multiprocessing import Process, Queue

logger = logging.getLogger("idrs")

listenTo = ['alert']
name = 'PrioSimAlertOrient'

class PlugIn (Process):

    def __init__(self, q, dbs):
        Process.__init__(self)
        self.subscribe = q[listenTo[0] + '_' + name]
        self.dbs = dbs
        self.connectToDB()    

    def connectToDB(self):
        self.client = pyorient.OrientDB(self.dbs.server, self.dbs.port)
        self.session_id = self.client.connect(self.dbs.user, self.dbs.pwd)
        self.client.db_open(self.dbs.database, self.dbs.user, self.dbs.pwd)

    def disconnectFromDB(self):
        self.client.db_close()

    def reconnectToDB(self):
        self.disconnectFromDB()
        self.connectToDB()

    def refreshDB(self):
        self.client.db_reload()

    def stop(self):
        logger.info( 'Stopped "{0}"'.format(self.__module__) )
        self.disconnectFromDB()

    def getPrioVal(self, contextRid):

        newContextPrio = 0
        alertCounter = 0

        queries = ["SELECT EXPAND(IN('alerttocontext')) from alertcontext where @RID = " + contextRid, "SELECT EXPAND(IN('contexttocontext')) from alertcontext where @RID = " + contextRid ]

        for query in queries:

            result = self.client.query(query,-1)
                        
            for entry in result:
                try:
                    prioAlert = entry.oRecordData['_prio']
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
        for i in range(10):
            try:
                UpdateResult = self.client.command("Update " + table + " set " + key + " = " + str(value) + " where @RID = " + rid)
                logger.debug("Update Prio of %s %s with %s. (%s)", table, rid, value, UpdateResult)
                break
            except:
                pass


    def run(self):

        logger.info( 'Start "{0}"'.format(self.__module__) )

        while (True):
            changed = self.subscribe.get()
            table = changed['table']
            operation = changed['operation']
            ident = changed['ident']
            logger.info( '"{0}" got incomming change ("{1}") "{2}" in "{3}"'.format(self.__module__, operation, changed['ident'], table) )

            if operation != 'insert':
                logger.info("Skip Operation.")
                continue

            # dirty hack to wait for the commit
            if "-" in ident:
                while (True):
                    dt = parser.parse(changed['new']['detectiontime'])
                    result = self.client.query("select from alert where name = '" + changed['new']['name']+"' and detectiontime = DATE('" + str(dt) + "')",-1)
                    if len(result) > 0:
                        ident = result[0]._rid
                        break


            prio = random.randint(1, 100)
            self.update(ident, prio, "_prio", "alert")
            logger.info("Update Prio of alert %s with %s.", ident, prio)

            result = self.client.query("select EXPAND(OUT('alerttocontext')) from alert where @RID = "+ident,-1)
            
            for elem in result:
                
                contextRid = elem._rid
                
                # Update directly connected alert Context
                    
                contextPrio = self.getPrioVal(contextRid) 
                if contextPrio != None:
                    self.update(contextRid, contextPrio, "_prio", "alertcontext")


