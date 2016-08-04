import logging
import select
import pyorient
import time
import random

from topology import nodes, edges

from multiprocessing import Process, Queue

logger = logging.getLogger("idrs")

listenTo = ['alertcontext']
name = 'PrioSimAlertContextOrient'

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

        query = "SELECT EXPAND(IN('contexttocontext')) from alertcontext where @RID = " + contextRid
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

    def update(self, rid, value, key):
        for i in range(10):
            try:
                UpdateResult = self.client.command("Update alertcontext set " + key + " = " + str(value) + " where @RID = " + rid)
                logger.info("Update Prio of alertContext %s with %s. (%s)", rid, value, UpdateResult)
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
            

            if operation != 'update':
                logger.info("Skip Operation (%s). - No Update Operation on AlertContext (%s).", operation, ident)
                continue

            # dirty hack to wait for the commit
            if "-" in ident:
                while (True):
                    result = self.client.query("select from alertcontext where name = '" + changed['new']['name']+"'",-1)
                    if len(result) > 0:
                        ident = result[0]._rid
                        logger.info("Got commit for %s", ident)
                        break

            try: 
                if changed['new']['in_contexttocontext'] != changed['original']['in_contexttocontext']:
                    logger.info("Update self (%s) as new incomming edge was inserted.", ident)
                    alertContextPrio = self.getPrioVal(ident)
                    if alertContextPrio != None:
                        self.update(ident, alertContextPrio, "_prio")
            except:
                pass

            try:
                prioNew = changed['new']['_prio']
            except:
                logger.info("No new Prio update for %s. Skip Priorisation.", ident)
                continue

            try:
                prioOld = changed['original']['_prio']
            except:
                prioOld = None

            if prioNew == None or prioNew == prioOld:
                logger.info("No new Prio update for %s. Skip Priorisation.", ident)
                continue

            logger.info("create Resulting Prios starting from : %s", ident)

                    
            query = "TRAVERSE OUT('contexttocontext') FROM " + ident
            result = self.client.query(query,-1)
                
            for entry in result:
                if type(entry) is pyorient.otypes.OrientRecordLink:
                    alertContextID = entry.clusterID + ":" + entry.recordPosition
                elif type(entry) is pyorient.otypes.OrientRecord:
                    alertContextID = entry._rid
                else:
                    logger.warning("Unkown Return Type %s.", type(entry))
                    alertContextID = None
                    continue

                if alertContextID == ident:
                    continue

                alertContextPrio = self.getPrioVal(alertContextID)
                if alertContextPrio != None:
                    logger.info("created Prio %s for %s.", alertContextPrio, alertContextID)
                    self.update(alertContextID, alertContextPrio, "_prio")


