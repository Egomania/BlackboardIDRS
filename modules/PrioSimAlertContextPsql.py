import logging
import select
import psycopg2
import psycopg2.extensions
import time
import random

from topology import nodes, edges

from multiprocessing import Process, Queue

listenTo = ['alertcontext']
name = 'PrioSimAlertContextPsql'

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
        self.disconnectFromDB(commit=commit)
        
    def reconnectToDB(self, commit=False):
        self.disconnectFromDB(commit=commit)
        self.connectToDB()

    def disconnectFromDB(self, commit=False):
        if commit:
            self.conn.commit()
        self.cur.close()
        self.conn.close()

    def getPrioVal(self, contextRid):

        newContextPrio = 0
        alertCounter = 0

        statement = "SELECT a.id, a._prio from contexttocontext ac, alertcontext a where ac.tonode = %s AND ac.fromnode = a.id"
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

    def update(self, rid, value, key):
        UpdateResult = self.cur.execute("Update alertcontext set " + key + " = " + str(value) + " where id = " + str(rid))
        logger.debug("Update Prio of alertcontext %s with %s. (%s)", rid, value, UpdateResult)
        

    def run(self):

        logger.info( 'Start "{0}"'.format(self.__module__) )

        while (True):
            changed = self.subscribe.get()
            
            table = changed['table']
            operation = changed['operation'].lower()
            ident = changed['ident']
            logger.debug( '"{0}" got incomming change ("{1}") "{2}" in "{3}"'.format(self.__module__, operation, changed['ident'], table) )
            

            if operation == 'update':
                logger.debug("Skip Operation (%s). - No Update Operation on AlertContext (%s).", operation, ident)
                self.conn.commit()
                continue

            if operation == 'insert':
                alertContextID = ident
                statement = "SELECT * from alerttocontext where tonode = %s"
                statement = self.cur.mogrify(statement, (ident, ))
                self.cur.execute(statement)
                result = self.cur.fetchall()
                if len(result) != 0:
                    logger.debug("Not my departement....")
                    self.conn.commit()
                    continue
                alertContextPrio = self.getPrioVal(alertContextID)
                if alertContextPrio != None:
                    logger.info("created Prio %s for %s.", alertContextPrio, alertContextID)
                    self.update(alertContextID, alertContextPrio, "_prio")
                self.conn.commit()
                continue

            try:
                prioNew = changed['new']['_prio']
            except:
                logger.debug("No new Prio update for %s. Skip Priorisation.", ident)
                self.conn.commit()
                continue

            try:
                prioOld = changed['original']['_prio']
            except:
                prioOld = None

            if prioNew == None or prioNew == prioOld:
                logger.debug("No new Prio update for %s. Skip Priorisation.", ident)
                self.conn.commit()
                continue

            logger.debug("create Resulting Prios starting from : %s", ident)

                    
            statement = "select a.tonode from alerttocontext a where a.fromnode = %s"
            statement = self.cur.mogrify(statement, (ident, ))
            self.cur.execute(statement)
            result = self.cur.fetchall()
            for entry in result:
                alertContextID = entry[0]
                if alertContextID == ident:
                    continue

                alertContextPrio = self.getPrioVal(alertContextID)
                if alertContextPrio != None:
                    logger.info("created Prio %s for %s.", alertContextPrio, alertContextID)
                    self.update(alertContextID, alertContextPrio, "_prio")

            self.conn.commit()


