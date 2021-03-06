import logging
import select
import pyorient
import time

from topology import nodes, edges

from multiprocessing import Process, Queue

logger = logging.getLogger("idrs")

listenTo = ['alertcontext']
name = 'AggregatorOrient'


class PlugIn (Process):  
    def __init__(self, q, dbs):
        Process.__init__(self)
        self.rules = ['sameSource', 'sameTarget', 'sameClassification']
        self.subscribe = q[listenTo[0] + '_' + name]
        self.dbs = dbs
        self.connectToDB()
        self.rulesToId = {}
        for elem in self.rules:
            self.rulesToId[elem] = nodes.attack(elem)
    

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

    def run(self):

        logger.info( 'Start "{0}"'.format(self.__module__) )

        while (True):
            changed = self.subscribe.get()
            #print (self.subscribe.qsize())
            table = changed['table']
            operation = changed['operation']
            ident = changed['ident']
            logger.info( '"{0}" got incomming change ("{1}") "{2}" in "{3}"'.format(self.__module__, operation, changed['ident'], table) )

            if operation == 'delete' or operation == 'update':
                logger.info("Skip Aggregation. Operation is set to %s.", operation)
                continue

            name = changed['new']['name']   
            cont = True
            for elem in self.rules:
                if elem in name:
                   cont = False
            if not cont:
                logger.warning("Own aggregated alert context. Skip aggregation.")
                continue

            # dirty hack to wait for the commit
            if "-" in ident:
                while (True):
                    result = self.client.query("select from alertcontext where name = '" + changed['new']['name']+"'",-1)
                    if len(result) > 0:
                        ident = result[0]._rid
                        break

            contAggregation = {}
            for elem in self.rules:
                test = self.client.query("select from (select name from (select EXPAND(OUT('contexttocontext')) from alertcontext where @RID = "+ident+")) where name like '%" + elem + "%'",-1)
                #print (len(test), test)
                if len(test) != 0:
                    logger.warning("Elem already in %s. Skip aggregation.", elem)
                    contAggregation[elem] = (False)
                else:
                    contAggregation[elem] = (True)
            if True not in contAggregation.values():
                logger.warning("Elem already completely aggregated. Skip aggregation.")
                #print ("+++++++++++++++++ ALL done")
                continue

            #print (contAggregation)

            result = self.client.query("select name from (select EXPAND(OUT('alertcontextisoftype')) from alertcontext where @RID = "+ident+") ",-1)
            try:
                attack = nodes.attack(result[0].oRecordData['name'], client=self.client)
                logger.info("Got Attack %s.", attack.rid)
            except:
                logger.warning("No Attack Type found. Not Aggregatable.")
                continue

            #if attack.name in self.rules:
            #    logger.info("Skip incomming alert (%s).", attack.name)
            #    continue
            
            result = self.client.query("select name from (select EXPAND(OUT('alertcontexthassource')) from alertcontext where @RID = "+ident+") ",-1)
            try:
                src = nodes.ip(result[0].oRecordData['name'], client=self.client)
                logger.info("Got Source %s.", src.rid)
            except:
                logger.warning("No Source IP found. Not Aggregatable.")
                continue

            result = self.client.query("select name from (select EXPAND(OUT('alertcontexthastarget')) from alertcontext where @RID = "+ident+") ",-1)
            try:
                trg = nodes.ip(result[0].oRecordData['name'], client=self.client)
                logger.info("Got Target %s.", trg.rid)
            except:
                logger.warning("No Target IP found. Not Aggregatable.")
                continue

            logger.info("Start Aggregation with %s (src), %s (trg) and %s (attack)",src,trg,attack)
            #print("Start Aggregation with ",src.name,trg.name,attack.name)

            for elem in self.rules:
                if contAggregation[elem]:
                    functionCall = getattr(self, elem)(src, trg, attack, elem)

    def newContext(self, name, elem):
        alertcontext = nodes.alertcontext(name, client=self.client)
        acIsOfType = edges.alertcontextisoftype(alertcontext, self.rulesToId[elem], client=self.client)
        return alertcontext

    def addContext(self, context, rid):
        alertcontext = nodes.node() 
        alertcontext.rid = rid
        contextToContext = edges.contexttocontext(alertcontext, context, client=self.client)

    def sameSource(self, src, trg, attack, elem):
        logger.info("Check for %s ... ", elem)

        query = "select count(*) from (select EXPAND(IN('alertcontexthassource')) from ip WHERE IN('alertcontexthassource').size() > 1 and @RID = " + src.rid + ")"
        result = self.client.query(query,-1)

        if result[0].oRecordData['count'] > 0:
            name = elem + "_" + src.name
            
            curContext = self.newContext(name, elem)

            query = (
            "select EXPAND($c) "
            "LET $a = (select EXPAND(IN('alertcontexthassource')) from ip WHERE IN('alertcontexthassource').size() > 1 and @RID = " + src.rid + "),"
            "$b = (select EXPAND(IN('contexttocontext')) from alertcontext where @RID = " + curContext.rid + "),"
            "$c = difference($a, $b)"
            )
            result = self.client.query(query,-1)

            for elem in result:
                self.addContext(curContext, elem._rid) 
        
    def sameTarget(self, src, trg, attack, elem):
        logger.info("Check for %s ... ", elem)

        query = "select count(*) from (select EXPAND(IN('alertcontexthastarget')) from ip WHERE IN('alertcontexthastarget').size() > 1 and @RID = " + trg.rid + ")"
        result = self.client.query(query,-1)

        if result[0].oRecordData['count'] > 0:
            name = elem + "_" + trg.name
            
            curContext = self.newContext(name, elem)

            query = (
            "select EXPAND($c) "
            "LET $a = (select EXPAND(IN('alertcontexthastarget')) from ip WHERE IN('alertcontexthastarget').size() > 1 and @RID = " + trg.rid + "),"
            "$b = (select EXPAND(IN('contexttocontext')) from alertcontext where @RID = " + curContext.rid + "),"
            "$c = difference($a, $b)"
            )
            result = self.client.query(query,-1)

            for elem in result:
                self.addContext(curContext, elem._rid) 


    def sameClassification(self, src, trg, attack, elem):
        logger.info("Check for %s ... ", elem)
        
        query = "select count(*) from (select EXPAND(IN('alertcontextisoftype')) from attack WHERE IN('alertcontextisoftype').size() > 1 and @RID = " + attack.rid + ")"
        result = self.client.query(query,-1)

        if result[0].oRecordData['count'] > 0:
            name = elem + "_" + attack.name.replace(" ", "")
            
            curContext = self.newContext(name, elem)

            query = (
            "select EXPAND($c) "
            "LET $a = (select EXPAND(IN('alertcontextisoftype')) from attack WHERE IN('alertcontextisoftype').size() > 1 and @RID = " + attack.rid + "),"
            "$b = (select EXPAND(IN('contexttocontext')) from alertcontext where @RID = " + curContext.rid + "),"
            "$c = difference($a, $b)"
            )
            result = self.client.query(query,-1)

            for elem in result:
                self.addContext(curContext, elem._rid) 


