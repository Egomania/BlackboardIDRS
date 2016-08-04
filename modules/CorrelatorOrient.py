import logging
import select
import pyorient
import time

from topology import nodes, edges

from multiprocessing import Process, Queue

logger = logging.getLogger("idrs")

listenTo = ['alertcontext']
name = 'CorrelatorOrient'

class PlugIn (Process):

    def __init__(self, q, dbs):
        Process.__init__(self)
        self.rules = ['attackPath']
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
                logger.info("Skip Correlation. Operation is set to %s.", operation)
                continue

            name = changed['new']['name']   
            cont = True
            for elem in self.rules:
                if elem in name:
                   cont = False
            if not cont:
                logger.warning("Own aggregated alert context. Skip aggregation.")
                #print ('############################ OWN')
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
                #print (test)
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
                logger.warning("No Attack Type found. Not Correlation possible.")
                continue

            if attack.name in self.rules:
                logger.info("Skip incomming alert (%s).", attack.name)
                continue
            
            result = self.client.query("select name from (select EXPAND(OUT('alertcontexthassource')) from alertcontext where @RID = "+ident+") ",-1)
            try:
                src = nodes.ip(result[0].oRecordData['name'], client=self.client)
                logger.info("Got Source %s.", src.rid)
            except:
                logger.warning("No Source IP found. Not Correlation possible.")
                continue

            result = self.client.query("select name from (select EXPAND(OUT('alertcontexthastarget')) from alertcontext where @RID = "+ident+") ",-1)
            try:
                trg = nodes.ip(result[0].oRecordData['name'], client=self.client)
                logger.info("Got Target %s.", trg.rid)
            except:
                logger.warning("No Target IP found. Not Correlation possible.")
                continue

            result = self.client.query("select name from alertcontext where @RID = "+ident,-1)

            context = nodes.alertcontext(result[0].oRecordData['name'], client=self.client)
            logger.info("Got Context %s.", context.rid)

            logger.info("Start Correlation with %s (src), %s (trg) and %s (attack)",src,trg,attack)

            for elem in self.rules:
                if contAggregation[elem]:
                    functionCall = getattr(self, elem)(ident, table, src, trg, attack, elem, context)

    def newContext(self, src, trg, elem, context, CandContext):
        result = self.client.query("select name from (select EXPAND(OUT('alertcontexthassource')) from alertcontext where @RID = "+CandContext.rid+") ",-1)
        pathSRC = result[0].oRecordData['name']
        attackPathName = elem + "_" + pathSRC
        alertcontext = nodes.alertcontext(attackPathName, client=self.client)
        acIsOfType = edges.alertcontextisoftype(alertcontext, self.rulesToId[elem], client=self.client)
        contextToContext = edges.contexttocontext(context, alertcontext, client=self.client)
        contextToContextCand = edges.contexttocontext(CandContext, alertcontext, client=self.client)
        return alertcontext

    def addContext(self, context, rid):
        alertcontext = nodes.node() 
        alertcontext.rid = rid
        contextToContext = edges.contexttocontext(context, alertcontext, client=self.client)

    def attackPath(self, ident, table, src, trg, attack, elem, context):
        
        logger.info("Check for %s ... ", elem)
        query = "select EXPAND(IN('alertcontexthastarget')) from ip where @RID = " + src.rid
        result1 = self.client.query(query,-1)
        if len(result1) == 0:
            logger.info("No candidate context.")
            return
        
        for entry in result1:
            
            CandContext = nodes.alertcontext(entry.oRecordData['name'], client=self.client)
            # check if cand already in attack path

            queryInner = (
            "SELECT expand($c)"
            "LET $a = (select EXPAND(IN('alertcontextisoftype')) from attack where name='"+elem+"'),"
            "$b = (select EXPAND(OUT('contexttocontext')) from alertcontext where @RID = " + entry._rid + "),"
            "$c = intersect($a, $b)"
            )

            result = self.client.query(queryInner,-1)

            if len(result) == 0:
                attackPath = self.newContext(src, trg, elem, context, CandContext)
            else:
                for attackPathNode in result:
                    self.addContext(context, attackPathNode._rid)
 
        #print ("ABCDE ",src.rid, src.name, trg.rid, trg.name)
        #query = (
        #    "SELECT expand($d)"
        #    "LET $a = (select EXPAND(IN('alertcontextisoftype')) from attack where name='"+elem+"'),"
        #    "$b = (select EXPAND(IN('alertcontexthastarget')) from ip where @RID = "+src.rid+"),"
        #    "$c = (select DISTINCT(@RID) FROM (select EXPAND(OUT('contexttocontext')) from alertcontext where @RID IN $b)),"
        #    "$e = (select FROM (select EXPAND(IN('attackpathhassource')) from ip where @RID = "+trg.rid+")),"
        #    "$f = intersect($a, $c),"
        #    "$d = difference($e, $f)"
        #    )
        #query = (
        #"SELECT expand($c)"
        #"LET $a = (select EXPAND(IN('alertcontextisoftype')) from attack where name='"+elem+"'),"
        #""
        #)
        #result = self.client.query(query,-1)
        #if len(result) == 0:
        #if len(result) == 0:
        #    alertcontext = self.newContext(src, trg, elem, context)
        #    for entry in result1:
        #        #print ("+++++++++++++++++++++++++", entry)
        #        context = nodes.alertcontext(entry.name, client=self.client)
        #        contextToContext = edges.contexttocontext(context, alertcontext, client=self.client)
        #else:
        #    for entry in result:
        #        #print ("#######################", entry)
        #        self.addContext(context, entry._rid)
        

