import logging
import select
import psycopg2
import psycopg2.extensions

from multiprocessing import Process, Queue
from topology import nodes

listenTo = ['alertcontext']
name = 'CorrelatorPsql'

logger = logging.getLogger("idrs."+name)
#logger.setLevel(20)

class PlugIn (Process):
    def __init__(self, q, dbs):
        Process.__init__(self)
        self.rules = ['attackPath']
        self.subscribe = q[listenTo[0] + '_' + name]
        self.dbs = dbs
        self.connectToDB()
        self.rulesToId = {}
        for elem in self.rules:
            searchStatement = 'SELECT id FROM attack WHERE name like %s'
            searchStatement = self.cur.mogrify(searchStatement, (elem, ))
            self.cur.execute(searchStatement)
            ident = self.cur.fetchone()
            if ident == None:
                statement = 'insert into attack (name) VALUES(%s) RETURNING id;'
                statement = self.cur.mogrify(statement, (elem, ))
                self.cur.execute(statement)
                self.rulesToId[elem] = self.cur.fetchone()[0]
                self.conn.commit()
            else:
                self.rulesToId[elem] = ident[0]       

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

    def run(self):

        logger.info( 'Start "{0}"'.format(self.__module__) )

        while (True):
            while not self.subscribe.empty():
                changed = self.subscribe.get()
                
                table = changed['table']
                operation = changed['operation']
                ident = changed['ident']
                logger.debug( '"{0}" got incomming change ("{1}") "{2}" in "{3}"'.format(self.__module__, operation, ident, table) )
                if operation == 'delete' or operation == 'update':
                    logger.debug("Skip Aggregation. Operation is set to %s.", operation)
                    self.conn.commit()
                    continue
                
                name = changed['new']['name']   
                cont = True
                for elem in self.rules:
                    if name in elem:
                        cont = False
                if not cont:
                    logger.debug("Own aggregated alert context. Skip aggregation.")
                    self.conn.commit()
                    continue

                contAggregation = []
                for elem in ['%attackPath%']:
                    statement = "select a.id from contexttocontext c, alertcontext a where c.fromnode = %s and c.tonode = a.id and a.name like %s"
                    statement = self.cur.mogrify(statement, (ident, elem, ))
                    self.cur.execute(statement)
                    test = self.cur.fetchall()
                    
                    if len(test) != 0:
                        logger.debug("Elem already in %s. Skip correlation.", elem)
                        contAggregation.append(False)
                    else:
                        contAggregation.append(True)

                if True not in contAggregation:
                    logger.debug("Elem already completely aggregated. Skip aggregation.")
                    self.conn.commit()
                    continue

                meta = {}
                for elem in ['alertcontexthassource', 'alertcontexthastarget', 'alertcontextisoftype']:
                    statement = "select a.tonode from " + elem + " a where a.fromnode = %s"
                    statement = self.cur.mogrify(statement, (ident, ))
                    self.cur.execute(statement)
                    test = self.cur.fetchall()
                    if len(test) == 0:
                        logger.debug("No information given in table %s. Skip aggregation.", elem)
                        cont = False
                    else:
                        meta[elem] = test[0]
                if not cont:
                    self.conn.commit()
                    continue
                
                for elem in self.rules:
                    functionCall = getattr(self, elem)(ident, meta)
                    self.conn.commit()
                
                
    def attackPath(self, ident, meta):
        source = meta['alertcontexthassource'][0]
        target = meta['alertcontexthastarget'][0]
        
        

        statement = "select a.id from alertcontext a, alertcontexthastarget source where source.tonode = %s and a.id = source.fromnode"
        statement = self.cur.mogrify(statement, (source, ))
        
        self.cur.execute(statement)
        result = self.cur.fetchall()

        if len(result) == 0:
            
            logger.debug("No candidate context.")
            self.conn.commit()
            return

        for entry in result:
            
            CandContextID = entry[0]
            
            # check if cand already in attack path

            statement = "select distinct(super.id) from alertcontext super, alertcontextisoftype isatt, contexttocontext has, alertcontext sub where super.id = isatt.fromnode and isatt.tonode = %s and has.fromnode = %s and super.id = has.tonode"
            statement = self.cur.mogrify(statement, (self.rulesToId["attackPath"], CandContextID ))
            
            self.cur.execute(statement)
            attackPath = self.cur.fetchall()
            
            if len(attackPath) == 0:

                self.cur.execute("select i.name from ip i, alertcontexthassource a where i.id = a.tonode and a.fromnode = " + str(CandContextID) + ";")
                pathSRC = self.cur.fetchone()[0]
                attackPathName = "attackPath_" + pathSRC
                
                logger.info("Insert Attack Path: %s", attackPathName)

                attackPathEntry = nodes.alertcontext(attackPathName, client=self.cur)
                attackPathNode = attackPathEntry.rid
                statement = 'INSERT INTO alertcontextisoftype (name, fromnode, tonode) VALUES (%s,%s, %s) RETURNING id;'
                statement = self.cur.mogrify(statement, ("alertcontextisoftype",attackPathNode ,self.rulesToId["attackPath"],  ))
                self.cur.execute(statement)
                statement = "INSERT INTO contexttocontext (name, fromnode, tonode) VALUES (%s, %s, %s)"
                statement = self.cur.mogrify(statement, ("contexttocontext", CandContextID, attackPathNode ))
                self.cur.execute(statement)
                statement = "INSERT INTO contexttocontext (name, fromnode, tonode) VALUES (%s, %s, %s)"
                statement = self.cur.mogrify(statement, ("contexttocontext", ident, attackPathNode ))
                self.cur.execute(statement)

            else:
                for attackPathNode in attackPath:
                    
                    statement = "INSERT INTO contexttocontext (name, fromnode, tonode) VALUES (%s, %s, %s)"
                    statement = self.cur.mogrify(statement, ("contexttocontext", CandContextID, attackPathNode[0] ))
                    self.cur.execute(statement)
                    statement = "INSERT INTO contexttocontext (name, fromnode, tonode) VALUES (%s, %s, %s)"
                    statement = self.cur.mogrify(statement, ("contexttocontext", ident, attackPathNode[0] ))
                    self.cur.execute(statement)

        self.conn.commit()
        
