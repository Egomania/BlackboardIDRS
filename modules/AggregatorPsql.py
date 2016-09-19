import logging
import select
import psycopg2
import psycopg2.extensions

from multiprocessing import Process, Queue
from topology import nodes as nodes
from topology import edges as edges

listenTo = ['alertcontext']
name = 'AggregatorPsql'

logger = logging.getLogger("idrs."+name)
#logger.setLevel(20)

class PlugIn (Process):
    def __init__(self, q, dbs):
        Process.__init__(self)
        self.rules = ['sameSource', 'sameTarget','sameClassification']
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
                    if elem in name:
                        cont = False
                if not cont:
                    logger.debug("Own aggregated alert context. Skip aggregation.")
                    self.conn.commit()
                    continue

                contAggregation = []
                for elem in ['%sameSource%', '%sameTarget%', '%sameClassification%']:
                    statement = "select a.id from contexttocontext c, alertcontext a where c.fromnode = %s and c.tonode = a.id and a.name like %s"
                    statement = self.cur.mogrify(statement, (ident, elem, ))
                    self.cur.execute(statement)
                    test = self.cur.fetchall()
                    
                    if len(test) != 0:
                        logger.debug("Elem already in %s. Skip aggregation.", elem)
                        contAggregation.append(False)
                    else:
                        contAggregation.append(True)

                if True not in contAggregation:
                    logger.debug("Elem already completely aggregated. Skip aggregation.")
                    self.conn.commit()
                    continue

                for elem in ['alertcontexthassource', 'alertcontexthastarget', 'alertcontextisoftype']:
                    statement = "select * from " + elem + " a where a.fromnode = %s"
                    statement = self.cur.mogrify(statement, (ident, ))
                    self.cur.execute(statement)
                    test = self.cur.fetchall()
                    if len(test) == 0:
                        logger.debug("No information given in table %s. Skip aggregation.", elem)
                        cont = False
                    
                if not cont:
                    self.conn.commit()
                    continue
                
                try:
                    name.split("_")[0]
                    name.split("_")[1]
                    name.split("_")[2]
                except:
                    logger.warning("Bad formed alertcontext %s. Skip aggregation.", name)
                    self.conn.commit()
                    continue
                
                if contAggregation[0]:
                    same(self, ident, 'alertcontexthassource', 'sameSource', name.split("_")[0])
                if contAggregation[1]:
                    same(self, ident, 'alertcontexthastarget', 'sameTarget', name.split("_")[1])
                if contAggregation[2]:
                    same(self, ident, 'alertcontextisoftype', 'sameClassification', name.split("_")[2])
                self.conn.commit()
                
                
            
def same(self, alertContext, table, attacktype, name):
    statement = 'select t.tonode from ' + table + ' t where t.fromnode = %s'
    
    statement = self.cur.mogrify(statement, (alertContext, ))
    self.cur.execute(statement)
    elem = self.cur.fetchone()
    
    #statement = 'select a.id from alertContext a, ' + table + ' t, alertcontextisoftype type, attack att where t.tonode = %s and att.name = %s and t.fromnode = a.id and type.fromnode = a.id and type.tonode = att.id;'
    statement = "select t.fromnode from " + table + " t where t.tonode = %s"

    #statement = self.cur.mogrify(statement, (elem, alertContext[1], ))
    statement = self.cur.mogrify(statement, (elem, ))
    self.cur.execute(statement)
    result = self.cur.fetchall()
    
    if len(result) > 1:
        statement = "select distinct(super.id) from alertcontext super, contexttocontext has, alertcontext sub, alertcontextisoftype isatt where has.fromnode in %s and super.id = isatt.fromnode and isatt.tonode = %s and super.id = has.tonode"
        contextName = attacktype + "_" + name.replace(" ", "")
        statement = self.cur.mogrify(statement, (tuple(result),self.rulesToId[attacktype] , ))
        
        self.cur.execute(statement)
        superset = self.cur.fetchone()
        
        if superset == None:
            
            statement = 'INSERT INTO alertcontext (name, _solved) VALUES (%s,%s) RETURNING id;'
            statement = self.cur.mogrify(statement, (contextName, False, ))
            self.cur.execute(statement)
            superset = self.cur.fetchone()
            statement = 'INSERT INTO alertcontextisoftype (name, fromnode, tonode) VALUES (%s,%s, %s) RETURNING id;'
            statement = self.cur.mogrify(statement, ("alertcontextisoftype",superset[0] ,self.rulesToId[attacktype],  ))
            self.cur.execute(statement)
            logger.info("Aggregated: %s into %s", alertContext, table)
            
                
        supernode = nodes.node()
        supernode.rid = superset[0]
        subnode = nodes.node()
        
        for elem in result:
            try:
                subnode.rid = elem[0]
                contexttocontext = edges.contexttocontext(fromNode = subnode, toNode = supernode, client=self.cur)
                
            except Exception as e:
                logger.error( 'Error in "{0}": "{1}"'.format(self.__module__, e) ) 
                self.conn.rollback()   

    self.conn.commit()
