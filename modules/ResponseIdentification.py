import logging
import psycopg2
import psycopg2.extensions
import os

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

    def connectToDBTemp(self):
        if self.dbs.backend == 'psql':
            DBconnect = psycopg2.connect(database=self.dbs.database, user=self.dbs.user, password=self.dbs.pwd, port=self.dbs.port, host=self.dbs.server)
            insert = DBconnect.cursor()
        elif self.dbs.backend == 'orient':
            insert = pyorient.OrientDB(self.dbs.server, self.dbs.port)
            DBconnect = insert.connect(self.dbs.user, self.dbs.pwd)
            insert.db_open(self.dbs.database, self.dbs.user, self.dbs.pwd)
        else:
            logger.error("Unknown Backend: %s", self.dbs.backend)
        return (DBconnect, insert)

    def stop(self, commit=False):
        logger.info( 'Stopped "{0}"'.format(self.__module__) )
        self.disconnectFromDB(commit)
        
    def reconnectToDB(self, commit=False):
        self.disconnectFromDB(commit)
        self.connectToDB()

    def disconnectFromDB(self, commit=False):
        if self.dbs.backend == 'psql':
            if commit:
                self.DBconnect.commit()
            self.insert.close()
            self.DBconnect.close()
        elif self.dbs.backend == 'orient':
            self.insert.db_close()
        else:
            pass

    def disconnectFromDBTemp(self, DBconnect, insert, commit=False):
        if self.dbs.backend == 'psql':
            if commit:
                DBconnect.commit()
            insert.close()
            DBconnect.close()
        elif self.dbs.backend == 'orient':
            insert.db_close()
        else:
            pass

    def geteffectedEntitiesOrient(self, insert, issue):

        effectedEntities = {}

        # todo : orient query
        listing = []
        for elem in result:
            listing.append(elem[0])

        effectedEntities['service'] = listing

        # todo : orient query
        listing = []
        for elem in result:
            listing.append(elem[0])

        effectedEntities['host'] = listing

        # todo : orient query
        listing = []
        for elem in result:
            listing.append(elem[0])

        effectedEntities['user'] = listing

        return effectedEntities

    def getImplementationsOrient(self, insert, effectedEntities):

        impls = []

        # todo : orient query

        return impls     

    def getImplementationsAttackOrient(self, insert, issue):  

        impls = []

        # todo : orient query

        return impls   

    def geteffectedEntitiesPsql(self, insert, issue):

        effectedEntities = {}

        query = "WITH RECURSIVE contextTree (fromnode, level, tonode) AS ( SELECT id, 0, id FROM alertcontext WHERE id = %s UNION ALL SELECT cTree.tonode, cTree.level + 1, context.fromnode FROM contexttocontext context, contextTree cTree WHERE context.tonode = cTree.fromnode) select distinct d.id from service d, alertcontexthasservicetarget aht where d.id = aht.tonode and (aht.fromnode = %s or aht.fromnode in (SELECT distinct tonode FROM contextTree WHERE level > 0));"
        query = insert.mogrify(query, (issue.ident, issue.ident, ))
        insert.execute(query)
        result = insert.fetchall()
        listing = []
        for elem in result:
            listing.append(elem[0])

        effectedEntities['service'] = listing

        query = "WITH RECURSIVE contextTree (fromnode, level, tonode) AS ( SELECT id, 0, id FROM alertcontext WHERE id = %s UNION ALL SELECT cTree.tonode, cTree.level + 1, context.fromnode FROM contexttocontext context, contextTree cTree WHERE context.tonode = cTree.fromnode) select distinct d.id from device d, alertcontexthashosttarget aht where d.id = aht.tonode and (aht.fromnode = %s or aht.fromnode in (SELECT distinct tonode FROM contextTree WHERE level > 0));"
        query = insert.mogrify(query, (issue.ident, issue.ident, ))
        insert.execute(query)
        result = insert.fetchall()
        listing = []
        for elem in result:
            listing.append(elem[0])

        effectedEntities['host'] = listing

        query = "WITH RECURSIVE contextTree (fromnode, level, tonode) AS ( SELECT id, 0, id FROM alertcontext WHERE id = %s UNION ALL SELECT cTree.tonode, cTree.level + 1, context.fromnode FROM contexttocontext context, contextTree cTree WHERE context.tonode = cTree.fromnode) select distinct d.id from users d, alertcontexthasusertarget aht where d.id = aht.tonode and (aht.fromnode = %s or aht.fromnode in (SELECT distinct tonode FROM contextTree WHERE level > 0));"
        query = insert.mogrify(query, (issue.ident, issue.ident, ))
        insert.execute(query)
        result = insert.fetchall()
        listing = []
        for elem in result:
            listing.append(elem[0])

        effectedEntities['user'] = listing

        return effectedEntities

    def getImplementationsPsql(self, insert, effectedEntities):

        impls = []

        # get implementations for hostbased deployed on device
        if len(effectedEntities['host']) > 0:
            query = "select distinct rel.fromnode from implementationisdeployedondevice rel where rel.tonode in %s and rel.fromnode in (select has.tonode  from hostbasedisaresponse isa, responsehasimplementation has where isa.tonode = has.fromnode);"
            query = insert.mogrify(query, (tuple(effectedEntities['host']), ))
            insert.execute(query)
            result = insert.fetchall()
            for elem in result:
                impls.append(elem[0])

        # get implementations for userbased deployed on device the user is logged on

        if len(effectedEntities['user']) > 0:
            query = "select distinct rel.fromnode from implementationisdeployedondevice rel, userloggedondevice uld where rel.tonode = uld.tonode and uld.fromnode in %s and rel.fromnode in (select has.tonode  from userbasedisaresponse isa, responsehasimplementation has where isa.tonode = has.fromnode);"
            query = insert.mogrify(query, (tuple(effectedEntities['user']), ))
            insert.execute(query)
            result = insert.fetchall()
            for elem in result:
                impls.append(elem[0])

        # get implementations for servciebased deployed on device the service is running on
        if len(effectedEntities['service']) > 0:
            query = "select distinct rel.fromnode from implementationisdeployedondevice rel, servicerunsondevice srd where rel.tonode = srd.tonode and srd.fromnode in %s and rel.fromnode in (select has.tonode  from servicebasedisaresponse isa, responsehasimplementation has where isa.tonode = has.fromnode);"
            query = insert.mogrify(query, (tuple(effectedEntities['service']), ))
            insert.execute(query)
            result = insert.fetchall()
            for elem in result:
                impls.append(elem[0])

        return impls      

    def getImplementationsAttackPsql(self, insert, issue):  

        impls = []

        query = "WITH RECURSIVE contextTree (fromnode, level, tonode) AS ( SELECT id, 0, id FROM alertcontext WHERE id = %s UNION ALL SELECT cTree.tonode, cTree.level + 1, context.fromnode FROM contexttocontext context, contextTree cTree WHERE context.tonode = cTree.fromnode) select distinct i.tonode from responsehasimplementation i, responsemitigatesconsequence r, attackhasconsequence a, alertcontextisoftype aht where i.fromnode = r.fromnode and r.tonode = a.tonode and a.fromnode = aht.tonode and (aht.fromnode = %s or aht.fromnode in (SELECT distinct tonode FROM contextTree WHERE level > 0));"
        query = insert.mogrify(query, (issue.ident, issue.ident, ))
        insert.execute(query)
        result = insert.fetchall()
        for elem in result:
            impls.append(elem[0])

        return impls

    def callbackFKT (self, issue):

        (DBConnect, insert) = self.connectToDBTemp()

        print ('Timer abgelaufen: ', issue.ident)
        if self.dbs.backend == 'psql':
            effectedEntities = self.geteffectedEntitiesPsql(insert, issue)    
            implementationsOnEffected = self.getImplementationsPsql(insert, effectedEntities)
            implementationsForAttack = self.getImplementationsAttackPsql(insert, issue)
        elif self.dbs.backend == 'orient':
            effectedEntities = self.geteffectedEntitiesOrient(insert, issue)    
            implementationsOnEffected = self.getImplementationsOrient(insert, effectedEntities)
            implementationsForAttack = self.getImplementationsAttackOrient(insert, issue)
        else:
            logger.error("Unknown backend: %s", self.dbs.backend)
            effectedEntities = {}
            implementationsOnEffected = []
            implementationsForAttack = []

        # intersection between applicable and helpful responses
        implementations = list(set(implementationsOnEffected) & set(implementationsForAttack))
        
        newBundle = nodes.bundle(name = issue.name, rid = None, client=insert)
        for elem in implementations:
            implNode = nodes.implementation(rid = elem, client=insert)
            newedge = edges.implementationisinbundle(implNode, newBundle, client=insert)

        self.disconnectFromDBTemp(DBConnect, insert, commit = True)

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
                # todo : trigger conditions refinement
                if (changed['new']['_prio'] != None and changed['new']['_prio'] > 0 and "sameClassification_a3" in changed['new']['name']):
                    
                    self.openIssues[ident] = AP.Issue(os.path.realpath(__file__), self.__module__ , self, "callbackFKT", ident)
                    print ("Schedule : ", i, changed['new']['name'])
                    i = i + 1
                   
                        



