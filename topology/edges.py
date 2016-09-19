import logging
from errors import error

logger = logging.getLogger("idrs")

class edge (object):
    client = None
    className = 'E'
    backend = None


    def __init__(self, fromNode, toNode):
        self.rid = None
        self.fromNode = fromNode.rid
        self.toNode = toNode.rid
        self.name = self.__class__.__name__


    def insertOrient(self, client, batch):
        query = "create edge "+ self.__class__.__name__ +" from " + self.fromNode + " to " + self.toNode + " set name = '" + self.name + "'"
        if batch:
            self.query = query
        else:
            for i in range(10):
                try:
                    edgeInsert = client.command(query)
                    self.rid = edgeInsert[0]._OrientRecord__rid
                    break
                except:
                    pass

    def createOrGetOrient(self, client, batch):
        if batch:
            query = "SELECT FROM " +  self.__class__.__name__ + " where in = " + self.toNode + " and out = " + self.fromNode
        else:
            query = "SELECT FROM " +  self.__class__.__name__ + " where in = " + self.toNode + " and out = " + self.fromNode + " and name = '" + self.name + "'"
        result = client.query(query, -1)
        if (len(result)) == 0:
            self.insertOrient(client, batch)
            if batch:
                logger.info("Batch Request for edge %s.", self.name)
                logger.debug("Return Query (%s).", query)
            else:
                logger.info("Edge did not exist and was inserted: '%s'.", self.rid)
        else:
            self.rid = result[0]._rid
            logger.info("Edge exists: '%s'.", self.rid)

    def insertPsql(self, cur):
        values = ""
        strings = ""
        for elem in self.__slots__:
            strings = strings = strings + elem + ","
            if (self.mapper[elem].lower() == "string"):
                values = values + "'" + getattr(self, elem) + "',"
            elif (self.mapper[elem].lower() == "integer"):
                values = values + str(getattr(self, elem)) + ","
            elif (self.mapper[elem].lower() == "double"):
                values = values + str(getattr(self, elem)) + ","
            elif self.mapper[elem].lower() == "boolean":
                values = values + str(getattr(self, elem)) +  ","
            elif self.mapper[elem].lower() == "real":
                values = values + str(getattr(self, elem)) +  ","
            else:
                raise error.WrongPropertyType(self.mapper[elem] + " not supported or implemented.")

        strings = strings + 'fromnode,' + 'tonode'
        values = values + str(self.fromNode) + "," + str(self.toNode)
        query = "INSERT INTO " + self.__class__.__name__ + " (" + strings + ") VALUES( " + values + ") RETURNING id;"
        cur.execute(query)
        result = cur.fetchone()
        self.rid = result[0]

    def createOrGetPsql(self, cur, batch):
        query = "SELECT id FROM " +  self.__class__.__name__ + " where tonode = " + str(self.toNode) + " and fromnode = " + str(self.fromNode) + ";"
        cur.execute(query)
        result = cur.fetchone()
        if result == None:
            self.insertPsql(cur)
            logger.info("Edge did not exist and was inserted: '%s'.", self.rid)
        else:
            self.rid = result[0]
            logger.info("Edge exists: '%s'.", self.rid)
        

    def createOrGet(self, client, batch):
        if self.backend == 'orient':
            self.createOrGetOrient(client, batch)
        elif self.backend == 'psql':
            self.createOrGetPsql(client, batch)
        else:
            logging.error("Unknown backend %s", self.backend)


class devicerunsonserver(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'device', 'tonode': 'server'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class deviceusestemplate(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'device', 'tonode': 'template'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class userloggedondevice(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'users', 'tonode': 'device'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client        
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class userusesservice(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'users', 'tonode': 'service'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client        
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class serviceusesip(edge):
    cluster_id = None
    mapper = {'name': 'STRING', 'port': 'INTEGER'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'service', 'tonode': 'ip'}
    
    def __init__ (self, fromNode, toNode, port, client=False, batch=False):
        if not client:
            client = self.client        
        edge.__init__(self, fromNode, toNode)
        self.port = port
        self.createOrGet(client, batch)

class servicerunsondevice(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'service', 'tonode': 'device'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class servicedependsonservice(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'service', 'tonode': 'service'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class devicehasinterface(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'device', 'tonode': 'interface'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class interfaceisinnetwork(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'interface', 'tonode': 'l2network'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class ipisinnetwork(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'ip', 'tonode': 'l3network'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class mactointerface(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'mac', 'tonode': 'interface'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class iptomac(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'ip', 'tonode': 'mac'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class attackhasconsequence(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'attack', 'tonode': 'consequence'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class alertcontexthassource(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'alertcontext', 'tonode': 'ip'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class alertcontexthastarget(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'alertcontext', 'tonode': 'ip'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class alertcontexthasservicetarget(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'alertcontext', 'tonode': 'service'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class alertcontexthasusertarget(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'alertcontext', 'tonode': 'users'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class alertcontexthashosttarget(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'alertcontext', 'tonode': 'device'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class alertcontextisoftype(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'alertcontext', 'tonode': 'attack'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class alerttocontext(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'alert', 'tonode': 'alertcontext'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class contexttocontext(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'alertcontext', 'tonode': 'alertcontext'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class userbasedisaresponse(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'userbased', 'tonode': 'response'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class networkbasedisaresponse(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'networkbased', 'tonode': 'response'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class servicebasedisaresponse(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'servicebased', 'tonode': 'response'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class hostbasedisaresponse(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'hostbased', 'tonode': 'response'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class responseconflictswithresponse(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'response', 'tonode': 'response'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class responseispreconditionofresponse(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'response', 'tonode': 'response'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class responsemitigatesconsequence(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'response', 'tonode': 'consequence'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class responsehasimplementation(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'response', 'tonode': 'implementation'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class implementationhasmetric(edge):
    cluster_id = None
    mapper = {'name': 'STRING', '_value' : 'DOUBLE'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'implementation', 'tonode': 'metric'}
    
    def __init__ (self, fromNode, toNode, value=0, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self._value = value
        self.createOrGet(client, batch)

class implementationisdeployedondevice(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'implementation', 'tonode': 'device'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class implementationisexecutedbydevice(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'implementation', 'tonode': 'device'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class bundlesolvesalertcontext(edge):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'bundle', 'tonode': 'alertcontext'}
    
    def __init__ (self, fromNode, toNode, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self.createOrGet(client, batch)

class implementationisinbundle(edge):
    cluster_id = None
    mapper = {'name': 'STRING', '_selected': 'Boolean', '_executed': 'Boolean', '_iteration': 'INTEGER'}
    __slots__ = list(mapper.keys())
    psql = {'fromnode': 'implementation', 'tonode': 'bundle'}
    
    def __init__ (self, fromNode, toNode, selected=False, executed=False, iteration=0, client=False, batch=False):
        if not client:
            client = self.client
        edge.__init__(self, fromNode, toNode)
        self._selected = selected
        self._executed = executed
        self._iteration = iteration
        self.createOrGet(client, batch)

