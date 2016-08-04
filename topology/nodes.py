import logging
import sys
import pyorient
from errors import error

logger = logging.getLogger("idrs")

class node (object):
    client = None
    className = 'V'
    backend = None

    def __init__(self, rid=None):
        self.rid = rid
        self.version = None

    def printInfo(self):
        print ("RID = ", self.rid, " Name = ", self.name)

    def getByRid(self, client, batch):
        if self.backend == 'orient':
            query = "SELECT FROM " + self.__class__.__name__ + " WHERE @RID = " + self.rid
            if batch:
                self.query = query
            result = client.query(query, 1)
            for elem in result:
                for key in elem.oRecordData.keys():
                    value = elem.oRecordData[key]
                    if type(value) != pyorient.otypes.OrientBinaryObject:
                        setattr(self, key, value)
            
        elif self.backend == 'psql':
            query = "SELECT * FROM " + self.__class__.__name__ + " WHERE id = " + str(self.rid)
            client.execute(query)
            result = client.fetchone()
            column_names = [desc[0] for desc in client.description]
            i = 0
            for elem in column_names:
                if elem == 'id':
                    i = i + 1
                    continue
                setattr(self, elem, result[i])
                i = i + 1
        else:
            logging.error("Unknown backend %s", self.backend)
    
    def update(self, client=False, batch=False):
        if not client:
            client = self.client
        #no batch supported, yet
        if batch:
            raise error.NotImplemented(self.mapper[elem] + " batch mode not implemented for update.")
        else:
            name = "@" + self.__class__.__name__
            values = {}
            for elem in self.__slots__:
                values[elem] = getattr(self, elem)
            rec = { name: values }

            update_success = client.record_update(self.rid, self.rid, rec, self.version)
            logger.info("Update Value %s.", self.rid)
            logger.debug("Update Success : %s", update_success)

    def insertOrient(self, client, batch):
        if batch:
            query = "create vertex "+ self.__class__.__name__ + " "
            setPart = " set "
            for elem in self.__slots__:
                if self.mapper[elem].lower() == "string":
                    setPart = setPart + elem + '="' + getattr(self, elem) + '" , '
                elif self.mapper[elem].lower() == "integer":
                    setPart = setPart + elem + '=' + str(getattr(self, elem)) + ' , '
                elif self.mapper[elem].lower() == "date":
                    setPart = setPart + elem + "=DATE('" + str(getattr(self, elem)) + "') , "
                elif self.mapper[elem].lower() == "datetime":
                    setPart = setPart + elem + "=DATE('" + str(getattr(self, elem)) + "') , "
                elif self.mapper[elem].lower() == "boolean":
                    setPart = setPart + elem + "= '" + str(getattr(self, elem)) + "' , "
                else:
                    raise error.WrongPropertyType(self.mapper[elem] + " not supported or implemented.")
            setPart = setPart[:-2]
            query = query + setPart
            self.query = query

        else:
            name = "@" + self.__class__.__name__
            values = {}
            for elem in self.__slots__:
                values[elem] = getattr(self, elem)
            rec = { name: values }

            for i in range(10):
                try:
                    rec_position = client.record_create(self.cluster_id, rec)
                    self.rid = rec_position._rid
                    self.version = rec_position._version
                    break
                except:
                    pass


    def createOrGetOrient(self, client, batch):
        where = "where "
        for elem in self.__slots__:
            if elem.startswith("_"):
                continue
            if self.mapper[elem].lower() == "string":
                where = where + elem + '="' + getattr(self, elem) + '" and '
            elif self.mapper[elem].lower() == "integer":
                where = where + elem + '=' + str(getattr(self, elem)) + ' and '
            elif self.mapper[elem].lower() == "date":
                where = where + elem + "=DATE('" + str(getattr(self, elem)) + "') and "
            elif self.mapper[elem].lower() == "datetime":
                where = where + elem + "=DATE('" + str(getattr(self, elem)) + "') and "
            elif self.mapper[elem].lower() == "boolean":
                where = where + elem + "= '" + str(getattr(self, elem)) + "' and "
            else:
                raise error.WrongPropertyType(self.mapper[elem] + " not supported or implemented.")
        where = where[:-4]
        query = "SELECT FROM " + self.__class__.__name__ + " " + where
        result = client.query(query, -1)
        if (len(result)) == 0:
            self.insertOrient(client, batch)
            if batch:
                logger.info("Batch Request for node %s.", self.name)
                logger.debug("Set Query (%s).", query)
            else:
                logger.info("Node did not exist and was inserted: '%s'.", self.rid)
        else:
            self.rid = result[0]._rid
            self.version = result[0]._version
            logger.info("Node exists: '%s'.", self.rid)


    def insertPsql(self, cur):
        values = ""
        strings = ""
        for elem in self.__slots__:
            if getattr(self, elem) == None:
                continue
            strings = strings = strings + elem + ","
            if (self.mapper[elem].lower() == "string"):
                values = values + "'" + getattr(self, elem) + "',"
            elif (self.mapper[elem].lower() == "integer"):
                values = values + str(getattr(self, elem)) + ","
            elif self.mapper[elem].lower() == "boolean":
                values = values + str(getattr(self, elem)) +  ","
            else:
                raise error.WrongPropertyType(self.mapper[elem] + " not supported or implemented.")

        strings = strings[:-1]
        values = values[:-1]
        query = "INSERT INTO " + self.__class__.__name__ + " (" + strings + ") VALUES( " + values + ") RETURNING id;"
        cur.execute(query)
        result = cur.fetchone()
        self.rid = result[0]

    def createOrGetPsql(self, cur, batch):
        where = "where "
        for elem in self.__slots__:
            if elem.startswith("_"):
                continue
            if self.mapper[elem].lower() == "string":
                where = where + elem + "='" + getattr(self, elem) + "' and "
            elif self.mapper[elem].lower() == "integer":
                where = where + elem + '=' + str(getattr(self, elem)) + ' and '
            #elif self.mapper[elem].lower() == "date":
                #where = where + elem + "=DATE('" + str(getattr(self, elem)) + "') and "
            #elif self.mapper[elem].lower() == "datetime":
                #where = where + elem + "=DATE('" + str(getattr(self, elem)) + "') and "
            elif self.mapper[elem].lower() == "boolean":
                where = where + elem + "= '" + str(getattr(self, elem)) + "' and "
            else:
                raise error.WrongPropertyType(self.mapper[elem] + " not supported or implemented.")
        where = where[:-4]
        query = "SELECT id FROM " + self.__class__.__name__ + " " + where
        cur.execute(query)
        result = cur.fetchone()
        if result ==  None:
            self.insertPsql(cur)
            logger.info("Node did not exist and was inserted: '%s'.", self.rid)
        else:
            self.rid = result[0]
            logger.info("Node exists: '%s'.", self.rid)
            

    def createOrGet(self, client, batch):
        if self.backend == 'orient':
            self.createOrGetOrient(client, batch)
        elif self.backend == 'psql':
            self.createOrGetPsql(client, batch)
        else:
            logging.error("Unknown backend %s", self.backend)

class server(node):
    cluster_id = None
    mapper = {'name': 'STRING', 'uri': 'STRING'}
    __slots__ = list(mapper.keys())
    index = ['name']
    def __init__ (self, name = None, uri = None, rid = None, client=False, batch=False):
        if not client:
            client = self.client
        node.__init__(self, rid)
        self.name = name
        self.uri = uri
        if self.rid == None:
            self.createOrGet(client, batch)
        else:
            self.getByRid(client, batch)

class template(node):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    index = ['name']
    def __init__ (self, name = None, rid = None, client=False, batch=False):
        if not client:
            client = self.client
        node.__init__(self, rid)
        self.name = name
        if self.rid == None:
            self.createOrGet(client, batch)
        else:
            self.getByRid(client, batch)

class l3network(node):
    cluster_id = None
    mapper = {'name': 'STRING', 'prefix': 'INTEGER'}
    __slots__ = list(mapper.keys())
    index = ['name']
    def __init__ (self, name = None, prefix = None, rid = None, client=False, batch=False):
        if not client:
            client = self.client
        node.__init__(self, rid)
        self.name = name
        self.prefix = prefix
        if self.rid == None:
            self.createOrGet(client, batch)
        else:
            self.getByRid(client, batch)

class l2network(node):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    index = ['name']
    def __init__ (self, name = None, rid = None, client=False, batch=False):
        if not client:
            client = self.client
        node.__init__(self, rid)
        self.name = name
        if self.rid == None:
            self.createOrGet(client, batch)
        else:
            self.getByRid(client, batch)

class device(node):
    cluster_id = None
    mapper = {'name': 'STRING', '_cpus': 'INTEGER', '_memoryMax': 'INTEGER', '_memoryMin': 'INTEGER'}
    __slots__ = list(mapper.keys())
    index = ['name']
    def __init__ (self, name = None, rid = None, cpus=2, memoryMax=1024, memoryMin=512, client=False, batch=False):
        if not client:
            client = self.client
        node.__init__(self, rid)
        self.name = name  
        self._cpus = cpus
        self._memoryMax = memoryMax
        self._memoryMin = memoryMin
        if self.rid == None:
            self.createOrGet(client, batch)
        else:
            self.getByRid(client, batch)

class users(node):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    index = ['name']
    def __init__ (self, name = None, rid = None, client=False, batch=False):
        if not client:
            client = self.client
        node.__init__(self, rid)
        self.name = name
        if self.rid == None:
            self.createOrGet(client, batch)
        else:
            self.getByRid(client, batch)

class service(node):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    index = ['name']
    def __init__ (self, name = None, rid = None, client=False, batch=False):
        if not client:
            client = self.client
        node.__init__(self, rid)
        self.name = name
        if self.rid == None:
            self.createOrGet(client, batch)
        else:
            self.getByRid(client, batch)


class interface(node):
    cluster_id = None
    mapper = {'name': 'STRING', 'orderNum': 'INTEGER', '_rate': 'INTEGER'}
    __slots__ = list(mapper.keys())
    index = ['name']
    def __init__ (self, name = None, orderNum = None, rid = None, rate=500, client=False, batch=False):
        if not client:
            client = self.client
        node.__init__(self, rid)
        self.name = name
        self.orderNum = orderNum
        self._rate = rate
        if self.rid == None:
            self.createOrGet(client, batch)
        else:
            self.getByRid(client, batch)

class ip(node):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    index = ['name']
    def __init__ (self, name = None, rid = None, client=False, batch=False):
        if not client:
            client = self.client
        node.__init__(self, rid)
        self.name = name
        if self.rid == None:
            self.createOrGet(client, batch)
        else:
            self.getByRid(client, batch)

class mac(node):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    index = ['name']
    def __init__ (self, name = None, rid = None, client=False, batch=False):
        if not client:
            client = self.client
        node.__init__(self, rid)
        self.name = name
        if self.rid == None:
            self.createOrGet(client, batch)
        else:
            self.getByRid(client, batch)


class attack(node):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    index = ['name']
    def __init__ (self, name = None, rid = None, client=False, batch=False):
        if not client:
            client = self.client
        node.__init__(self, rid)
        self.name = name
        if self.rid == None:
            self.createOrGet(client, batch)
        else:
            self.getByRid(client, batch)

class consequence(node):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    index = ['name']
    def __init__ (self, name = None, rid = None, client=False, batch=False):
        if not client:
            client = self.client
        node.__init__(self, rid)
        self.name = name
        if self.rid == None:
            self.createOrGet(client, batch)
        else:
            self.getByRid(client, batch)

class alertcontext(node):
    cluster_id = None
    mapper = {'name': 'STRING', '_solved': 'BOOLEAN', '_prio' : 'INTEGER'}
    __slots__ = list(mapper.keys())
    def __init__ (self, name = None, rid = None, solved=False, prio=None, client=False, batch=False):
        if not client:
            client = self.client
        node.__init__(self, rid)
        self.name = name
        self._solved = solved
        self._prio = prio
        if self.rid == None:
            self.createOrGet(client, batch)
        else:
            self.getByRid(client, batch)

class alert(node):
    cluster_id = None
    mapper = {'name': 'STRING', 'detectiontime': 'DATETIME', '_prio' : 'INTEGER'}
    __slots__ = list(mapper.keys())
    def __init__ (self, ident = None, ts = None, rid = None, prio=None, client=False, batch=False):
        if not client:
            client = self.client
        node.__init__(self, rid)
        self.name = str(ident)
        self.detectiontime = ts
        self._prio = prio
        if self.rid == None:
            self.createOrGet(client, batch)
        else:
            self.getByRid(client, batch)

class response(node):
    cluster_id = None
    mapper = {'name': 'STRING', 'active': 'Boolean'}
    __slots__ = list(mapper.keys())
    def __init__ (self, name = None, active = None, rid = None, client=False, batch=False):
        if not client:
            client = self.client
        node.__init__(self, rid)
        self.name = name
        self.active = active
        if self.rid == None:
            self.createOrGet(client, batch)
        else:
            self.getByRid(client, batch)

class userbased(node):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    isa = ['response']
    def __init__ (self, name = None, rid = None, client=False, batch=False):
        if not client:
            client = self.client
        node.__init__(self, rid)
        self.name = name
        if self.rid == None:
            self.createOrGet(client, batch)
        else:
            self.getByRid(client, batch)

class networkbased(node):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    isa = ['response']
    def __init__ (self, name = None, rid = None, client=False, batch=False):
        if not client:
            client = self.client
        node.__init__(self, rid)
        self.name = name
        if self.rid == None:
            self.createOrGet(client, batch)
        else:
            self.getByRid(client, batch)

class servicebased(node):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    isa = ['response']
    def __init__ (self, name = None, rid = None, client=False, batch=False):
        if not client:
            client = self.client
        node.__init__(self, rid)
        self.name = name
        if self.rid == None:
            self.createOrGet(client, batch)
        else:
            self.getByRid(client, batch)

class hostbased(node):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    isa = ['response']
    def __init__ (self, name = None, rid = None, client=False, batch=False):
        if not client:
            client = self.client
        node.__init__(self, rid)
        self.name = name
        if self.rid == None:
            self.createOrGet(client, batch)
        else:
            self.getByRid(client, batch)

class implementation(node):
    cluster_id = None
    mapper = {'name': 'STRING'}
    __slots__ = list(mapper.keys())
    def __init__ (self, name = None, rid = None, client=False, batch=False):
        if not client:
            client = self.client
        node.__init__(self, rid)
        self.name = name
        if self.rid == None:
            self.createOrGet(client, batch)
        else:
            self.getByRid(client, batch)

class metric(node):
    cluster_id = None
    mapper = {'name': 'STRING', 'value': 'INTEGER'}
    __slots__ = list(mapper.keys())
    def __init__ (self, name = None, value = None, rid = None, client=False, batch=False):
        if not client:
            client = self.client
        node.__init__(self, rid)
        self.name = name
        if self.rid == None:
            self.createOrGet(client, batch)
        else:
            self.getByRid(client, batch)

class bundle(node):
    cluster_id = None
    mapper = {'name': 'STRING', '_executing': 'Boolean', '_active': 'Boolean'}
    __slots__ = list(mapper.keys())
    def __init__ (self, name = None, rid = None, executing=False, active=True, client=False, batch=False):
        if not client:
            client = self.client
        node.__init__(self, rid)
        self.name = name
        self._executing = _executing
        self._active = active
        if self.rid == None:
            self.createOrGet(client, batch)
        else:
            self.getByRid(client, batch)

