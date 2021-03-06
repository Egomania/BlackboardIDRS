import pyorient
import logging
import sys, inspect
import psycopg2
import json

from topology import nodes as nodes
from topology import edges as edges

from helper_functions import helper

logger = logging.getLogger('idrs')
triggers = ['alert', 'alertcontext', 'bundle', 'implementationisinbundle']

def setPropertiesPsql(newClass, setProperties, toUseProperties, cur, propertyMap):
    logger.info("Check for needed Properties in class '%s'.", newClass)
    delete = list(set(setProperties) - set(toUseProperties))
    insert = list(set(toUseProperties) - set(setProperties))
    exists = list(set(toUseProperties) - set(insert))
    logger.info("The following properties already exist: '%s'.", exists)
    logger.info("Delete the following properties: '%s'.", delete)
    logger.info("Insert the following properties: '%s'.", insert)
    for elem in delete:
        cur.execute("ALTER TABLE " + newClass + " DROP COLUMN " + elem + ";")
    for elem in insert:
        cur.execute("ALTER TABLE " + newClass + " ADD COLUMN " + elem + " " + propertyMap[newClass][elem] + ";")

def triggerMan(table, cur):
    cur.execute("SELECT trigger_name from information_schema.triggers WHERE event_object_table = '" + table.lower() + "';")
    result = cur.fetchall()
    for elem in result:
        cur.execute("DROP TRIGGER " + elem[0] + " ON " + table.lower())

    if table.lower() in triggers:
        logger.info("Create triggers for %s", table)
        cur.execute("CREATE TRIGGER " + table.lower() + "_notify_update AFTER UPDATE ON " + table.lower() + " FOR EACH ROW EXECUTE PROCEDURE table_update_notify();")
        cur.execute("CREATE TRIGGER " + table.lower() + "_notify_insert AFTER INSERT ON " + table.lower() + " FOR EACH ROW EXECUTE PROCEDURE table_update_notify();")
        cur.execute("CREATE TRIGGER " + table.lower() + "_notify_delete AFTER DELETE ON " + table.lower() + " FOR EACH ROW EXECUTE PROCEDURE table_update_notify();")

def delIndexPsql(cur, ownTables):
    statement = "select indexname from pg_indexes where tablename in %s"
    statement = cur.mogrify(statement, (tuple(ownTables), ))
        
    cur.execute(statement)
    result = cur.fetchall()
    
    for elem in result:
        indexName = str(elem[0])
        if "_postgresindex_" in indexName:
            cur.execute("DROP INDEX IF EXISTS " + indexName)


def setIndexPsql(indices, cur):

    for index in indices.keys():
        for elem in indices[index]:
            indexName = index + "_postgresindex_" + elem
            logger.info("Create index named %s in table %s on column %s", indexName, index, elem)
            try:
                query = "CREATE UNIQUE INDEX " + indexName + " ON " + index + "(" + elem + ")" 
                cur.execute(query)
            except Exception as inst:
                logger.debug(type(inst))
                logger.debug(inst.args)
                logger.debug(inst)
                logger.info("Index named %s in table %s on column %s Already Exists.", indexName, index, elem)
            

def setConstraints(constraints, cur):

    query = "select table_name, constraint_name from information_schema.table_constraints WHERE CONSTRAINT_TYPE = 'FOREIGN KEY';"
    cur.execute(query)
    result = cur.fetchall()
    for elem in result:
        cur.execute("ALTER TABLE " + elem[0] + " DROP CONSTRAINT IF EXISTS " + elem[1] + ";")
    
    for table in constraints.keys():
        fromnode = constraints[table]['fromnode']
        tonode = constraints[table]['tonode']

        query = "ALTER TABLE " + table + " ADD FOREIGN KEY(fromnode) REFERENCES " + fromnode + "(id) ON UPDATE CASCADE ON DELETE CASCADE; "
        
        cur.execute(query)
        query = "ALTER TABLE " + table + " ADD FOREIGN KEY(tonode) REFERENCES " + tonode + "(id) ON UPDATE CASCADE ON DELETE CASCADE; "
        
        cur.execute(query)

def delElems(conn, client, dbs, values, allElems):

    if type(values) is list:    
        listToDelete = values
    else:
        if values == "policy":
            logger.info("Delete Policy")
            listToDelete = ["response", "attack", "consequence", "userbased", "networkbased", "hostbased", "servicebased", "implementation", "metric"]
        elif values == "infrastructure":
            logger.info("Delete Infrastructure")
            listToDelete = ["l3network","l2network", "device", "service", "interface", "ip", "mac", "template", "server", "users"]
        else:
            listToDelete = []

    if dbs.backend == 'orient':
        for elem in listToDelete:
            for i in range(10):
                try:
                    client.command("delete vertex " + elem)
                    client.command("truncate class " + elem)
                except Exception as inst:
                    print (inst)

    elif dbs.backend == 'psql':
        for elem in listToDelete:
            for i in range(10):
                try:
                    client.execute("TRUNCATE TABLE " + elem + " RESTART IDENTITY CASCADE;")
                    conn.commit()
                except Exception as inst:
                    logger.error("Database locked in RUN %s : %s", i, inst)
    else:
        logger.error("Wrong Database Backend %s", dbs.backend)


def createPsql(dbs, conn, cur):

    ret = []
    classes = []
    propertyMap = {}

    if conn == None:
        logging.warning("Database %s does not exist.", dbs.database)
        conn = psycopg2.connect(database='postgres', user=dbs.user, password=dbs.pwd, port=dbs.port, host=dbs.server)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute('CREATE DATABASE ' + dbs.database)
        conn.commit()
        cur.close()
        conn.close()
        conn = psycopg2.connect(database=dbs.database, user=dbs.user, password=dbs.pwd, port=dbs.port, host=dbs.server)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

    query = (
        "CREATE OR REPLACE FUNCTION table_update_notify() RETURNS trigger AS $$"
        "DECLARE"
        "  id int;"
        "  newData json;"
        "  oldData json;"
        "BEGIN"
        "  IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN "
        "    id = NEW.id; "
        "      IF TG_OP = 'INSERT' THEN "
        "        oldData = json_object('{}');"
        "      ELSE  "
        "        oldData = row_to_json(OLD);"
        "      END IF;"
        "    newData = row_to_json(NEW);"
        "  ELSE "
        "    id = OLD.id; "
        "    oldData = row_to_json(OLD);"
        "    newData = json_object('{}');"
        "  END IF; "
        "  PERFORM pg_notify('table_update', json_build_object('table', TG_TABLE_NAME, 'id', id, 'type', TG_OP, 'original', oldData, 'new', newData )::text); "
        "  RETURN NEW; "
        "END; "
        "$$ LANGUAGE plpgsql;"
    )

    cur.execute(query)

    cur.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'""")
    for table in cur.fetchall():
        classes.append(table[0])
    logger.info("Found following tables: '%s' .", classes)

    eval("nodes.node").client = cur
    eval("edges.edge").client = cur

    classesToExclude = ['node', 'edge']
    classesToImport = ['nodes', 'edges']
    #classesToImport = []

    constraints = {}
    index = {}

    for classToImport in classesToImport:
    
        clsmembers = inspect.getmembers(eval(classToImport), inspect.isclass)
        for elem in clsmembers:
            newClass = elem[0]
            if newClass in classesToExclude:
                continue
            ret.append(newClass.lower())

            propertiesTMP = eval(classToImport+"."+newClass).mapper
            properties = {}
            properties["id"] = 'INTEGER'
            
            for key in propertiesTMP.keys():
                prop = propertiesTMP[key]
                if prop == 'STRING':
                    properties[key.lower()] = 'TEXT'
                elif prop == 'DOUBLE':
                    properties[key.lower()] = 'REAL'
                elif prop == 'DATETIME':
                    properties[key.lower()+"_date"] = 'DATE'
                    properties[key.lower()+"_time"] = 'TIME WITH TIME ZONE'
                    
                else:
                    properties[key.lower()] = prop

            className = eval(classToImport+"."+newClass).className
            if className == 'E':
                properties['fromnode'] = 'INTEGER'
                properties['tonode'] = 'INTEGER'
                constraints[newClass] = eval(classToImport+"."+newClass).psql
            
            try:
                index[newClass] = eval(classToImport+"."+newClass).index
            except:
                pass

            propertyMap[newClass] = properties
            if newClass.lower() not in classes:
                logger.info("created table '%s'.", newClass)
                cur.execute("create table " + newClass + "(id SERIAL PRIMARY KEY NOT null);")
            else:
                logger.info("Table '%s' already exists.", newClass)
            
            cur.execute("select column_name from INFORMATION_SCHEMA.COLUMNS where table_name = '" + newClass.lower() + "';")
            attributes = cur.fetchall()
            propertiesACT = []
            for attribute in attributes:
                propertiesACT.append(attribute[0])
            setPropertiesPsql(newClass, propertiesACT, list(propertyMap[newClass].keys()), cur, propertyMap)
            triggerMan(newClass, cur)

    setConstraints(constraints ,cur)
    if dbs.index:
        setIndexPsql(index, cur)
    else:
        delIndexPsql(cur, ret)

    for table in classes:
        if table not in ret:
            cur.execute("drop table " + table + " CASCADE;")
            logger.info("Dropped Table %s", table)

    if dbs.deletePolicy:
        delElems(conn, cur, dbs, "policy", ret)

    if dbs.deleteInfrastructure:
        delElems(conn, cur, dbs, "infrastructure", ret)

    if len(dbs.policy) > 0 or len(dbs.infrastructure) > 0:
        readPolicy(dbs, conn, cur)

    if dbs.EXAMPLE_DATA:
        createExamples()

    return ret

def setProperties(newClass, setProperties, toUseProperties, client, propertyMap):
    logger.info("Check for needed Properties in class '%s'.", newClass)
    delete = list(set(setProperties) - set(toUseProperties))
    insert = list(set(toUseProperties) - set(setProperties))
    exists = list(set(toUseProperties) - set(insert))
    logger.info("The following properties already exist: '%s'.", exists)
    logger.info("Delete the following properties: '%s'.", delete)
    logger.info("Insert the following properties: '%s'.", insert)
    for elem in delete:
        client.command("DROP PROPERTY " + newClass + "." + elem)
    for elem in insert:
        client.command("CREATE PROPERTY " + newClass + "." + elem + " " + propertyMap[newClass][elem])

def delIndexOrient(client):
    query = "select name from (select expand(indexes) from metadata:indexmanager)"

    result = client.query(query,-1)

    for elem in result:
        if "_" in elem.name:
            client.command("DROP INDEX " + elem.name)

def setIndexOrient(indices, client):

    for index in indices.keys():
        for elem in indices[index]:
            indexName = index + "_" + elem
            logger.info("Create index named %s in table %s on column %s", indexName, index, elem)
            try:
                query = "CREATE INDEX " + indexName + " ON " + index + " (" + elem + ") unique" 
                client.command(query)
            except:
                logger.info("Index named %s in table %s on column %s Already Exists.", indexName, index, elem)

def createOrient(dbs, client, session_id):

    database = dbs.database 
    server = dbs.server
    port = dbs.port
    user = dbs.user
    pwd = dbs.pwd

    ret = []
    classes = []
    propertyMap = {}
    clusters = {}

    # check whether or not an appropriate database exists
    if client.db_exists(database, pyorient.STORAGE_TYPE_PLOCAL):
        logger.info("database '%s' already exists.", database)
    else:
        client.db_create(database,pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_PLOCAL)
        logger.info("created database '%s'.", database)

    client.db_open(database, user, pwd)

    res = client.query('select name, defaultClusterId from (select expand(classes) from 0:1)',-1)
    for elem in res:
        clusters[elem.oRecordData['name']] = elem.oRecordData['defaultClusterId']

    eval("nodes.node").client = client
    eval("edges.edge").client = client

    result = client.query('SELECT name FROM ( SELECT expand( classes ) FROM metadata:schema )', -1)
    for elem in result:
        classes.append(elem.name)

    logger.info("Found following classes: '%s' .", classes)

    classesToExclude = ['node', 'edge']
    classesToImport = ['nodes', 'edges']

    index = {}

    for classToImport in classesToImport:
    
        clsmembers = inspect.getmembers(eval(classToImport), inspect.isclass)
        for elem in clsmembers:
            newClass = elem[0]
            if newClass in classesToExclude:
                continue
            ret.append(newClass)
            cluster_id = None
            properties = eval(classToImport+"."+newClass).mapper
            propertyMap[newClass] = properties
            if newClass not in classes:
                logger.info("created class '%s'.", newClass)
                query = "create class " + newClass + " extends " + eval(classToImport+"."+newClass).className
                cluster_id = client.command(query)[0]
            else:
                logger.info("Class '%s' already exists.", newClass)
                cluster_id = clusters[newClass]

            result = client.query('select name from (select expand(properties) from (select expand(classes) from metadata:schema) where name="' + newClass + '")')
            propertiesACT = []
            for elem in result:
                propertiesACT.append(elem.name)
            setProperties(newClass, propertiesACT, list(propertyMap[newClass].keys()), client, propertyMap)
            eval(classToImport+"."+newClass).cluster_id = cluster_id

            try:
                index[newClass] = eval(classToImport+"."+newClass).index
            except:
                pass

    if dbs.index:
        setIndexOrient(index, client)
    else:
        delIndexOrient(client)

    if dbs.deletePolicy:
        delElems(session_id, client, dbs, "policy", ret)

    if dbs.deleteInfrastructure:
        delElems(session_id, client, dbs, "infrastructure", ret)

    if len(dbs.policy) > 0 or len(dbs.infrastructure) > 0:
        readPolicy(dbs, session_id, client)

    if dbs.EXAMPLE_DATA:
        createExamples()

    return ret

def createExamples():
    helper.exampleData()

def getAllNodes(dbs, dbConnect, tableName):

    elems = []

    if dbs.backend == 'orient':
            query = "SELECT * FROM " + tableName
            result = dbConnect.query(query, -1)
            for elem in result:
                node = getattr(nodes, tableName)(rid=elem._rid)
                elems.append(node)
            
    elif dbs.backend == 'psql':
            query = "SELECT id FROM " + tableName
            dbConnect.execute(query)
            result = dbConnect.fetchall()
            for elem in result:
                node = getattr(nodes, tableName)(rid=elem[0])
                elems.append(node)
    else:
        logging.error("Unknown backend %s", self.backend)

    return elems

def readPolicy(dbs, connection, insert):

    if len(dbs.policy) > 0:
        json_dataPol=open(dbs.policy).read()
        dataPol = json.loads(json_dataPol)
        attacks = dataPol['attacks']
        responses = dataPol['responses']
        consequences = dataPol['consequences']
        
    else:
        attacks = []
        responses = []
        consequences = []
        

    if len(dbs.infrastructure) > 0:
        json_dataInf=open(dbs.infrastructure).read()
        dataInf = json.loads(json_dataInf)
        devices = dataInf['devices']
        services = dataInf['services']
        networks = dataInf['networks']
        templates = dataInf['templates']
        users = dataInf['users']
      
    else:
        devices = []
        services = []
        networks = []

    templateNodes = getAllNodes(dbs, insert, 'template')
    for template in templates:
        name = template['template']['name']
        templateNode = nodes.template(name)
        templateNodes.append(templateNode)

    networkNodes = getAllNodes(dbs, insert, 'l3network')
    for network in networks:
        name = network['network']['name']
        prefix = int(network['network']['prefix'])
        networkNode = nodes.l3network(name, prefix)
        networkNodes.append(networkNode)

    consequenceNodes = getAllNodes(dbs, insert, 'consequence')
    for consequence in consequences:
        name = consequence['consequence']['name']
        consequenceNode = nodes.consequence(name)
        consequenceNodes.append(consequenceNode)

    attackNodes = getAllNodes(dbs, insert, 'attack')
    for attack in attacks:
        name = attack['attack']['name']
        attackNode = nodes.attack(name)
        attackNodes.append(attackNode)
        AhasC = attack['attack']['attackhasconsequences']
        for elem in AhasC:
            toNode = helper.getElem(consequenceNodes, 'name', elem)
            if toNode != None:
                edges.attackhasconsequence(attackNode, toNode)

    serviceNodes = getAllNodes(dbs, insert, 'service')
    for service in services:
        name = service['service']['name']
        port = int(service['service']['port'])
        serviceNode = nodes.service(name)
        serviceNode.port = port
        try:
            serviceNode.depends = service['service']['servicedependsonservice']
        except:
            serviceNode.depends = []
        serviceNodes.append(serviceNode)

    for fromNode in serviceNodes:
        try:
            for elem in fromNode.depends:
                toNode = helper.getElem(serviceNodes, 'name', elem)
                if toNode != None:
                    SdependsonS = edges.servicedependsonservice(fromNode, toNode)
        except:
            continue

    deviceNodes = getAllNodes(dbs, insert, 'device')
    ipNodes = getAllNodes(dbs, insert, 'ip')
    for device in devices:
        servicesOnDevice = {}
        name = device['device']['name']
        deviceNode = nodes.device(name)
        toNode = helper.getElem(templateNodes, 'name', device['device']['template'])
        deviceusestemplate = edges.deviceusestemplate(deviceNode, toNode)
        deviceNodes.append(deviceNode)
        for elem in device['device']['interfaces']:
            iface = elem['interface']
            ip = iface['ip']
            mac = iface['mac']
            
            l3Node = helper.getElem(networkNodes, 'name', iface['l3'])
            try:
                l2 = iface['l2']
            except:
                l2 = name + "_eth" + str(order) 
            order = int(iface['order'])
            ipNode = nodes.ip(ip)
            ipNodes.append(ipNode)
            macNode = nodes.mac(mac)
            ifaceNode = nodes.interface("eth"+str(order)+"_"+name, order)
            l2Node = nodes.l2network(l2)

            devicehasinterface = edges.devicehasinterface(deviceNode, ifaceNode)
            interfaceisinnetwork = edges.interfaceisinnetwork(ifaceNode, l2Node)
            mactointerface = edges.mactointerface(macNode, ifaceNode)
            iptomac = edges.iptomac(ipNode, macNode)
            ipisinnetwork = edges.ipisinnetwork(ipNode, l3Node)

            try:
                servicesOnIP = iface['service']
            except:
                servicesOnIP = []

            for serviceOnIP in servicesOnIP:
                serviceOnIPNode = helper.getElem(serviceNodes, 'name', serviceOnIP)
                servicesOnDevice[serviceOnIP] = serviceOnIPNode
                try:
                    serviceusesip = edges.serviceusesip(serviceOnIPNode, ipNode, serviceOnIPNode.port)
                except:
                    pass

        for servOnDev in servicesOnDevice.keys():
            servicerunsondevice = edges.servicerunsondevice(servicesOnDevice[servOnDev], deviceNode)

    userNodes = getAllNodes(dbs, insert, 'users')
    for user in users:
        name = user['user']['name']
        userNode = nodes.users(name)
        userNodes.append(userNode)
        loggedOn = user['user']['loggedOn']
        hostNode = helper.getElem(deviceNodes, 'name', loggedOn)
        userisloggedondevice = edges.userloggedondevice(userNode, hostNode)
        usesService = user['user']['uses']
        for elem in usesService:
            servNode = helper.getElem(serviceNodes, 'name', elem)
            userusesservice = edges.userusesservice(userNode, servNode)

    responseNodes = getAllNodes(dbs, insert, 'response')
    for response in responses:
        resp = response['response']
        name = resp['name']
        targets = resp['target']
        if len(targets) == 0:
            active = False
        else:
            active = True
        responseNode = nodes.response(name, active)
        responseNodes.append(responseNode)
        try:
            mitigates = resp['responsemitigatesconsequences']
        except:
            mitigates = []

        for elem in mitigates:
            conseqNode = helper.getElem(consequenceNodes, 'name', elem)
            responsemitigatesconsequence = edges.responsemitigatesconsequence(responseNode, conseqNode)

        for elem in targets:
            specificResp = elem+"based"
            specificRespClass = getattr(nodes, specificResp)
            specificRespNode = specificRespClass(name)
            specificRespRel = specificResp + "isaresponse"
            specificRespRelClass = getattr(edges, specificRespRel)
            specificRespRelNode = specificRespRelClass(specificRespNode, responseNode)

        for elem in resp['implementations']:
            impl = elem['implementation']
            implNode = nodes.implementation(impl['name'])
            responsehasimplementation = edges.responsehasimplementation(responseNode, implNode)
            execNode = helper.getElem(deviceNodes, 'name', impl['executor'])
            implementationisexecutedbydevice = edges.implementationisexecutedbydevice(implNode, execNode)
            for entry in impl['deployedOn']:
                deployedOn = helper.getElem(deviceNodes, 'name', entry)
                implementationisdeployedondevice = edges.implementationisdeployedondevice(implNode, deployedOn)
            for entry in impl['metrics'].keys():
                metricNode = nodes.metric(entry)
                implhasMetric = edges.implementationhasmetric(implNode, metricNode, value=impl['metrics'][entry])
            
    for response in responses:
        c1 = helper.getElem(responseNodes, 'name', response['response']['name'])
        for entry in response['response']['conflicts']:
            c2 = helper.getElem(responseNodes, 'name', entry)
            conflictrel = edges.responseconflictswithresponse(c1, c2)
        for entry in response['response']['preconditions']:
            pre = helper.getElem(responseNodes, 'name', entry)
            preconditionRel = edges.responseispreconditionofresponse(pre, c1)

