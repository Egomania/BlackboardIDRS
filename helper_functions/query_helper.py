import logging
import psycopg2
import psycopg2.extensions
import pyorient

from classes import alertProcessing as AP

logger = logging.getLogger("idrs")

def geteffectedEntitiesOrient(insert, issue):

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

def getImplementationsOrient(insert, effectedEntities):

    impls = []

    # todo : orient query

    return impls     

def getImplementationsAttackOrient(insert, issue):  

    impls = []

    # todo : orient query

    return impls   

def geteffectedEntitiesPsql(insert, issue):

    effectedEntities = {}

    query = "WITH RECURSIVE contextTree (fromnode, level, tonode) AS ( SELECT id, 0, id FROM alertcontext WHERE id = %s UNION ALL SELECT cTree.tonode, cTree.level + 1, context.fromnode FROM contexttocontext context, contextTree cTree WHERE context.tonode = cTree.fromnode) select distinct d.id from service d, alertcontexthasservicetarget aht where d.id = aht.tonode and (aht.fromnode = %s or aht.fromnode in (SELECT distinct tonode FROM contextTree WHERE level > 0));"
    query = insert.mogrify(query, (issue, issue, ))
    insert.execute(query)
    result = insert.fetchall()
    listing = []
    for elem in result:
        listing.append(elem[0])

    effectedEntities['service'] = listing

    query = "WITH RECURSIVE contextTree (fromnode, level, tonode) AS ( SELECT id, 0, id FROM alertcontext WHERE id = %s UNION ALL SELECT cTree.tonode, cTree.level + 1, context.fromnode FROM contexttocontext context, contextTree cTree WHERE context.tonode = cTree.fromnode) select distinct d.id from device d, alertcontexthashosttarget aht where d.id = aht.tonode and (aht.fromnode = %s or aht.fromnode in (SELECT distinct tonode FROM contextTree WHERE level > 0));"
    query = insert.mogrify(query, (issue, issue, ))
    insert.execute(query)
    result = insert.fetchall()
    listing = []
    for elem in result:
        listing.append(elem[0])

    effectedEntities['host'] = listing

    query = "WITH RECURSIVE contextTree (fromnode, level, tonode) AS ( SELECT id, 0, id FROM alertcontext WHERE id = %s UNION ALL SELECT cTree.tonode, cTree.level + 1, context.fromnode FROM contexttocontext context, contextTree cTree WHERE context.tonode = cTree.fromnode) select distinct d.id from users d, alertcontexthasusertarget aht where d.id = aht.tonode and (aht.fromnode = %s or aht.fromnode in (SELECT distinct tonode FROM contextTree WHERE level > 0));"
    query = insert.mogrify(query, (issue, issue, ))
    insert.execute(query)
    result = insert.fetchall()
    listing = []
    for elem in result:
        listing.append(elem[0])

    effectedEntities['user'] = listing

    query = "WITH RECURSIVE contextTree (fromnode, level, tonode) AS ( SELECT id, 0, id FROM alertcontext WHERE id = %s UNION ALL SELECT cTree.tonode, cTree.level + 1, context.fromnode FROM contexttocontext context, contextTree cTree WHERE context.tonode = cTree.fromnode) select distinct dhasiface.fromnode from alertcontexthastarget aht, devicehasinterface dhasiface, mactointerface mactoiface, iptomac where iptomac.fromnode = aht.tonode and iptomac.tonode = mactoiface.fromnode and mactoiface.tonode = dhasiface.tonode and (aht.fromnode = %s or aht.fromnode in (SELECT distinct tonode FROM contextTree WHERE level > 0));"
    query = insert.mogrify(query, (issue, issue, ))
    insert.execute(query)
    result = insert.fetchall()
    listing = []
    for elem in result:
        listing.append(elem[0])

    effectedEntities['passive'] = listing

    return effectedEntities

def getImplementationsPsql(insert, effectedEntities):

    impls = []
    implsMapper = {}

    # get implementations for hostbased deployed on device
    if len(effectedEntities['host']) > 0:
        query = "select distinct rel.fromnode from implementationisdeployedondevice rel where rel.tonode in %s and rel.fromnode in (select has.tonode from hostbasedisaresponse isa, responsehasimplementation has where isa.tonode = has.fromnode);"
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

    # todo : network based

    # get applicable passive actions

    if len(effectedEntities['passive']) > 0:
        query = "select distinct rel.fromnode from implementationisdeployedondevice rel where rel.tonode in %s and rel.fromnode in (select rhi.tonode  from responsehasimplementation rhi, response r where rhi.fromnode = r.id and r.active = %s);"
        query = insert.mogrify(query, (tuple(effectedEntities['passive']), False, ))
        insert.execute(query)
        result = insert.fetchall()
        for elem in result:
            impls.append(elem[0])

    return impls      


def getImplementationsAttackPsql(insert, issue):  

    impls = []

    query = "WITH RECURSIVE contextTree (fromnode, level, tonode) AS ( SELECT id, 0, id FROM alertcontext WHERE id = %s UNION ALL SELECT cTree.tonode, cTree.level + 1, context.fromnode FROM contexttocontext context, contextTree cTree WHERE context.tonode = cTree.fromnode) select distinct i.tonode from responsehasimplementation i, responsemitigatesconsequence r, attackhasconsequence a, alertcontextisoftype aht where i.fromnode = r.fromnode and r.tonode = a.tonode and a.fromnode = aht.tonode and (aht.fromnode = %s or aht.fromnode in (SELECT distinct tonode FROM contextTree WHERE level > 0));"
    query = insert.mogrify(query, (issue, issue, ))
    insert.execute(query)
    result = insert.fetchall()
    for elem in result:
        impls.append(elem[0])

    return impls

def getMaxIterationOrient(insert, bundleID):
    
    # todo : OrientQuery

    return 0

def getMaxIterationPsql(insert, bundleID):
    query = "select max(_iteration) from implementationisinbundle WHERE tonode = %s;"
    query = insert.mogrify(query, (bundleID, ))
    insert.execute(query)
    iteration = insert.fetchone()[0]
    return iteration

def updateEdgeOrient(connector, insert, edgeName, fromNode, toNode, updateValues, commit):
    pass

def updateNodeOrient(connector, insert, tableName, ident, updateValues, commit):
    pass

def updateEdgePsql(connector, insert, edgeName, fromNode, toNode, updateValues, commit):
    for elem in updateValues.keys():
        query = "update " + edgeName + " set " + elem + " = %s where fromnode = %s and tonode = %s;"
        query = insert.mogrify(query, (updateValues[elem], fromNode, toNode, ))
        insert.execute(query)
    if commit:
        connector.commit()

def updateNodePsql(connector, insert, tableName, ident, updateValues, commit):
    for elem in updateValues.keys():
        query = "update " + tableName + " set " + elem + " = %s where id = %s;"
        query = insert.mogrify(query, (updateValues[elem], ident, ))
        insert.execute(query)
    if commit:
        connector.commit()

def selectSingleValuePsql(insert, table, name, matchValue=None, matchCondition=None, fetchall=True):

    if matchValue == None or matchCondition == None:
        query = "select " + name + " from " + table + ";"
        query = insert.mogrify(query, ( ))
    else:
        query = "select " + name + " from " + table + " WHERE " + matchValue + " = %s;"
        query = insert.mogrify(query, (matchCondition, ))
    insert.execute(query)    
    if fetchall:
        result = insert.fetchall()
    else:
        result = insert.fetchone()[0]

    return result

def selectSingleValueOrient(insert, table, name, matchValue, matchCondition):
    # todo : Orient
    return []

def selectSingleEdgeValuePsql(insert, table, name, fromNode, toNode, fetchall = True):

    query = "select " + name + " from " + table + " where fromnode = %s and tonode = %s;"
    query = insert.mogrify(query, (fromNode, toNode, ))
    insert.execute(query)    
    if fetchall:
        result = insert.fetchall()
    else:
        result = insert.fetchone()[0]

    return result

def selectSingleEdgeValueOrient(insert, table, name, fromNode, toNode, fetchall = True):
    # todo : Orient
    return []

def getNotYetSelectedImplementationsPsql(insert, bundleID):

    query = "select fromnode from implementationisinbundle WHERE tonode = %s and _selected = %s;"
    query = insert.mogrify(query, (bundleID, False, ))
    insert.execute(query)
    result = insert.fetchall()

    return result

def getNotYetSelectedImplementationsOrient(insert, bundleID):
    # todo : Orient
    return []

def getLastSelectedImplementationsWithExecutorPsql(insert, bundleID):

    query = "select i.name, d.name, iib.fromnode, ied.tonode from implementationisinbundle iib, implementationisexecutedbydevice ied, implementation i, device d WHERE iib.tonode = %s and iib._selected = %s and iib._iteration = (select max(_iteration) from implementationisinbundle where tonode = %s) and iib.fromnode = ied.fromnode and iib.fromnode = i.id and ied.tonode = d.id;"
    query = insert.mogrify(query, (bundleID, True, bundleID, ))
    insert.execute(query)
    result = insert.fetchall()

    return result

def getLastSelectedImplementationsWithExecutorOrient(insert, bundleID):
    # todo : Orient
    return []

def getImplementationInformationPsql(insert, impl):

    query = "select idd.tonode, ied.tonode, ihm.fromnode, m.name, ihm._value from implementationhasmetric ihm, metric m, implementationisexecutedbydevice ied, implementationisdeployedondevice idd where idd.fromnode = %s and ihm.fromnode = %s and m.id = ihm.tonode and ied.fromnode = %s;"
    query = insert.mogrify(query, (impl, impl, impl, ))
    insert.execute(query)
    result = insert.fetchall()

    return result

def getImplementationInformationOrient(insert, impl):
    # todo : Orient
    return []

def getImplementationConflictsPsql(insert, response, respList):
    query = "select distinct rhi2.tonode from responsehasimplementation rhi, responseconflictswithresponse rcr, responsehasimplementation rhi2 where rhi.tonode = %s and rcr.fromnode = rhi.fromnode and rcr.tonode = rhi2.fromnode and rhi2.tonode in %s;"
    query = insert.mogrify(query, (response, tuple(respList), ))
    insert.execute(query)
    result = insert.fetchall()

    return result

def getImplementationConflictsOrient(insert, response):
    # todo : Orient
    return []

def getImplementationPreconditionsPsql(insert, response):
    query = "select distinct rhi2.tonode, ied.tonode from responsehasimplementation rhi, responseispreconditionofresponse rpr, responsehasimplementation rhi2, implementationisexecutedbydevice ied where rhi.tonode = %s and rpr.tonode = rhi.fromnode and rpr.fromnode = rhi2.fromnode and rhi2.fromnode = ied.fromnode;"
    query = insert.mogrify(query, (response, ))
    insert.execute(query)
    result = insert.fetchall()

    return result

def getImplementationPreconditionsOrient(insert, response):
    # todo : Orient
    return []

def getImplementationPreconditionsWithExecutorPsql(insert, response):
    query = "select distinct i.name, d.name, rhi2.tonode, ied.tonode from responsehasimplementation rhi, responseispreconditionofresponse rpr, responsehasimplementation rhi2, implementationisexecutedbydevice ied, implementation i, device d where rhi.tonode = %s and rpr.tonode = rhi.fromnode and rpr.fromnode = rhi2.fromnode and rhi2.fromnode = ied.fromnode and i.id = rhi2.tonode and ied.tonode = d.id;"
    query = insert.mogrify(query, (response, ))
    insert.execute(query)
    result = insert.fetchall()

    return result

def getImplementationPreconditionsWithExecutorOrient(insert, response):
    # todo : Orient
    return []


def getSuperContextPsql(insert, contextID):

    query = "WITH RECURSIVE contextTree (fromnode, level, tonode) AS ( SELECT id, 0, id FROM alertcontext WHERE id = %s UNION ALL SELECT cTree.tonode, cTree.level + 1, context.tonode FROM contexttocontext context, contextTree cTree WHERE context.fromnode = cTree.fromnode) SELECT distinct tonode FROM contextTree WHERE level = (select max(level) from contextTree);"

    query = insert.mogrify(query, (contextID, ))
    insert.execute(query)
    result = insert.fetchall()

    return result

def getSuperContextOrient(insert, contextID):
    # todo : Orient
    return []

def getSubContextPsql(insert, contextID):

    query = "WITH RECURSIVE contextTree (fromnode, level, tonode) AS ( SELECT id, 0, id FROM alertcontext WHERE id = %s UNION ALL SELECT cTree.tonode, cTree.level + 1, context.fromnode FROM contexttocontext context, contextTree cTree WHERE context.tonode = cTree.fromnode) SELECT distinct tonode FROM contextTree WHERE level > 0;"

    query = insert.mogrify(query, (contextID, ))
    insert.execute(query)
    result = insert.fetchall()

    return result

def getSubContextOrient(insert, contextID):
    # todo : Orient
    return []

def getIssuePsql(insert, contextID):

    subcontexts = getSubContextPsql(insert, contextID)
    subcontextList = []
    for elem in subcontexts:
        subcontextList.append(elem[0])

    if len(subcontextList) == 0:
        return None

    query = "select id from alertcontext where name like '%%issue%%'"
    insert.execute(query)
    result = insert.fetchall()

    for issue in result:

        issueSubContext = getSubContextPsql(insert, issue[0])
        issueSubContextList = []
        for elem in issueSubContext:
            issueSubContextList.append(elem[0])

        intersect = list(set(issueSubContextList) & set(subcontextList))
        if len(intersect) != 0:
            return issue

    return None

def getIssueOrient(insert, contextID):
    # todo : Orient
    return []

