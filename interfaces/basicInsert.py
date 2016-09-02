from classes import alertProcessing as AP

def insertAlertOrient(alert, session_id, client):

    query = (
        "select EXPAND($inter2) "
        "LET $a = (select EXPAND(IN('alertcontexthastarget')) from ip where @RID = " + alert.targetID + "), "
        "$b = (select EXPAND(IN('alertcontexthassource')) from ip where @RID = " + alert.sourceID + "), "
        "$c = (select EXPAND(IN('alertcontextisoftype')) from attack where @RID = " + alert.classificationID + "), "
        "$inter1 = INTERSECT($a, $b), $inter2 = INTERSECT($inter1, $c)"
    )

    result = client.query(query,-1)

    contextName = str(alert.source) + "_" + str(alert.target) + "_" + str(alert.classification) 
    contextRID = "(select from alertcontext where name = '" + contextName + "')"
    alertRID = "(select from alert where name = '" + alert.msgID + "' and detectiontime = DATE('" + str(alert.dt) + " '))"

    if len(result) == 0:

        if alert.serviceID != None:
            servicestatement = "let $g = create edge alertcontexthasservicetarget from " + contextRID + " to "  + alert.serviceID + "set name = 'alertcontexthasservicetarget';"
        else:
            servicestatement = ""

        cmd = ("begin;"
        "let $a = create vertex alert set name = '" + alert.msgID + "', detectiontime = DATE('" + str(alert.dt) + "');"
        "let $b = create vertex alertcontext set name = '" + contextName + "', _solved = 'False' ;"
        "let $c = create edge alertContextIsOfType from " + contextRID + " to "  + alert.classificationID + " set name = 'alertContextIsOfType';"
        "let $d = create edge alertContextHasSource from " + contextRID + " to "  + alert.sourceID + " set name = 'alertContextHasSource';"
        "let $e = create edge alertContextHasTarget from " + contextRID + " to "  + alert.targetID + "set name = 'alertContextHasTarget';"
        "let $f = create edge alertToContext from " + alertRID + " to "  + contextRID + "set name = 'alertToContext';" + servicestatement +
        "commit;")

    else:
        
        if alert.serviceID != None:
            # todo check if exists
            servicestatement = "let $g = create edge alertcontexthasservicetarget from " + result[0]._rid + " to "  + alert.serviceID + "set name = 'alertcontexthasservicetarget';"
        else:
            servicestatement = ""

        cmd = ("begin;" +
        "let $a = create vertex alert set name = '" + alert.msgID + "', detectiontime = DATE('" + str(alert.dt) + "');"
        "let $f = create edge alertToContext from " + alertRID + " to "  + result[0]._rid + "set name = 'alertToContext';" + servicestatement +
        "commit;")

    for i in range (100):
        try:
            client.batch(cmd)
            break
        except:
            pass
        #except Exception as inst:
        #    print ("EXCEPCTION in RUN ", i)
        #    print (type(inst))
        #    print (inst.args)
        #    print (inst)

def insertAlertPsql(alert, conn, cur):

    statement = "select id from alert where detectiontime_date = %s AND detectiontime_time = %s AND name = %s;"
    statement = cur.mogrify(statement, (alert.creationDate, alert.creationTime, alert.msgID, ))
    cur.execute(statement)

    result = cur.fetchone()
    if result == None:

        statement = 'insert into alert (detectiontime_date, detectiontime_time, name) VALUES(%s, %s,%s) RETURNING id'
        statement = cur.mogrify(statement, (alert.creationDate, alert.creationTime, alert.msgID, ))
        cur.execute(statement)
        alertID = cur.fetchone()[0]

    else:   
        alertID = result[0]

    statement = 'select a.id from alertContext a, alertContexthasSource s, alertContexthasTarget t, alertContextIsOfType ty where s.tonode = %s and t.tonode = %s and ty.tonode = %s and ty.fromnode = a.id and s.fromnode = a.id and t.fromnode = a.id'
    statement = cur.mogrify(statement, (alert.sourceID, alert.targetID, alert.classificationID, ))
    cur.execute(statement)
    alertContextID = cur.fetchone()

    if alertContextID == None:
        statement = 'insert into alertContext (name, _solved) VALUES(%s,%s) RETURNING id'
        statement = cur.mogrify(statement, (str(alert.source + "_" + alert.target + "_" + alert.classification), False, ))
        cur.execute(statement)
        alertContextID = cur.fetchone()[0]
        statement = 'insert into alertContextIsOfType (fromnode,tonode,name) VALUES(%s,%s,%s) RETURNING id'
        statement = cur.mogrify(statement, (alertContextID, alert.classificationID, "alertcontextisoftype", ))
        cur.execute(statement)
        statement = 'insert into alertContexthasSource (fromnode,tonode,name) VALUES(%s, %s, %s)'
        statement = cur.mogrify(statement, (alertContextID, alert.sourceID, "alertcontexthassource", ))
        cur.execute(statement)
        statement = 'insert into alertContexthasTarget (fromnode,tonode,name) VALUES(%s, %s,%s)'
        statement = cur.mogrify(statement, (alertContextID, alert.targetID, "alertcontexthastarget"))
        cur.execute(statement)
        statement = 'insert into alertContexthasTarget (fromnode,tonode,name) VALUES(%s, %s,%s)'
        statement = cur.mogrify(statement, (alertContextID, alert.targetID, "alertcontexthastarget"))
        cur.execute(statement)
       
    else:
        alertContextID = alertContextID[0]
                
    statement = 'insert into alertToContext (fromnode,tonode,name) VALUES(%s,%s,%s)'
    statement = cur.mogrify(statement, (alertID, alertContextID, "alerttocontext"))
    cur.execute(statement)

    specialTarget = False
    if alert.serviceID != None:
        specialTarget = True
        statement = "select * from alertcontexthasservicetarget r where r.fromnode = %s and r.tonode = %s"
        statement = cur.mogrify(statement, (alertContextID, alert.serviceID))
        cur.execute(statement)
        res = cur.fetchall()
        if len(res) == 0:
            statement = 'insert into alertcontexthasservicetarget (fromnode,tonode,name) VALUES(%s, %s,%s)'
            statement = cur.mogrify(statement, (alertContextID, alert.serviceID, "alertcontexthasservicetarget", ))
            cur.execute(statement)

    if alert.userID != None:
        specialTarget = True
        statement = "select * from alertcontexthasusertarget r where r.fromnode = %s and r.tonode = %s"
        statement = cur.mogrify(statement, (alertContextID, alert.userID))
        cur.execute(statement)
        res = cur.fetchall()
        if len(res) == 0:
            statement = 'insert into alertcontexthasusertarget (fromnode,tonode,name) VALUES(%s, %s,%s)'
            statement = cur.mogrify(statement, (alertContextID, alert.userID, "alertcontexthasusertarget", ))
            cur.execute(statement)

    if not specialTarget:
        statement = "select d.id from device d, devicehasinterface dhasiface, mactointerface mactoiface, iptomac, ip where dhasiface.fromnode = d.id and dhasiface.tonode = mactoiface.tonode and mactoiface.fromnode = iptomac.tonode and iptomac.fromnode = ip.id and ip.id = %s"
        statement = cur.mogrify(statement, (alert.targetID, ))
        cur.execute(statement)
        deviceID = cur.fetchone()[0]
        statement = "select * from alertcontexthashosttarget r where r.fromnode = %s and r.tonode = %s"
        statement = cur.mogrify(statement, (alertContextID, deviceID, ))
        cur.execute(statement)
        res = cur.fetchall()
        if len(res) == 0:
            statement = 'insert into alertcontexthashosttarget (fromnode,tonode,name) VALUES(%s, %s,%s)'
            statement = cur.mogrify(statement, (alertContextID, deviceID, "alertcontexthashosttarget", ))
            cur.execute(statement)

    conn.commit()
