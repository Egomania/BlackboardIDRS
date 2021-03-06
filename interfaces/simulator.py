import select
import logging
import sys
import psycopg2
import psycopg2.extensions
import pyorient
import os

import configparser
import xmltodict
import datetime
import time
import csv

import timeout_decorator

from multiprocessing import Process, Queue
import xml.etree.ElementTree as ET

from topology import edges, nodes
from classes import alertProcessing as AP
from interfaces import basicInsert

Config = configparser.ConfigParser()
Config.read('configs/simulator.ini')
repeats = int(Config.getint('Times','repeats'))
timeout = int(Config.getint('Times','timeout'))
cleanDBFromAlert = bool(Config.getboolean('Database','cleanDBFromAlert'))
initialClean = bool(Config.getboolean('Database','initialClean'))
simFolder = str(Config.get('Files','simFolder'))
resultsFile = str(Config.get('Files','resultsFile'))
resultsPath = simFolder + resultsFile
EVAL = bool(Config.getboolean('Basics','eval'))
alertFormat = str(Config.get('Basics','format'))
if EVAL:
    name = 'simulatorEval'
    listenTo = ['eval']
    if not os.path.exists(resultsPath):
        with open(resultsPath,"w+") as f:
            fileWriter = csv.writer(f, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            fileWriter.writerow(['Filename','Duration', 'FileSize', 'NumberAlerts', 'NumberContext', 'NumberContextNonIssue','UniqueContext','Backend','attackPath', 'sameSource', 'sameTarget', 'sameClass', 'bundle','selected','nonselected','executed','nonexecuted'])
else:
    name = 'simulatorSilent'
    listenTo = []

logger = logging.getLogger("idrs."+name)
#logger.setLevel(20)

class PlugIn (Process):

    alertList = {}
    sourceIP = {}
    targetIP = {}
    classification = {}
    timeList = {}
    service = {}

    def __init__(self, dbs, q):
        Process.__init__(self)
        self.backend = dbs.backend
        if self.backend == 'orient':
            self.client = pyorient.OrientDB(dbs.server, dbs.port)
            self.session_id = self.client.connect(dbs.user, dbs.pwd)
            self.client.db_open(dbs.database, dbs.user, dbs.pwd)
        elif self.backend == 'psql':
            self.conn = psycopg2.connect(database=dbs.database, user=dbs.user, password=dbs.pwd, port=dbs.port, host=dbs.server)
            self.cur = self.conn.cursor()
        else:
            logger.error("Wrong Database Backend %s", self.backend)
            sys.exit()
        
        self.cancel = q['main']
        self.feedback = q['feedback']

        if EVAL:
            self.subscribe = q[listenTo[0] + '_' + name]

    def stop(self):
        logger.info( 'Stopped "{0}"'.format(self.__module__) )
        if self.backend == 'orient':
            self.client.db_close()
        elif self.backend == 'psql':
            self.cur.close()
            self.conn.close()
        else:
            logger.error("Wrong Database Backend %s", self.backend)

    def createData(self):
        if self.backend == 'orient':
            client = self.client
        elif self.backend == 'psql':
            client = self.cur
        else:
            logger.error("Wrong Database Backend %s", self.backend)
            return

        for ip in self.sourceIP.keys():
            ipNode = nodes.ip(ip, client = client)
            self.sourceIP[ip] = ipNode.rid
        for ip in self.targetIP.keys():
            ipNode = nodes.ip(ip, client = client)
            self.targetIP[ip] = ipNode.rid
        for classification in self.classification.keys():
            typeNode = nodes.attack(classification, client = client)
            self.classification[classification] = typeNode.rid
        for service in self.service.keys():
            if service == None:
                continue
            serviceNode = nodes.service(service, client=client)
            self.service[service] = serviceNode.rid

    def cleanDB(self):
        
        if self.backend == 'orient':
            commit = False
            i = 0
            while not commit:
                i = i + 1
                try:
                    self.client.command("delete vertex alert")
                    self.client.command("delete vertex alertcontext")
                    self.client.command("truncate class alert")
                    self.client.command("truncate class alertcontext")
                    commit = True
                except Exception as inst:
                    logger.warning("Databased locked in RUN %s : %s", i, inst)
                    print ("Databased locked in RUN %s : %s", i, inst)
                    self.client.db_reload()
                    self.client.command("truncate class alert unsafe")
                    self.client.command("truncate class alertcontext unsafe")
                    self.client.command("truncate class alertcontexthassource unsafe")
                    self.client.command("truncate class alertcontexthasservicetarget unsafe")
                    self.client.command("truncate class alertcontexthastarget unsafe")
                    self.client.command("truncate class alertcontextisoftype unsafe")
                    self.client.command("truncate class alerttocontext unsafe")
                    self.client.command("truncate class contexttocontext unsafe")
                    while True:
                        try:
                            self.client.command("update attack remove in_alertcontextisoftype")
                            self.client.command("update ip remove in_alertcontexthastarget")
                            self.client.command("update ip remove in_alertcontexthassource")
                            self.client.command("update service remove in_alertcontexthasservicetarget")
                            break
                        except:
                            print ("so ein rotz")
                    
        elif self.backend == 'psql':
            for i in range(10):
                try:
                    self.cur.execute("TRUNCATE TABLE alert RESTART IDENTITY CASCADE;")
                    self.cur.execute("TRUNCATE TABLE alertcontext RESTART IDENTITY CASCADE;")
                    self.cur.execute("TRUNCATE TABLE bundle RESTART IDENTITY CASCADE;")
                    self.conn.commit()
                except Exception as inst:
                    logger.error("Database locked in RUN %s : %s", i, inst)
            

        else:
            logger.error("Wrong Database Backend %s", self.backend)
    
    @timeout_decorator.timeout(timeout)
    def getValue(self):
        return self.subscribe.get()

    def writeResults(self, fileName, duration):
        # ['Filename','Duration', 'FileSize', 'NumberAlerts', 'NumberContext','NumberContextNonIssue','UniqueContext','Backend','attackPath', 'sameSource', 'sameTarget', 'sameClass', 'bundle']

        row = []
        row.append(fileName)
        row.append(duration)
        row.append(os.stat(simFolder+fileName).st_size )
        row.append(len(self.alertList[fileName]))

        sames = {}

        if self.backend == 'orient':
            query = "SELECT count(*) from alertcontext"
            result = self.client.query(query,-1)
            numberContext = result[0].oRecordData['count']
            query = "select count(*) from alertcontext where out('contexttocontext').size() = 0"
            result = self.client.query(query,-1)
            uniqueCon = result[0].oRecordData['count']
            for elem in ['attackPath', 'sameSource', 'sameTarget', 'sameClass']:
                query = "SELECT count(*) from alertcontext where name like '%" + elem + "%'"
                result = self.client.query(query,-1)
                sames[elem] = result[0].oRecordData['count']

        elif self.backend == 'psql':
            statement = "SELECT count(*) from alertcontext;"
            self.cur.execute(statement)
            numberContext = int(self.cur.fetchone()[0])
            statement = "select count(distinct(tonode)) from contexttocontext where tonode not in (select fromnode from contexttocontext);"
            self.cur.execute(statement)
            uniqueCon = int(self.cur.fetchone()[0])
            for elem in ['attackPath', 'sameSource', 'sameTarget', 'sameClass']:
                statement = "SELECT count(*) from alertcontext where name like '%" + elem + "%';"
                self.cur.execute(statement)
                sames[elem] = int(self.cur.fetchone()[0])
            statement = "SELECT count(*) from bundle;"
            self.cur.execute(statement)
            bundleCount = int(self.cur.fetchone()[0])
            statement = "SELECT count(*) from alertcontext where name not like '%issue%';"
            self.cur.execute(statement)
            numberContextNonIssue = int(self.cur.fetchone()[0])
            statement = "select count(*) from implementationisinbundle where _selected = True;"
            self.cur.execute(statement)
            selectedCount = int(self.cur.fetchone()[0])
            statement = "select count(*) from implementationisinbundle where _selected = False;"
            self.cur.execute(statement)
            NonselectedCount = int(self.cur.fetchone()[0])
            statement = "select count(*) from implementationisinbundle where _executed = True;"
            self.cur.execute(statement)
            execCount = int(self.cur.fetchone()[0])
            statement = "select count(*) from implementationisinbundle where _executed = False;"
            self.cur.execute(statement)
            NonexecCount = int(self.cur.fetchone()[0])
            self.conn.commit()
        else:
            numberContext = None
            uniqueCon = None
            bundle = None
            numberContextNonIssue = None
            sames['attackPath'] = None
            sames['sameSource'] = None
            sames['sameTarget'] = None
            sames['sameClass'] = None
            logger.error("Wrong Database Backend %s", self.backend)

        row.append(numberContext)
        row.append(numberContextNonIssue)
        row.append(uniqueCon)
        row.append(self.backend)
        row.append(sames['attackPath'])
        row.append(sames['sameSource'])
        row.append(sames['sameTarget'])
        row.append(sames['sameClass'])
        row.append(bundleCount)
        row.append(selectedCount)
        row.append(NonselectedCount)
        row.append(execCount)
        row.append(NonexecCount)

        with open(resultsPath,"a+") as f:
            fileWriter = csv.writer(f, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            fileWriter.writerow(row)

    def createAlerts(self, fileName):

        #print ("start with ", fileName)

        start = time.time()

        if self.backend == 'orient':
            for alert in self.alertList[fileName]:
                basicInsert.insertAlertOrient(alert, self.session_id, self.client)
        elif self.backend == 'psql':
            for alert in self.alertList[fileName]:
                basicInsert.insertAlertPsql(alert, self.conn, self.cur)
        else:
            logger.error("Wrong Database Backend %s", self.backend)
        
        print ("++++++++++++++++++++++++++++++++++++++++wait+++++++++++++++++++++++++++++")

        if EVAL:
            while (True):
                try:
                    changed = self.getValue()
                    #print (changed['table'],changed['operation'],changed['ident'])
                except Exception as inst:
                    logger.warning("Error : %s", inst)
                    break

        end = time.time()

        duration = end - start - timeout

        self.timeList[fileName].append(start)
        self.timeList[fileName].append(end)
        self.timeList[fileName].append(duration)
        self.timeList[fileName].append(len(self.alertList[fileName]))
     
        if EVAL:
            print ("Send Cancel Request")
            self.cancel.put("cancel")
            while (True):
                feedbackValue = self.feedback.get()
                if feedbackValue == "cancelSuccess":
                    break
            #print ("Write Results and Flush DB...")
            self.conn.commit()
            self.writeResults(fileName, duration)

            if cleanDBFromAlert:
                self.cleanDB()

            print ("Send Restart Request")
            self.cancel.put("restart")
            while (True):
                feedbackValue = self.feedback.get()
                if feedbackValue == "restartSuccess":
                    break
            #print ("Go ahead with evaluation ...")

        #print ("Next File")

    def readIDMEFv1File(fileName):

        tree = ET.parse(lldosFolder+fileName)      
        root = tree.getroot()

        for idmefMsg in root:
            for alert in idmefMsg:
                alertElem = AP.Alert(alert.get('alertid'))
                alertElem.source = alert.findall(".//Source//address")[0].text
                alertElem.target = alert.findall(".//Target//address")[0].text
                try:
                    alertClass = alert.findall(".//Service//name")[0].text + alert.findall(".//Service//dport")[0].text
                except:
                    alertClass = alert.findall(".//Service//name")[0].text
                alertElem.classification = alertClass
                creation = alert.findall(".//Time//date")[0].text + " " + alert.findall(".//Time//time")[0].text
                formattedTime = time.strptime(creation, "%d/%m/%Y %H:%M:%S")
                dt = datetime.datetime.fromtimestamp(time.mktime(formattedTime))
                alertElem.dt = dt

                self.alertList[fileName].append(alertElem)
                self.sourceIP[alertElem.source] = True
                self.targetIP[alertElem.target] = True
                self.classification[alertElem.classification] = True

        for entry in self.alertList.keys():
            for elem in self.alertList[entry]:
                logger.debug("File : %s has Alert =  %s", entry, elem.printAlert())   

        logger.info("Source IPs found: %s", self.sourceIP.keys())
        logger.info("Target IPs found: %s", self.targetIP.keys())
        logger.info("Classifications found: %s", self.classification.keys())
        logger.info("Services found: %s", self.service.keys())


    def readIDMEFFile(self, fileName):

        tree = ET.parse(simFolder+fileName)      
        root = tree.getroot()

        for idmefMsg in root:
            for alert in idmefMsg:
                alertElem = AP.Alert(alert.get('messageid'))
                alertElem.source = alert.findall(".//Source//address")[0].text
                alertElem.target = alert.findall(".//Target//address")[0].text
                alertElem.classification = alert.findall(".//Classification")[0].get('text')
                try:
                    alertElem.port = alert.findall(".//Target//Service//port")[0].text
                    alertElem.service = alert.findall(".//Target//Service//name")[0].text
                except:
                    pass
                
                creation = alert.findall(".//CreateTime")[0].text
                formattedTime = time.strptime(creation, "%Y-%m-%dT%H:%M:%S.%fZ")
                dt = datetime.datetime.fromtimestamp(time.mktime(formattedTime))
                alertElem.dt = dt
                
                self.alertList[fileName].append(alertElem)
                self.sourceIP[alertElem.source] = True
                self.targetIP[alertElem.target] = True
                self.classification[alertElem.classification] = True
                self.service[alertElem.service] = True

        for entry in self.alertList.keys():
            for elem in self.alertList[entry]:
                logger.debug("File : %s has Alert =  %s", entry, elem.printAlert())   

        logger.info("Source IPs found: %s", self.sourceIP.keys())
        logger.info("Target IPs found: %s", self.targetIP.keys())
        logger.info("Classifications found: %s", self.classification.keys())


    def run(self):

        logger.info( 'Start "{0}"'.format(self.__module__) )
    
        for fileName in os.listdir(simFolder):
            if fileName.endswith(".xml"):
                self.alertList[fileName] = []
                self.timeList[fileName] = []
                if alertFormat == 'idmef':
                    self.readIDMEFFile(fileName)
                elif alertFormat == 'idmefv1':
                    self.readIDMEFv1File(fileName)
                else:
                    print ("Wrong alert message format: ", format)
                    sys.exit(0)

        print ("Read all files.")

        self.createData()
        for elem in self.alertList.keys():
            for alertElem in self.alertList[elem]:
                alertElem.sourceID = self.sourceIP[alertElem.source]
                alertElem.targetID = self.targetIP[alertElem.target]
                alertElem.classificationID = self.classification[alertElem.classification]
                if alertElem.service != None:
                    alertElem.serviceID = self.service[alertElem.service]
                alertElem.creationDate = datetime.datetime.date(alertElem.dt)
                alertElem.creationTime = datetime.datetime.time(alertElem.dt)

        print ("Created Data.")

        logger.info("Update info for Source IPs : %s", self.sourceIP)
        logger.info("Update info for Target IPs : %s", self.targetIP)
        logger.info("Update info for Classifications : %s", self.classification)
        
        if initialClean:
            logger.info("Clean database and reset RIDs for class alert and alertcontext.")
            self.cleanDB()

        print ("Cleaned Database")

        for fileName in self.alertList.keys():
            for i in range(repeats):
                print ("Execute for ", fileName, " in repetition ", i)
                self.createAlerts(fileName)

        print ("Inserted Alerts.")

        #print (self.timeList)

        

