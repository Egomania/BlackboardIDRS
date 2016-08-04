import select
import psycopg2
import psycopg2.extensions
import logging
import sys
import signal
import os

import pyinotify
import xmltodict
import datetime
import time

from flask import Flask, request
from flask_restful import Resource, Api
from psycopg2.extensions import AsIs
from multiprocessing import Process, Queue

from topology import nodes, edges
from classes import alertProcessing as AP
import basicInsert

logger = logging.getLogger("idrs")

listenTo = []
name = 'RestAPI'

class IDMEF(Resource):
    def __init__ (self, cur, conn):
        self.cur = cur
        self.conn = conn

    def put(self):
        data = request.form['data']
        dataDict = xmltodict.parse(data)
        if type(dataDict['IDMEF-Message']['Alert']) is list:
            elem = dataDict['IDMEF-Message']['Alert']
        else:
            elem = []
            elem.append(dataDict['IDMEF-Message']['Alert'])
        for alert in elem:

            alertToInsert = AP.Alert(alert['@messageid'])
            alertToInsert.target = alert['Target']['Node']['Address']['address']
            alertToInsert.source = alert['Source']['Node']['Address']['address']
            alertToInsert.classification = alert['Classification']['@text']
            alertToInsert.dt = alert['CreateTime']
            formattedTime = time.strptime(alertToInsert.dt, "%Y-%m-%dT%H:%M:%S.%fZ")
            dt = datetime.datetime.fromtimestamp(time.mktime(formattedTime))
            alertToInsert.creationDate = datetime.datetime.date(dt)
            alertToInsert.creationTime = datetime.datetime.time(dt)
            
            alertToInsert.targetID = nodes.ip(alertToInsert.target, client = insert).rid
            alertToInsert.sourceID = nodes.ip(alertToInsert.source, client = insert).rid
            alertToInsert.classificationID = nodes.ip(alertToInsert.classification, client = insert).rid

            if self.dbs.backend == 'psql':
                basicInsert.insertAlertPsql(alert, self.DBconnect, self.insert)
            elif self.dbs.backend == 'orient':
                basicInsert.insertAlertOrient(alert, self.DBconnect, self.insert)
            else:
                print ("Wrong backend: ", dbs.backend)
                logger.error("Wrong backend: %s", dbs.backend)
                sys.exit(0)
            
        logger.info( '"{0}" committed incomming Alerts with AlertID "{1}" and ContextID "{2}"'.format(self.__module__, alertID, alertContextID) )

        return {'status': 'success'}

class PlugIn (Process):

    def __init__(self, dbs, q):
        Process.__init__(self)
        self.app = Flask(__name__)
        self.log = logging.getLogger('werkzeug')
        self.log.disabled = True
        self.app.logger.disabled = True
        self.api = Api(self.app)
        self.dbs = dbs
        if self.dbs.backend == 'psql':
            self.DBconnect = psycopg2.connect(database=dbs.database, user=dbs.user, password=dbs.pwd, port=dbs.port, host=dbs.server)
            self.insert = self.conn.cursor()
        elif self.dbs.backend == 'orient':
            self.insert = pyorient.OrientDB(dbs.server, dbs.port)
            self.DBconnect = self.client.connect(dbs.user, dbs.pwd)
            self.insert.db_open(dbs.database, dbs.user, dbs.pwd)
        else:
            print ("Wrong backend: ", dbs.backend)
            logger.error("Wrong backend: %s", dbs.backend)
            sys.exit(0)


    def stop(self):
        logger.info( 'Stopped "{0}"'.format(self.__module__) )
        self.cur.close()
        self.conn.close()

    def run(self):

        logger.info( 'Start "{0}"'.format(self.__module__) )

        self.api.add_resource(IDMEF, '/alert', resource_class_kwargs={'cur': self.cur, 'conn': self.conn})

        self.app.run(debug=True, port=7873, use_reloader=False)
            

