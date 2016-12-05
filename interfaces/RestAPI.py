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
from interfaces import basicInsert

from helper_functions import dbConnector
from helper_functions import query_helper as qh

listenTo = []
name = 'RestAPI'

logger = logging.getLogger("idrs."+name)
logger.setLevel(20)


class IDMEF(Resource):
    def __init__ (self, cur, conn, dbs):
        self.insert = cur
        self.DBconnect = conn
        self.dbs = dbs

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
            
            alertToInsert.targetID = nodes.ip(alertToInsert.target, client = self.insert).rid
            alertToInsert.sourceID = nodes.ip(alertToInsert.source, client = self.insert).rid
            alertToInsert.classificationID = nodes.attack(alertToInsert.classification, client = self.insert).rid

            self.DBconnect.commit();

            if self.dbs.backend == 'psql':
                basicInsert.insertAlertPsql(alertToInsert, self.DBconnect, self.insert)
            elif self.dbs.backend == 'orient':
                basicInsert.insertAlertOrient(alertToInsert, self.DBconnect, self.insert)
            else:
                print ("Wrong backend: ", dbs.backend)
                logger.error("Wrong backend: %s", dbs.backend)
                sys.exit(0)

            
            
        logger.info( '"{0}" committed incomming Alerts with AlertID "{1}"'.format(self.__module__, alertToInsert.msgID) )

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
        dbConnector.connectToDB(self)

    def stop(self):
        logger.info( 'Stopped "{0}"'.format(self.__module__) )
        dbConnector.disconnectFromDB(self, False)

    def run(self):

        logger.info( 'Start "{0}"'.format(self.__module__) )

        self.api.add_resource(IDMEF, '/alert', resource_class_kwargs={'cur': self.insert, 'conn': self.DBconnect, 'dbs': self.dbs})

        self.app.run(debug=True, port=7873, use_reloader=False)
            

