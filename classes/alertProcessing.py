import threading
import imp
import re
import datetime

class Alert():

    def __init__(self, alertID):
        self.msgID = alertID
        self.source = None
        self.sourceID = None
        self.target = None
        self.targetID = None
        self.classification = None
        self.classificationID = None
        self.creationDate = None
        self.creationTime = None
        self.dt = None
        self.serviceID = None
        self.service = None
        self.port = None
        self.user = None
        self.userID = None

    def printAlert(self):
        return ("Message ID = " + self.msgID + " Source = " + self.source + " Target = " + self.target + " Classification = " + self.classification)

class Issue():

    def __init__(self, callbackLocation, callbackModule, obj, callbackFKT,ident):
        self.ident = ident
        self.name = "issue_" + str(ident) + "_" + str(datetime.datetime.now().isoformat())
        imp.load_source(callbackModule, callbackLocation)
        self.obj = obj
        self.callbackFKT = callbackFKT
        self.t = threading.Timer(10,getattr(self.obj, self.callbackFKT), [self])
        self.t.start()
        self.sheduled = False

    def restartTimer(self):
        self.t.cancel()
        self.t = threading.Timer(10,getattr(self.obj, self.callbackFKT), [self])
        self.t.start()
