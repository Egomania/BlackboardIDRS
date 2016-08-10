import logging

from multiprocessing import Process, Queue

from helper_functions import dbConnector
from topology import nodes, edges

logger = logging.getLogger("idrs")

listenTo = ['implementationisinbundle']
name = 'ResonseExecution'

class PlugIn (Process):

    def __init__(self, q, dbs):
        Process.__init__(self)
        self.subscribe = q[listenTo[0] + '_' + name]
        self.dbs = dbs
        dbConnector.connectToDB(self)
        self.current = []

    def stop(self, commit=False):
        logger.info( 'Stopped "{0}"'.format(self.__module__) )
        dbConnector.disconnectFromDB(self, commit)

    def run(self):

        logger.info( 'Start "{0}"'.format(self.__module__) )
        while (True):
            changed = self.subscribe.get()
            current = changed['new']['tonode']
            selected = changed['new']['_selected']
            if selected and current not in self.current:
                self.current.append(current)
                print ("Start Execution of bundle ", current)

