import logging
import psycopg2
import psycopg2.extensions
import pyorient
import sys

from multiprocessing import Process, Queue

from helper_functions import dbConnector
from helper_functions import query_helper as qh
from topology import nodes, edges

from classes import ResponseSelection as rs
from modules.solvers import greedyCost as solver

logger = logging.getLogger("idrs")

listenTo = ['bundle']
name = 'ResponseSelection'

class PlugIn (Process):

    def __init__(self, q, dbs):
        Process.__init__(self)
        self.subscribe = q[listenTo[0] + '_' + name]
        self.dbs = dbs
        dbConnector.connectToDB(self)

    def stop(self, commit=False):
        logger.info( 'Stopped "{0}"'.format(self.__module__) )
        dbConnector.disconnectFromDB(self, commit)

    def getListing(self, bundleID):

        data = {}
        listing = []
        impls = []

        functionName = 'selectSingleValue' + self.dbs.backend.title()
        issue = getattr(qh, functionName)(self.insert, "bundlesolvesalertcontext", "tonode", "fromnode", bundleID, False)
        
        functionName = 'geteffectedEntities' + self.dbs.backend.title()
        effectedEntities = getattr(qh, functionName)(self.insert, issue)
        
        devMapper = {}
        functionName = 'selectSingleValue' + self.dbs.backend.title()
        for victim in effectedEntities['service']:
            result = getattr(qh, functionName)(self.insert, "servicerunsondevice", "tonode", "fromnode", victim)
            for elem in result:
                if elem[0] not in devMapper.keys():
                    devMapper[elem[0]] = []
                devMapper[elem[0]].append(str(elem[0]) + "_s" + str(victim))

        for victim in effectedEntities['user']:
            result = getattr(qh, functionName)(self.insert, "userloggedondevice", "tonode", "fromnode", victim)
            for elem in result:
                if elem[0] not in devMapper.keys():
                    devMapper[elem[0]] = []
                devMapper[elem[0]].append(str(elem[0]) + "_u" + str(victim))

        for victim in effectedEntities['host']:
            if victim not in devMapper.keys():
                devMapper[victim] = []
            devMapper[elem[0]].append(str(victim))
        
        functionName = 'getNotYetSelectedImplementations' + self.dbs.backend.title()
        result = getattr(qh, functionName)(self.insert, bundleID)

        for elem in result:
            impls.append(elem[0])

        host_attacked = []
        host_executing = []
        responses_used = []
        metrics_used = []
        damage_used = []
        conflictsList = []
        responseList = {}

        functionName = 'getImplementationInformation' + self.dbs.backend.title()
        for impl in impls:
            result = getattr(qh, functionName)(self.insert, impl)
            for elem in result:
                if elem[0] in devMapper.keys():
                    for victim in devMapper[elem[0]]:
                        if elem[3] not in metrics_used:
                            metrics_used.append(elem[3])
                        if victim not in host_attacked:
                            host_attacked.append(victim)
                        if elem[1] not in host_executing:
                            host_executing.append(elem[1])

                        if elem[2] not in responseList.keys():
                            responseList[elem[2]] = {}
                            responseList[elem[2]]['metrics'] = []
                            responseList[elem[2]]['conflicts'] = []
                            responseList[elem[2]]['src'] = elem[1]
                            responseList[elem[2]]['dst'] = []

                        responseList[elem[2]]['dst'].append(victim)

                        metric = rs.Metric(elem[3], elem[4])
                        responseList[elem[2]]['metrics'].append(metric)
 
        functionName = 'getImplementationConflicts' + self.dbs.backend.title()
        for response in responseList.keys():
            result = getattr(qh, functionName)(self.insert, response, responseList.keys())
            for elem in result:
                conflictingResponse = rs.Response(name = elem[0])
                responseList[response]['conflicts'].append(conflictingResponse)
                conflictsList.append((response, elem[0]))

            responseElem = rs.Response(name=response, src=responseList[response]['src'], dest=responseList[response]['dst'], metrics=responseList[response]['metrics'], conflicts=responseList[response]['conflicts'])

            responses_used.append(responseElem)

        data["attacked"] = host_attacked
        data["executing"] = host_executing
        data["response"] = responses_used
        data["metric"] = metrics_used
        data["damage"] = damage_used
        data["conflict"] = conflictsList

        return data

    def run(self):

        logger.info( 'Start "{0}"'.format(self.__module__) )
        while (True):
            changed = self.subscribe.get()
            if (changed['new']['_active'] != None) and (changed['new']['_active']):
                data = {}
                bundleID = changed['new']['id']
                data = self.getListing(bundleID)
                functionName = 'getMaxIteration' + self.dbs.backend.title()
                try:
                    iteration = getattr(qh, functionName)(self.insert, bundleID) + 1
                except:
                    logger.error("No function %s found for backend: %s", functionName, self.dbs.backend)
                    continue

                print ('Start Solver for bundle ', bundleID)
                
                # todo : option to include multiple Solvers

                currentSolver = solver.Solver()
                problem = currentSolver.create_problem(data)
                erg = currentSolver.solve_problem(problem, None)

                for elem in erg[1]:
                    if elem[1] == True:
                        updateValues = {'_iteration' : iteration, '_selected' : True}
                        functionName = 'updateEdge' + self.dbs.backend.title()
                        getattr(qh, functionName)(self.DBconnect, self.insert, 'implementationisinbundle', elem[0], bundleID, updateValues, True)
