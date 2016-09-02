import logging
import datetime
import time
import threading

from multiprocessing import Process, Queue
from graph_tool.all import *
from string import Template

from helper_functions import query_helper as qh
from helper_functions import dbConnector
from topology import nodes, edges

listenTo = ['bundle', 'implementationisinbundle']
name = 'ResonseExecution'

logger = logging.getLogger("idrs."+name)
#logger.setLevel(20)

GPLMTFolder = "GPLMT"
GPLMTTargetFolder = GPLMTFolder + "/targets"
GPLMTTasklistsFolder = GPLMTFolder + "/tasklists"
GPLMTExecutionPlansFolder = GPLMTFolder + "/executionplans"
GPLMTTargetFile = "targetsGen.xml"
GPLMTTasklistFile = "tasklistsGen.xml"
tarPre = "target"
taskPre = "task"

class executePlan(threading.Thread):
    def __init__(self, executionPlan, bundle, current, dbs):
        threading.Thread.__init__(self)
        self.plan = executionPlan
        self.bundle = bundle
        self.dbs = dbs
        dbConnector.connectToDB(self)
        self.list = current

    def run(self):
        
        functionName = 'updateNode' + self.dbs.backend.title()
        logger.info('Start Execution for Bundle: %s' , self.bundle)
        getattr(qh, functionName)(self.DBconnect, self.insert, "bundle", self.bundle, {"_executing": True}, True)
        time.sleep(1)
        getattr(qh, functionName)(self.DBconnect, self.insert, "bundle", self.bundle, {"_executing": False}, True)
        logger.info('Finished Execution for Bundle: %s' , self.bundle)
        self.list.remove(self.bundle)
        dbConnector.disconnectFromDB(self, True)

class listenToBundleQueue(threading.Thread):
    def __init__(self, dbs, subscribe, current, loop):
        threading.Thread.__init__(self)
        self.alive = True
        self.subscribe = subscribe
        self.dbs = dbs
        dbConnector.connectToDB(self)
        self.current = current
        self.loop = loop

    def stop(self, commit=False):
        logger.info( 'Stopped THREAD "{0}"'.format(self.__module__) )
        self.alive = False
        self.subscribe.put(None)
        dbConnector.disconnectFromDB(self, commit)

    def run(self):
        logger.info( 'Start THREAD "{0}"'.format(self.__module__) )
        while (self.alive):
            changed = self.subscribe.get()
            if changed == None:
                continue
            current = changed['new']['id']
            ready = changed['new']['_ready']
            
            if ready and current in self.loop.keys():

                functionName = 'updateNode' + self.dbs.backend.title()
                getattr(qh, functionName)(self.DBconnect, self.insert, "bundle", current, {"_ready": False}, True)

                executionThread = executePlan(self.loop[current],current, self.current, self.dbs)
                executionThread.start()
                del self.loop[current]
                continue
        
class listenToBundleRelQueue(threading.Thread):
    def __init__(self, dbs, subscribe, current, loop):
        threading.Thread.__init__(self)
        self.alive = True
        self.subscribe = subscribe
        self.dbs = dbs
        dbConnector.connectToDB(self)
        self.current = current
        self.loop = loop
        self.executionPlan = Graph()
        self.v_prop_name = self.executionPlan.new_vertex_property("int")
        self.v_prop_exec = self.executionPlan.new_vertex_property("int")
        self.v_prop_name_string = self.executionPlan.new_vertex_property("string")
        self.v_prop_exec_string = self.executionPlan.new_vertex_property("string")
        self.executionPlan.vertex_properties["name"] = self.v_prop_name
        self.executionPlan.vertex_properties["executor"] = self.v_prop_exec
        self.GPLMTFile = Template(
        "<?xml version='1.0' encoding='utf-8'?> \n"
        "<experiment> \n"
            "<include file='"+GPLMTTargetFolder+"/"+GPLMTTargetFile+"' prefix='"+tarPre+"' /> \n"
            "<include file='"+GPLMTTasklistsFolder+"/"+GPLMTTasklistFile+"' prefix='"+taskPre+"' /> \n"
            "<targets /> \n"
            "<tasklists /> \n"
            "<steps> $steps </steps> \n"
        "</experiment>")
        self.stepDescription = Template("<step tasklist='$task' targets='$target' />")
        
    def stop(self, commit=False):
        logger.info( 'Stopped THREAD "{0}"'.format(self.__module__) )
        self.alive = False
        self.subscribe.put(None)
        dbConnector.disconnectFromDB(self, commit)


    def getPreconditionsOfElem(self, elem, v):
        functionName = 'getImplementationPreconditionsWithExecutor' + self.dbs.backend.title()
        precon = getattr(qh, functionName)(self.insert, elem)
        for pre in precon:          
            preConID = pre[2]
            v_preList = find_vertex(self.executionPlan, self.v_prop_name, preConID)
            if len(v_preList) == 0:
                v_pre = self.executionPlan.add_vertex()
                self.v_prop_name_string[v_pre] = pre[0]
                self.v_prop_exec_string[v_pre] = pre[1]
                self.v_prop_name[v_pre] = preConID
                self.v_prop_exec[v_pre] = pre[3]
            else:
                v_pre = v_preList[0]
            rel = self.executionPlan.add_edge(v_pre, v)
            self.getPreconditionsOfElem(preConID, v_pre)

    def createExecutionPlan(self, ident):
        GPLMTPlanFile = "GPLMTExecutionPlan_" + str(ident) + "_" + str(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H:%M:%S'))
        stepsToDo = ""
        vertexToDelete = []
        while self.executionPlan.num_vertices() != 0:
            for v in self.executionPlan.vertices():
                if v.in_degree() == 0:
                    vertexToDelete.append(v)
                    task = self.v_prop_name_string[v]
                    target = self.v_prop_exec_string[v]
                    stepsToDo = stepsToDo + self.stepDescription.substitute(task=task,target=target) + "\n"

            stepsToDo = stepsToDo + "<synchronize /> \n"
            for elem in reversed(sorted(vertexToDelete)):
                self.executionPlan.remove_vertex(elem)            
            vertexToDelete = []

        with open(GPLMTExecutionPlansFolder+"/"+GPLMTPlanFile, 'w') as outfile:
            outfile.write(self.GPLMTFile.substitute(steps=stepsToDo))

        return GPLMTPlanFile

    def run(self):
        logger.info( 'Start THREAD "{0}"'.format(self.__module__) )
        while (self.alive):
            changed = self.subscribe.get()
            if changed == None:
                continue
            selected = changed['new']['_selected']
            current = changed['new']['tonode']
            if selected and current not in self.current:
                self.current.append(current)
                logger.info("Start Execution of bundle: %s", current)
                # get all selected implementations
                functionName = 'getLastSelectedImplementationsWithExecutor' + self.dbs.backend.title()
                result = getattr(qh, functionName)(self.insert, current)
                for elem in result:
                    
                    v = self.executionPlan.add_vertex()
                    self.v_prop_name_string[v] = elem[0]
                    self.v_prop_exec_string[v] = elem[1]
                    self.v_prop_name[v] = elem[2]
                    self.v_prop_exec[v] = elem[3]
                    self.getPreconditionsOfElem(elem[2], v)

                plan = self.createExecutionPlan(current)
                logger.info("Prepared execution plan: %s", plan)
                bundleToExecute = nodes.bundle(rid=current, client=self.insert)
                self.loop[current] = plan
                functionName = 'updateNode' + self.dbs.backend.title()
                getattr(qh, functionName)(self.DBconnect, self.insert, "bundle", bundleToExecute.rid, {"_prepared": True}, True)


class PlugIn (Process):

    def __init__(self, q, dbs):
        Process.__init__(self)
        self.alive = True
        self.subscribe = q[listenTo[0] + '_' + name]
        self.dbs = dbs
        dbConnector.connectToDB(self)
        self.current = []
        self.loop = {}
        self.bundleListener = listenToBundleQueue(self.dbs, q[listenTo[0] + '_' + name], self.current, self.loop)
        self.bundleListener.start()
        self.bundleRelListener = listenToBundleRelQueue(self.dbs, q[listenTo[1] + '_' + name], self.current, self.loop)
        self.bundleRelListener.start()

    def stop(self, commit=False):
        logger.info( 'Stopped "{0}"'.format(self.__module__) )
        dbConnector.disconnectFromDB(self, commit)
        self.bundleListener.stop()
        self.bundleRelListener.stop()
        self.alive = False
        self.bundleListener.join()
        self.bundleRelListener.join()

    def run(self):
        pass

