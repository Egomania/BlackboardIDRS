import signal
import sys
import configparser
import imp
import inspect
import time
import logging
import argparse
import pyorient
import psycopg2
import threading
import gc

from multiprocessing import Process, Queue
from queue import Empty

from helper_functions import helper
from helper_functions import startup
from controllers import ControllerOrient
from controllers import ControllerPsql

from topology import nodes
from topology import edges

from classes import MetaData as meta

class inputThread(threading.Thread):
    def __init__(self, q):
        threading.Thread.__init__(self)
        self.q = q

    def run(self):
        input('Press Enter to Stop.\n')
        self.q.put("stop")
        

def loadModules(modulesToUse, path):
    """ 
    Dynamically loads modules for intrusion correlation process.
    """

    modules = []

    for module in modulesToUse:

        selectedModulePath = path + '/' + module + '.py'
        selectedModuleName = module
        my_module = imp.load_source(selectedModuleName, selectedModulePath)
        modules.append(my_module)

    return modules

def stop(backend, threads):
    print('You stopped Progamm!')

    if backend == 'orient':
        client.db_close()
    elif backend == 'psql':
        cur.close()
        conn.close()
    else:
        pass

    for thread in threads:
        thread.stop()

    for thread in threads:
        thread.terminate()

    for thread in threads:
        thread.join()
            
if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-l', '--log',
        help="Set log-level.",
        #dest="loglevel", 
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL', 'NOTSET'],
        default='INFO',
    )
    parser.add_argument(
        '-c', '--config',
        help="Set path to config file.",
        type=str,
        dest="config", 
        nargs='+',
        default=['configs/config.ini'],
    )
    parser.add_argument(
        '-e', '--example',
        help="Add example Data.",
        action='store_true', 
        default=False,
    )

    args = parser.parse_args()
    
    numeric_level = getattr(logging, args.log)

    logger = logging.getLogger('idrs')
    logger.setLevel(numeric_level)

    ch = logging.StreamHandler()    
    formatter = logging.Formatter('%(asctime)s - %(filename)s (%(process)d) - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.info("Log-Level set to: '%s' (%s).", args.log, numeric_level)
    CONFIG_FILEPATH = args.config
    logger.info("Path to Config File: %s", CONFIG_FILEPATH)
    EXAMPLE_DATA = args.example
    logger.info("Use Example Data: %s", EXAMPLE_DATA)

    Config = configparser.ConfigParser()
    Config.read(CONFIG_FILEPATH)

    try:
        backend = str(Config.get('Database','backend'))
        server = str(Config.get('Database','server'))
        port = int(Config.getint('Database','port'))
        user = str(Config.get('Database','user'))
        pwd = str(Config.get('Database','pwd'))
        database = str(Config.get('Database','database'))
        index = bool(Config.getboolean('Database','index'))
        policy = str(Config.get('Database','policyFile'))
        delPol = bool(Config.getboolean('Database','deletePolicy'))
        inf = str(Config.get('Database','infrastructureFile'))
        delInf = bool(Config.getboolean('Database','deleteInfrastructure'))
    except:
        logger.error("Errors in config file - cannot parse")
        sys.exit(0)

    dbs = meta.DatabaseSettings(server, port, user, pwd, database, EXAMPLE_DATA, backend, index, policy, delPol, inf, delInf)

    dbs.logSettings(logger)
    dbs.printSettings()

    nodes.node.backend = backend
    edges.edge.backend = backend

    print ("Prepare Database (" + backend + ") ... ")
    logger.info("Prepare Database ... ")
    if backend == 'orient':
        logger.info("Use Orient Backend ... ")
        client = pyorient.OrientDB(server, port)
        session_id = client.connect(user, pwd)
        classes = startup.createOrient(dbs, client, session_id)
    elif backend == 'psql':
        logger.info("Use Postgres Backend ... ") 
        try:
            conn = psycopg2.connect(database=database, user=user, password=pwd, port=port, host=server)
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
        except(psycopg2.OperationalError):
            conn = None
            cur = None
        classes = startup.createPsql(dbs, conn, cur)
        if conn == None:
            conn = psycopg2.connect(database=database, user=user, password=pwd, port=port, host=server)
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
    else:
        logging.error("Unknown Backend %s. Stop Execution.", backend)
        stop(backend, [])

    logger.info("Successfully checked Database with following nodes and edges: %s", classes)
    
    print ("Start modules ... ")

    q = {}
    threads = []
    modules = []
    moduleList = []
    qList = []
    q["main"] = Queue()
    q["feedback"] = Queue()
    ownQueue = q["main"]
    feedbackQueue = q["feedback"]
    listenQueue = {}
    listenQueue['eval'] = []

    # setup controller
    
    for table in classes:
        listenQueue[table.lower()] = []

    # setup configured modules
    
    if Config['Modules']['modules']:
        processorToUse = [elem.strip() for elem in Config['Modules']['modules'].split(',')]
        processorModules = loadModules(processorToUse, 'modules')   
        for module in processorModules:
            print ("Start module: " + module.name)
            for elem in module.listenTo:
                qName = str(elem) + "_" + str(module.name)
                q[qName] = Queue()
                listenQueue[elem].append(q[qName])
                qList.append(qName)
            thread = module.PlugIn(q, dbs)
            threads.append(thread)
            modules.append(thread)
            moduleList.append(module)


    if Config['Modules']['interfaces']:
        producersToUse = [elem.strip() for elem in Config['Modules']['interfaces'].split(',')]
        producerModules = loadModules(producersToUse, 'interfaces')
        for module in producerModules:
            print ("Start module: " + module.name)
            for elem in module.listenTo:
                qName = str(elem) + "_" + str(module.name)
                q[qName] = Queue()
                listenQueue[elem].append(q[qName])
            thread = module.PlugIn(dbs,q)
            threads.append(thread)

    if backend == 'orient':
        logger.info("Start Orient Controller ... ")
        controller = ControllerOrient.Controller(listenQueue, dbs)
    elif backend == 'psql':
        logger.info("Start Postgres Controller ... ")
        controller = ControllerPsql.Controller(listenQueue, dbs)
    else:
        logger.error("Unknown Backend %s. Stop Execution.", backend)
        stop(backend, [])
    
    controller.start()

    for thread in threads:
        thread.start()

    threads.append(controller)

    inputListener = inputThread(ownQueue)
    inputListener.start()

    print ("All Modules and Interfaces up")

    while (True):
        value = ownQueue.get()

        if value == "stop":

            print ("User Input Stopped Programm ...")

            if backend == 'orient':
                client.db_close()
            elif backend == 'psql':
                cur.close()
                conn.close()
            else:
                pass

            for thread in threads:
                thread.stop()

            for thread in threads:
                thread.terminate()

            for thread in threads:
                thread.join()

            break

        elif value == "cancel":
   
            print ("Got Cancel Request ...")

            logger.info("Stop Controller")
            controller.stop()
    
            logger.info("Running threads: %s", threads)
            logger.info("Stop Modules: %s", modules)
            for module in modules:
                module.stop()
                print ("Stopped Module: ", module)
            for module in modules:
                module.terminate()
                print ("Terminated Module: ", module)
            for module in modules:
                module.join()
                print ("Joined Module: ", module)
            modules = []

            threads = [t for t in threads if t.is_alive()]
            logger.info("Running threads: %s", threads)

            gc.collect()

            feedbackQueue.put("cancelSuccess")


        elif value == "restart":
            
            print ("Got Restart Request ... ")
            
            logger.info("Flush queues ... ")

            for elem in qList:
                queue = q[elem]
                tableName = str(elem.split("_")[0])
                listenQueue[tableName].remove(queue)
                q[elem] = Queue()
                listenQueue[tableName].append(q[elem])

            if backend == 'orient':
                logger.info("Start Orient Controller ... ")
                controller = ControllerOrient.Controller(listenQueue, dbs)
            elif backend == 'psql':
                logger.info("Start Postgres Controller ... ")
                controller = ControllerPsql.Controller(listenQueue, dbs)
            else:
                logger.error("Unknown Backend %s. Stop Execution.", backend)
                stop(backend, [])
            
            controller.start()
            threads.append(controller)

            logger.info("Restart modules: %s", moduleList)

            for module in moduleList:
                logger.info("Restart module: %s", module.name)
                thread = module.PlugIn(q, dbs)
                threads.append(thread)
                modules.append(thread)
            
            for module in modules:
                module.start()

            threads = [t for t in threads if t.is_alive()]
            logger.info("Running Threads: %s", threads)

            gc.collect()

            feedbackQueue.put("restartSuccess")

        else:
            logger.error("Error unknown Value in Queue: ", value)
    
        
    
    

