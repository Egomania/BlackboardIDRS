import logging
import ast
import json
import select
import time

import psycopg2
import psycopg2.extensions

from multiprocessing import Process, Queue

logger = logging.getLogger("idrs.controller")
#logger.setLevel(20)
EVAL = True

def transform(request):

    logger.debug('Got NOTIFY: "{0}","{1}","{2}"'.format(request.pid, request.channel, request.payload) )
    s = request.payload
    payload = json.loads(s)
    
    original = {}
    new = {}
    ret = {}
    for key in payload.keys():
        if key == 'type':
            ret['operation'] = payload[key]
        elif key == 'table':
            ret['table'] = payload[key].lower()
        elif key == 'id':
            ret['ident'] = payload[key]
        elif key == 'new':
            for elem in payload[key].keys():
                new[elem] = payload[key][elem]
        elif key == 'original':
            for elem in payload[key].keys():
                original[elem] = payload[key][elem]
        else:
            logger.warning("Unknown Field %s.", key)

    ret['original'] = original
    ret['new'] = new

    logger.debug('New incomming request - %s in %s was %s ', ret['ident'], ret['table'], ret['operation'])

    return (ret)
    

class Controller (Process):
    def __init__(self, q, dbs):
        Process.__init__(self)
        self.q = q
        self.conn = psycopg2.connect(database=dbs.database, user=dbs.user, password=dbs.pwd, port=dbs.port, host=dbs.server)
        self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self.curs = self.conn.cursor()
        self.curs.execute("LISTEN table_update;")

    def stop(self, timeout=None):
        logger.info( 'Stopping "{0}"'.format(self.__module__) )
        self.terminate()

    def putInQ(self, ret):
        for elem in self.q[ret['table'].lower()]:
            logger.debug("Put %s in Queue %s (%s)", ret, elem, ret['table'])
            elem.put(ret)
        if EVAL:
            for elem in self.q['eval']:
                logger.debug("Put %s in Queue %s (eval)", ret, elem)
                elem.put(ret)

    def run(self):
        logger.info( 'Start "{0}"'.format(self.__module__) )
        
        while (True):   
            if select.select([self.conn],[],[],5) == ([],[],[]):
                pass
            else:
                self.conn.poll()
                while self.conn.notifies:
                    notify = self.conn.notifies.pop(0)
                    ret = transform(notify)
                    self.putInQ(ret)

