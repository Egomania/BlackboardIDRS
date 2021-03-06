import logging
import ast
import select
import time

from multiprocessing import Process, Queue
from flask import Flask, request
from flask_restful import Resource, Api

logger = logging.getLogger('idrs')
EVAL = True

def transform(request):

    
    original = {}
    new = {}
    ret = {}
    for key in request.form.keys():
        if key == 'operation':
            ret['operation'] = request.form[key]
        elif key == 'table':
            ret['table'] = request.form[key].lower()
        elif key == 'rid':
            ret['ident'] = request.form[key]
        else:
            if 'original' in key:
                oldKey = key
                original[key.replace("_original","")] = request.form[oldKey]
            else:
                new[key] = request.form[key]

    ret['original'] = original
    ret['new'] = new

    logger.info('New incomming request - %s in %s was %s ', ret['ident'], ret['table'], ret['operation'])

    return (ret)
    

class Input(Resource):
    def __init__ (self, q):
        self.q = q

    def putInQ(self, ret):
        for elem in self.q[ret['table'].lower()]:
            elem.put(ret)
        if EVAL:
            for elem in self.q['eval']:
                #print ("Listener ", ret['table'], ret['operation'], ret['ident'])
                elem.put(ret)

    def put(self):
        ret = transform(request)
        self.putInQ(ret)

    def post(self):
        ret = transform(request)
        self.putInQ(ret)

class Controller (Process):
    def __init__(self, q, dbs):
        Process.__init__(self)
        self.q = q
        self.app = Flask(__name__)
        self.log = logging.getLogger('werkzeug')
        self.log.disabled = True
        self.app.logger.disabled = True
        self.api = Api(self.app)

    def stop(self, timeout=None):
        logger.info( 'Stopping "{0}"'.format(self.__module__) )

    def run(self):
        logger.info( 'Start "{0}"'.format(self.__module__) )
        self.api.add_resource(Input, '/update', resource_class_kwargs={'q': self.q})
        self.app.run(debug=True, port=6666, use_reloader=False)
            

