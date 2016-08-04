import logging

class DatabaseSettings(object):
    def __init__(self, server, port, user, pwd, database, EXAMPLE_DATA, backend, index, policy, delPol, inf, delInf):
        self.server = server
        self.port = port 
        self.user = user
        self.pwd = pwd
        self.database = database
        self.EXAMPLE_DATA = EXAMPLE_DATA
        self.backend = backend
        self.index = index
        self.policy = policy
        self.deletePolicy = delPol
        self.infrastructure = inf
        self.deleteInfrastructure = delInf

    def printSettings(self):
        print ("Server=",self.server, " Port=",self.port, " User=",self.user, " Pwd=",self.pwd, " Database=",self.database, " Backend=",self.backend, " Index=", self.index)

    def logSettings(self, logger):
        logger.info("Server=%s Port=%s User=%s Pwd=%s Database=%s Backend=%s Index=%s",self.server, self.port, self.user, self.pwd, self.database, self.backend, self.index)
        

        
