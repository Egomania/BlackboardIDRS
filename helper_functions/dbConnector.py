import logging
import psycopg2
import psycopg2.extensions
import pyorient

logger = logging.getLogger("idrs")

def cancelQuery(module):
    if module.dbs.backend == 'psql':
        module.DBconnect.cancel()
    elif module.dbs.backend == 'orient':
        pass
    else:
        logger.error("Unknown Backend: %s", module.dbs.backend)

def connectToDB(module):
    if module.dbs.backend == 'psql':
        module.DBconnect = psycopg2.connect(database=module.dbs.database, user=module.dbs.user, password=module.dbs.pwd, port=module.dbs.port, host=module.dbs.server)
        module.insert = module.DBconnect.cursor()
    elif module.dbs.backend == 'orient':
        module.insert = pyorient.OrientDB(module.dbs.server, module.dbs.port)
        module.DBconnect = module.insert.connect(module.dbs.user, module.dbs.pwd)
        module.insert.db_open(module.dbs.database, module.dbs.user, module.dbs.pwd)
    else:
        logger.error("Unknown Backend: %s", module.dbs.backend)

def connectToDBTemp(module):
    if module.dbs.backend == 'psql':
        DBconnect = psycopg2.connect(database=module.dbs.database, user=module.dbs.user, password=module.dbs.pwd, port=module.dbs.port, host=module.dbs.server)
        insert = DBconnect.cursor()
    elif module.dbs.backend == 'orient':
        insert = pyorient.OrientDB(module.dbs.server, module.dbs.port)
        DBconnect = insert.connect(module.dbs.user, module.dbs.pwd)
        insert.db_open(module.dbs.database, module.dbs.user, module.dbs.pwd)
    else:
        logger.error("Unknown Backend: %s", module.dbs.backend)
    return (DBconnect, insert)

def reconnectToDB(module, commit=False):
    disconnectFromDB(module, commit)
    connectToDB(module)

def disconnectFromDB(module, commit=False):
    if module.dbs.backend == 'psql':
        if commit:
	        module.DBconnect.commit()
        module.insert.close()
        module.DBconnect.close()
    elif module.dbs.backend == 'orient':
        module.insert.db_close()
    else:
        pass

def disconnectFromDBTemp(module, DBconnect, insert, commit=False):
    if module.dbs.backend == 'psql':
        if commit:
	        DBconnect.commit()
        insert.close()
        DBconnect.close()
    elif module.dbs.backend == 'orient':
        insert.db_close()
    else:
        pass

