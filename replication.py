import psycopg2
import logging
from creatingJson import *

logging.basicConfig(level= logging.INFO, filename='database.log',filemode='a',
                    format= '%(asctime)s - %(message)s')

def connectServer():
    hostnameServer = '192.168.0.253'
    databaseServer = 'replica_test'
    usernameServer = 'postgres'
    pwdServer = '*Total&2023'
    port_idServer = 5433
    connServer = psycopg2.connect(host = hostnameServer,
                            dbname = databaseServer,
                            user = usernameServer,
                            password = pwdServer,
                            port = port_idServer)
    return connServer

def connectLocal():
    hostnameLocal = 'localhost'
    databaseLocal = 'test'
    usernameLocal = 'postgres'
    pwdLocal = '1234'
    port_idLocal = 5433
    connLocal = psycopg2.connect(
            host = hostnameLocal,
            dbname = databaseLocal,
            user = usernameLocal,
            password = pwdLocal,
            port = port_idLocal) 
    return connLocal

def reading_local(database):
    try:
        if database == 'local':
            conn = connectLocal()
            cursor = conn.cursor()
        else:
            conn = connectServer()
            cursor = conn.cursor()

        cursor.execute("SELECT * FROM pessoas where REPLICADO = 'N'")

        if not cursor.fetchone:
            print('Nenhuma operação pendente')
        values = []
        for row in cursor:
            values.append(row)
        conn.close()
        tupleValues = tuple(values)
        return tupleValues

    except Exception as E:
        logging.error(E)
        exit(0)

def checkingData(cursor,id):
    try:
        cursor.execute(f'SELECT id FROM pessoas WHERE id = {id}')
        result = cursor.fetchone()
        if result:
            return True
        else:
            return False
    except Exception as E:
        logging.error(E)

def updatingData(values, destination, connServer, connLocal,structure):
    cursorLocal = connLocal.cursor()
    cursorServer = connServer.cursor()
    try:
        if destination == 'server':
            for record in values:
                if checkingData(cursorServer,record[0]):
                    cursorServer.execute(f"""UPDATE pessoas SET nome = '{record[1]}',
                                                            pais = '{record[2]}',
                                                            replicado = 'S',
                                                            ativo = '{record[4]}'
                                                            where id = {record[0]}""")
                    connServer.commit()
                    logging.info(f"registro {record} alterado no banco Servidor!")
                    #Creating local connection to change column "replicado"
                    cursorLocal.execute(f"""UPDATE pessoas SET replicado = 'S'
                                            where id = {record[0]}""")
                    connLocal.commit()
                    logging.info('Atributo "replicado" alterado no banco Local')
                    
                else:
                    cursorServer.execute(f"""INSERT INTO pessoas (nome,pais,replicado)
                                        VALUES ('{record[1]}','{record[2]}','{'S'}')""")
                    connServer.commit()
                    logging.info(f"registro {record} criado no banco Servidor!")

                    #Creating local connection to change column "replicado"
                    cursorLocal.execute(f"""UPDATE pessoas SET replicado = 'S'
                                        WHERE id = {record[0]}""")
                    connLocal.commit()
                    logging.info('Atributo "replicado" alterdo no banco local.')
            connLocal.close()
            connServer.close()
        else:
            for record in values:
                if checkingData(cursorLocal,record[0]):
                    cursorLocal.execute(f"""UPDATE pessoas SET nome = '{record[1]}',
                                                            pais = '{record[2]}',
                                                            replicado = 'S',
                                                            ativo = '{record[4]}'
                                                            where id = {record[0]}""")
                    connLocal.commit()
                    logging.info(f"Registro {record} alterado no banco Local!")
                    #Creating Server connection to change column "replicado"
                    cursorServer.execute(f"""UPDATE pessoas SET replicado = 'S'
                                            where id = {record[0]}""")
                    connServer.commit()
                    logging.info('Atributo "replicado" alterado no banco server!')
                else:
                    cursorLocal.execute(f"""INSERT INTO pessoas (nome,pais,replicado)
                                        VALUES ('{record[1]}','{record[2]}','{'S'}')""")
                    connLocal.commit()
                    logging.info(f"Registro {record} criado no banco Local!")

                    #Creating Server connection to change column "replicado"
                    cursorServer.execute(f"""UPDATE pessoas SET replicado = 'S'
                                        WHERE id = {record[0]}""")
                    connServer.commit()
                    logging.info('Atributo "replicado" alterdo no banco Servidor.')
            connLocal.close()
            connServer.close()
    except Exception as E:
        logging.error(E)
        exit(0)

def copyingToDestiny(destination,values):
    #conn Local
    connLocal = connectLocal()
    #conn Server
    connServer = connectServer()
    
    structure = separatingData()
    try:
        if destination == 'server':
            updatingData(values, 'server', connServer, connLocal,structure)
        else:
            updatingData(values, 'local', connServer, connLocal,structure)
    except Exception as E:
        logging.error(E)
        exit(0)
