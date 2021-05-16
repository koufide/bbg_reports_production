# coding: utf-8

import pyodbc
import sys
import os

import mysql.connector
from mysql.connector import Error


class ConnexionSqlServer:
    # print("__ConnexionSqlServer__")
    """
    docstring
    """

    # def __init__(self):
    # """ contructeur. initialisation: initialiser la classe """
    # print(__name__)

    def __init__(self, user, password, host, port, database):
        """ contructeur. initialisation: initialiser la classe """
        self.username = user
        self.password = password
        self.server = host
        self.port = port
        self.database = database
        self.driver = '{ODBC Driver 17 for SQL Server}'

    #     # self.host = "{},{}".format(self.host, self.port)
    #     # self.host = "{},{}".format(self.host, self.port)

    #     print(__name__)
    #     print(self.user, self.password, self.host, self.port, self.database)

        # connection = None
        # cursor = None
        # try:
        #     # driver = '{ODBC Driver 17 for SQL Server}'
        #     # driver = '{SQL Server}'
        #     # driver = '{SQL Server Native Client 11.0}'

        #     connection = pyodbc.connect(
        #         'DRIVER='+self.driver+';SERVER='+self.server + ';DATABASE='+self.database+';UID='+self.username+';PWD=' + self.password)

        #     # cursor = connection.cursor()
        #     # cursor.execute("SELECT @@version;")

        #     # record = cursor.fetchone()
        #     # print("record: {}".format(record))
        #     # if (record):
        #     #     return {"connexion": "OK"}
        #     # else:
        #     #     return {"connexion": "NOK"}

        #     return connection

        # except (Error) as exc:
        #     print("Error while connecting to Sqlserver", exc)
        #     error, = exc.args
        #     print("Code:     ", error.code, file=sys.stderr)
        #     print("Message   ", error.message.strip(), file=sys.stderr)
        #     print("Context   ", error.context, file=sys.stderr)
        #     # return {"erreur": error.message.strip()}
        #     return None
        # finally:
        #     if (connection):
        #         cursor.close()
        #         connection.close()

    def connexion(self):
        """
        connexion: permet de se connecter à la base de donnée
        """
        # print(__name__)

        try:
            # Connect to an existing database
            # driver = '{ODBC Driver 17 for SQL Server}'
            # driver = '{SQL Server}'
            connection = pyodbc.connect(
                'DRIVER='+self.driver+';SERVER='+self.server + ';DATABASE='+self.database+';UID='+self.username+';PWD=' + self.password)


            # if connection.is_connected():
            # db_Info = connection.get_server_info()
            # msgInfo = "Connected to MySQL Server version : {}".format(
            #     db_Info)
            # logging.info(msgInfo)
            # print("Connected to MySQL Server version ", db_Info)

            # Create a cursor to perform database operations
            cursor = connection.cursor()

            # Executing a SQL query
            # cursor.execute("select database();")
            cursor.execute("SELECT @@version;")

            # 4) collecter et afficher le résultat
            row = cursor.fetchone()
            # print(row)

            return connection
            # while row:
            #     print(row[0])
            #     row = cursor.fetchone()

            # cursor.execute("SELECT * from etudiant;")
            # for row in cursor:
            #     print(row)

        except (Error) as exc:
            print("Error while connecting to Sqlserver", exc)
            error, = exc.args
            print("Code:     ", error.code, file=sys.stderr)
            print("Message   ", error.message.strip(), file=sys.stderr)
            print("Context   ", error.context, file=sys.stderr)
            return None
        # finally:
        #     if (connection):
        #         cursor.close()
        #         connection.close()
            # print("Sqlserver connection is closed")

    # def testconnexion(self):
    #     """
    #     connexion: permet de tester la connexion à la base de donnée
    #     """
    #     connection = None
    #     cursor = None
    #     try:
    #         driver = '{ODBC Driver 17 for SQL Server}'

    #         driver = '{SQL Server}'
    #         # connection = pyodbc.connect('DRIVER='+driver+';SERVER=' + self.host + ';DATABASE='+self.database+';UID='+self.user+';PWD=' + self.password)

    #         # server = '192.168.56.1'
    #         # database = 'BBG_ReportSharePoint'
    #         # username = 'sa'
    #         # password = 'Bbgci2020'
    #         # connection = pyodbc.connect(
    #         #     'DRIVER={SQL Server};SERVER='+server + ';DATABASE='+database+';UID='+username+';PWD=' + password)

    #         cursor = connection.cursor()
    #         cursor.execute("SELECT @@version;")

    #         record = cursor.fetchone()
    #         if (record):
    #             return {"connexion": "OK"}
    #         else:
    #             return {"connexion": "NOK"}

    #     except (Error) as exc:
    #         print("Error while connecting to Sqlserver", exc)
    #         error, = exc.args
    #         print("Code:     ", error.code, file=sys.stderr)
    #         print("Message   ", error.message.strip(), file=sys.stderr)
    #         print("Context   ", error.context, file=sys.stderr)
    #         return {"connexion": error.message.strip()}
    #     finally:
    #         if (connection):
    #             cursor.close()
    #             connection.close()

    # def testconnexion(self, username, password, server, port, database):
    def testconnexion(self):
        # print("__testconnexion__")
        """
        connexion: permet de tester la connexion à la base de donnée
        """
        connection = None
        cursor = None
        try:
            # driver = '{ODBC Driver 17 for SQL Server}'
            # driver = '{SQL Server}'
            # driver = '{SQL Server Native Client 11.0}'

            connection = pyodbc.connect(
                'DRIVER='+self.driver+';SERVER='+self.server + ';DATABASE='+self.database+';UID='+self.username+';PWD=' + self.password)

            cursor = connection.cursor()
            cursor.execute("SELECT @@version;")

            record = cursor.fetchone()
            # print("record: {}".format(record))
            if (record):
                return {"connexion": "OK"}
            else:
                return {"connexion": "NOK"}

        except (Error) as exc:
            print("Error while connecting to Sqlserver", exc)
            error, = exc.args
            print("Code:     ", error.code, file=sys.stderr)
            print("Message   ", error.message.strip(), file=sys.stderr)
            print("Context   ", error.context, file=sys.stderr)
            return {"erreur": error.message.strip()}
        finally:
            if (connection):
                cursor.close()
                connection.close()

    def testrequete(self, sqlstr):
        # print("__testrequete__>__")
        """
        connexion: permet de tester la requete 
        """
        connection = None
        cursor = None
        try:
            # driver = '{ODBC Driver 17 for SQL Server}'
            # driver = '{SQL Server}'
            # driver = '{SQL Server Native Client 11.0}'

            # print(driver, server, database, username, password)

            connection = pyodbc.connect(
                'DRIVER='+self.driver+';SERVER='+self.server + ';DATABASE='+self.database+';UID='+self.username+';PWD=' + self.password)

            cursor = connection.cursor()
            cursor = connection.cursor()
            cursor.execute(sqlstr)

            record = cursor.fetchall()
            nbre = len(record)

            return {"nombreligne": nbre}

        except (Error) as exc:
            print("-----------une erreur")
            print("Error while connecting to Sqlserver", exc)
            error, = exc.args
            print("Code:     ", error.code, file=sys.stderr)
            print("Message   ", error.message.strip(), file=sys.stderr)
            print("Context   ", error.context, file=sys.stderr)
            # return {"erreur": error.message.strip()}

            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            erreur = "{} , {}, {} ".format(exc_type, fname, exc_tb.tb_lineno)
            return {"erreur": erreur}

            # return {"erreur": error.message.strip()}
        finally:
            if (connection):
                cursor.close()
                connection.close()





# if(__name__ == "__main__"):
#     print(__name__)

#     server = '192.168.56.1'
#     database = 'BBG_ReportSharePoint'
#     username = 'sa'
#     password = 'Bbgci2020'
#     port = 1433

#     # server = '192.168.200.104'
#     # database = 'ImageCheque'
#     # username = 'sa'
#     # password = 'stcesa'
#     # port = 1433

#     server2 = "{},{}".format(server, port)

#     cnx = ConnexionSqlServer()
#     # cnx.testconnexion2(username, password, server, port, database)
#     cnx.testconnexion2(username, password, server2, port, database)
