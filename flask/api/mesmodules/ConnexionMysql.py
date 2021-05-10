# coding: utf-8

# import os
import sys

import mysql.connector
from mysql.connector import Error, Warning


class ConnexionMysql:
    """
    docstring
    """

    def __init__(self, user, password, host, port, database):
        """ contructeur. initialisation: initialiser la classe """
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database

        print(__name__)
        print(self.user, self.password, self.host, self.port, self.database)

    def connexion(self):
        """
        connexion: permet de se connecter à la base de donnée
        """
        print(__name__)

        try:
            # Connect to an existing database
            connection = mysql.connector.connect(
                host=self.host, user=self.user, password=self.password, database=self.database
            )

            # if connection.is_connected():
            db_Info = connection.get_server_info()
            msgInfo = "Connected to MySQL Server version : {}".format(
                db_Info)
            # logging.info(msgInfo)
            # print("Connected to MySQL Server version ", db_Info)

            # Create a cursor to perform database operations
            cursor = connection.cursor()

            # Executing a SQL query
            cursor.execute("select database();")

            # 4) collecter et afficher le résultat
            record = cursor.fetchone()
            # print("You're connected to database: {}".format(record))
            msgInfo = "You're connected to database: {}".format(record)
            # logging.info(msgInfo)
            print(msgInfo)
            # return connection
            # else:
            # return None
            # None

            return connection

        except (Error) as exc:
            print("Error while connecting to Mysql", exc)
            error, = exc.args
            print("Code:     ", error.code, file=sys.stderr)
            print("Message   ", error.message.strip(), file=sys.stderr)
            print("Context   ", error.context, file=sys.stderr)
        # finally:
            # if (connection):
            #     cursor.close()
            #     connection.close()
            #     print("Mysql connection is closed")

    def testconnexion(self):
        """
        connexion: permet de tester la connexion à la base de donnée
        """
        # print(__name__)

        connection = None
        cursor = None

        try:
            # Connect to an existing database
            connection = mysql.connector.connect(
                host=self.host, user=self.user, password=self.password, database=self.database
            )

            # if connection.is_connected():
            db_Info = connection.get_server_info()
            msgInfo = "Connected to MySQL Server version : {}".format(
                db_Info)

            cursor = connection.cursor()

            cursor.execute("select database();")

            record = cursor.fetchone()
            msgInfo = "You're connected to database: {}".format(record)
            print(msgInfo)
            res = None
            if(record):
                res = {"connexion": "OK"}
            else:
                res = {"connexion": "NOK"}
            return res

            # return connection

        except (Error, Warning) as exc:
            print("Error while connecting to Mysql", exc)
            # error, = exc.args
            # print("Code:     ", error.code, file=sys.stderr)
            # print("Message   ", error.message.strip(), file=sys.stderr)
            # print("Context   ", error.context, file=sys.stderr)
            res = {"connexion": exc.args[1]}
            return res
        finally:
            if (connection):
                cursor.close()
                connection.close()
                print("Mysql connection is closed")

    def testrequete(self, sqlstr):
        """
            connexion: permet de tester la requete
            """
        # print(__name__)

        connection = None
        cursor = None

        try:
            # Connect to an existing database
            connection = mysql.connector.connect(
                host=self.host, user=self.user, password=self.password, database=self.database
            )

            # if connection.is_connected():
            db_Info = connection.get_server_info()
            msgInfo = "Connected to MySQL Server version : {}".format(
                db_Info)

            cursor = connection.cursor()

            cursor.execute(sqlstr)

            record = cursor.fetchall()
            nbre = len(record)

            return {"nombreligne": nbre}

            # return connection

        except (Error, Warning) as exc:
            print("Error while connecting to Mysql", exc)
            # error, = exc.args
            # print("Code:     ", error.code, file=sys.stderr)
            # print("Message   ", error.message.strip(), file=sys.stderr)
            # print("Context   ", error.context, file=sys.stderr)
            res = {"connexion": exc.args[1]}
            # return {"erreur": str(exc)}
            return {"erreur": str(exc.args[1])}
        finally:
            if (connection):
                cursor.close()
                connection.close()
                print("Mysql connection is closed")
