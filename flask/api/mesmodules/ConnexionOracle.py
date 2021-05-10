# coding: utf-8

import os
import sys
import cx_Oracle  


os.environ['NLS_LANG'] = 'FRENCH_FRANCE.UTF8'
# Définit la variable d'environnement NLS_LANG
# pour utiliser l'encodage UTF-8 côté client


class ConnexionOracle:
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
            dsn_tns = cx_Oracle.makedsn(self.host, self.port, self.database)
            connection = cx_Oracle.connect(self.user, self.password, dsn_tns)

            # Create a cursor to perform database operations
            cursor = connection.cursor()

            # Print PostgreSQL details
            # print("PostgreSQL server information")
            # print(connection.get_dsn_parameters(), "\n")

            # Executing a SQL query
            # cursor.execute("SELECT version();")
            # cursor.execute("select level n from dual connect by level < 10")
            print(cx_Oracle.clientversion())
            cursor.execute("select sysdate from dual")

            # Fetch result
            record = cursor.fetchone()
            print("Oracle Sysdate - ", record, "\n")
            # 4) collecter et afficher le résultat
            # for row in cursor:
            #     print(row)

        # except (Exception, cx_Oracle.DatabaseError) as exc:
        except (cx_Oracle.DatabaseError) as exc:
            print("Error while connecting to Oracle", exc)
            error, = exc.args
            print("Code:     ", error.code, file=sys.stderr)
            # print("Offset    ", error.offset, file=sys.stderr)
            #                           ^^^^^^
            # position de l'erreur dans la requête.
            # sans signification ici
            print("Message   ", error.message.strip(), file=sys.stderr)
            #                                 ^^^^^^^
            #                       élimine la "fin de ligne" (EOL)
            print("Context   ", error.context, file=sys.stderr)
            cx_Oracle.DatabaseError
        finally:
            if (connection):
                cursor.close()
                connection.close()
                print("Oracle connection is closed")

    def testconnexion(self):
        """
        testconnexion: permet de tester la connexion  à la base de donnée
        """

        connection = None
        cursor = None
        try:
            dsn_tns = cx_Oracle.makedsn(self.host, self.port, self.database)
            connection = cx_Oracle.connect(self.user, self.password, dsn_tns)

            cursor = connection.cursor()

            cursor.execute("select sysdate from dual")

            record = cursor.fetchone()

            if (record):
                return {"connexion": "OK"}
            else:
                return {"connexion": "NOK"}

        except (cx_Oracle.DatabaseError) as exc:
            print("Error while connecting to Oracle", exc)
            error, = exc.args
            print("Code:     ", error.code, file=sys.stderr)
            print("Message   ", error.message.strip(), file=sys.stderr)
            print("Context   ", error.context, file=sys.stderr)
            cx_Oracle.DatabaseError
            return {"connexion": error.message.strip()}
        finally:
            if (connection):
                cursor.close()
                connection.close()

    def testrequete(self, sqlstr):
        """
        testrequete: permet de tester la requete // voir si elle ramende des donnees
        """

        connection = None
        cursor = None
        try:
            dsn_tns = cx_Oracle.makedsn(self.host, self.port, self.database)
            connection = cx_Oracle.connect(self.user, self.password, dsn_tns)
            cursor = connection.cursor()

            # sqlstr = """+ sql """
            cursor.execute(sqlstr)

            # record = cursor.fetchone()
            record = cursor.fetchall()
            nbre = len(record)

            return {"nombreligne": nbre}

        except (cx_Oracle.DatabaseError) as exc:
            print("Error while connecting to Oracle DB", exc)
            error, = exc.args
            print("Code:     ", error.code, file=sys.stderr)
            print("Message   ", error.message.strip(), file=sys.stderr)
            print("Context   ", error.context, file=sys.stderr)
            cx_Oracle.DatabaseError
            return {"erreur": error.message.strip()}
        finally:
            if (connection):
                cursor.close()
                connection.close()
