# coding: utf-8

# from app import connexiondb
import psycopg2
import psycopg2.extras
from psycopg2 import Error


import sys

from xml.etree import ElementTree


class ConnexionPostgres:
    """
    docstring
    """
    # host = ''

    def __init__(self, user, password, host, port, database):

        # def __init__(self):
        """ contructeur. initialisation: initialiser la classe """
        print("contructeur __init__")
        print(__name__)
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database

    def connexion(self):
        """
        connexion: permet de se connecter à la base de donnée
     """
        print(__name__)
        print("ConnexionPostgres connexion::::")
        # print(ConnexionPostgres.host)
        print(self.host)

        try:
            # Connect to an existing database
            connection = psycopg2.connect(user=self.user,
                                          password=self.password,
                                          host=self.host,
                                          port=self.port,
                                          database=self.database)
            # Create a cursor to perform database operations
            cursor = connection.cursor()
            # Print PostgreSQL details
            print("PostgreSQL server information")
            print(connection.get_dsn_parameters(), "\n")
            # Executing a SQL query
            cursor.execute("SELECT version();")
            # Fetch result
            record = cursor.fetchone()
            print("You are connected to - ", record, "\n")

            return connection

        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL", error)
        # finally:
        #     if (connection):
        #         cursor.close()
        #         connection.close()
        #         print("PostgreSQL connection is closed")

    def testconnexion(self, user, password, host, port, database):
        """
        connexion: permet de se connecter à la base de donnée
        """

        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(user=user,
                                          password=password,
                                          host=host,
                                          port=port,
                                          database=database)

            cursor = connection.cursor()
            cursor.execute("SELECT version();")
            record = cursor.fetchone()
            print("You are connected to - ", record, "\n")
            print("record: {}".format(record))
            if (record):
                return {"connexion": "OK"}
            else:
                return {"connexion": "NOK"}

        except (Exception, Error) as exc:
            print("<= Error while connecting to PostgreSQL", exc)

            # error, = exc.args
            # print(exc)
            # print("Code:     ", error.code, file=sys.stderr)
            # print("Message   ", error.message.strip(), file=sys.stderr)
            # print("Context   ", error.context, file=sys.stderr)
            return {"connexion": str(exc)}

        finally:
            if (connection):
                cursor.close()
                connection.close()

    def testrequete(self, sqlstr):
        """
        connexion: permet la requete
        """

        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(user=self.user,
                                          password=self.password,
                                          host=self.host,
                                          port=self.port,
                                          database=self.database)

            cursor = connection.cursor()

            cursor.execute(sqlstr)

            record = cursor.fetchall()
            nbre = len(record)

            return {"nombreligne": nbre}

        except (Exception, Error) as exc:
            print("<= Error while connecting to PostgreSQL", exc)

            # error, = exc.args
            # print("error: {}".format(error))

            # print("Code:     ", error.code, file=sys.stderr)
            # print("Message   ", error.message.strip(), file=sys.stderr)
            # print("Context   ", error.context, file=sys.stderr)

            return {"erreur": str(exc)}
            # return {"erreur": exc.message.strip()}

        finally:
            if (connection):
                cursor.close()
                connection.close()

    def generatexmlfile(self, Courant, nom_fic_droits):
        """
        creer xml file 
        """

        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(user=self.user,
                                          password=self.password,
                                          host=self.host,
                                          port=self.port,
                                          database=self.database)

            cursor = connection.cursor()

            # liste des processus
            sqlstr = "select * from processus"
            sqlstr = """
            select p.nom, r.rep_destination , u.samaccount_name
            from processus p, requete r, requete_utilisateur ru, utilisateur u
            where r.processus_id=p.id 
                and ru.requete_id=r.id and ru.utilisateur_id=u.id
            order by p.nom
            """

            sqlstr = """
            select distinct p.nom, p.id
            from processus p, requete r, requete_utilisateur ru, utilisateur u
            where r.processus_id=p.id and ru.requete_id=r.id 
            and ru.utilisateur_id=u.id
            order by p.nom
            """

            cursor.execute(sqlstr)

            recordProcessus = cursor.fetchall()
            nbre = len(recordProcessus)
            print(nbre)

            if nbre > 0:

                tree = ElementTree.ElementTree()
                Securities = ElementTree.Element("Securities")
                Securities.text = "\n"

                for r in recordProcessus:
                    # print(r[1])
                    nomProcessus = r[0]
                    # print(nomProcessus+" ---")
                    idProcessus = r[1]

                    # liste des requetes du processus
                    sqlstr = """
                    select r.rep_destination, r.id from requete r where r.processus_id = (%s)
                    """

                    cursor.execute(sqlstr, (idProcessus,))

                    recordRequete = cursor.fetchall()
                    nbre = len(recordRequete)

                    for re in recordRequete:

                        listUsers = ""

                        # print(re)
                        repDestination = re[0]
                        # print(nomProcessus+"/"+repDestination+" --- ---")
                        idRequete = re[1]

                        sqlstr = """
                        select u.samaccount_name
                        from requete_utilisateur ru, utilisateur u
                        where ru.requete_id=(%s) and u.id=ru.utilisateur_id
                        order by u.samaccount_name
                        """
                        cursor.execute(sqlstr, (idRequete,))
                        recordUtilisateur = cursor.fetchall()

                        nbre = len(recordUtilisateur)
                        if nbre > 0:

                            for u in recordUtilisateur:
                                # print(u[0])
                                if (len(str(listUsers).strip())) == 0:
                                    listUsers = u[0]
                                else:
                                    if (len(str(u[0]).strip())) != 0:
                                        listUsers = listUsers + ","+u[0]

                        # print(nomProcessus+" ---"+listUsers)
                        Security = ElementTree.Element("Security")
                        Security.set('Role', listUsers)
                        Security.set('Directory', nomProcessus)
                        Security.tail = "\n"
                        Securities.append(Security)

                        Security = ElementTree.Element("Security")
                        Security.set('Role', listUsers)
                        Security.set('Directory', nomProcessus +
                                     "/"+repDestination)
                        Security.tail = "\n"
                        Securities.append(Security)

                        # print(nomProcessus+"/"+repDestination +                               " --- ---"+listUsers)
                        # print(listUsers)

                tree._setroot(Securities)
                # tree.write(Courant+"/Droit.xml", encoding="utf-8", xml_declaration=True)
                tree.write(Courant+"/"+nom_fic_droits, encoding="utf-8", xml_declaration=True)

            return {"createxmlfile": "OK"}

        except (Exception, Error) as exc:
            print("<= Error while connecting to PostgreSQL", exc)

            # error, = exc.args
            # print("error: {}".format(error))
            # print("Code:     ", error.code, file=sys.stderr)
            # print("Message   ", error.message.strip(), file=sys.stderr)
            # print("Context   ", error.context, file=sys.stderr)

            return {"erreur": str(exc)}
            # return {"erreur": exc.message.strip()}

        finally:
            if (connection):
                cursor.close()
                connection.close()

    def getAllConnexionsDB(self):
        print(__name__)
        print("__getAllConnexionsDB__")

        cnx = None
        curseur = None

        try:
            cnx = self.connexion()
            curseur = cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)

            curseur.execute("SELECT * from connexion;")
            record = curseur.fetchall()
            # print("You are connected to - ", record, "\n")
            # it calls .fetchone() in loop
            # res = [json.dumps(dict(record)) for record in curseur]

            return record

        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL", error)
        finally:
            # self.closeCurseur(curseur)
            # self.closeConnexion(cnx)
            if (cnx):
                curseur.close()
                cnx.close()
            # print("PostgreSQL connection is closed")

    def getDeclencheurs(self):
        print(__name__)
        print("__getDeclencheur__")

        # from datetime import datetime

        import psycopg2.extras

        connection = psycopg2.connect(user=self.user,
                                          password=self.password,
                                          host=self.host,
                                          port=self.port,
                                          database=self.database)

        cursor = connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
        # cursor = connection.cursor()

        sqlstr="""
        SELECT rd.requete_id, rd.declencheur_id, d.date_exec, d.heure_exec, d.frequence, d.mois, d.jours_du_mois,
            r.libelle libelle_req, r.rep_destination, r.sqlstr,
            c.serveur, c.port, c.basedonnees, c.utidb, c.passdb, c.nom connexion_nom,
            tc.code typeconnexion_code, tc.libelle typeconnexion_libelle , p.nom processus_nom
        FROM requete_declencheur rd, declencheur d, requete r, connexion c, type_connexion tc, processus p
        WHERE d.id=rd.declencheur_id AND r.id=rd.requete_id AND c.id=r.connexion_id
            AND tc.id=c.type_connexion_id AND p.id=r.processus_id
        ORDER BY d.frequence
        """
        cursor.execute(sqlstr)

        record = cursor.fetchall()
        nbre = len(record)
        print("nbre de declencheurs: {}".format(nbre))
        # print(self.user,self.password,self.host,self.port,self.database)
        # exit(1)
        return record

        



# if(__name__ == "__main__"):
#     print(__name__)

#     user = "postgres"
#     password = "postgres"
#     host = "192.168.56.220"
#     port = "5432"
#     database = "reportsdb"

#     cnx = ConnexionPostgres(user, password, host, port, database)
#     # cnx.testconnexion(user, password, host, port, database)

#     # cnx.createxmlfile()
