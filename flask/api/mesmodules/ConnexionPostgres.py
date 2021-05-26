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
        # print("contructeur __init__")
        # print(__name__)
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database

    def connexion(self):
        """
        connexion: permet de se connecter à la base de donnée
     """
        # print(__name__)
        # print("ConnexionPostgres connexion::::")
        # print(ConnexionPostgres.host)
        # print(self.host)

        connection = None
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
            # print("PostgreSQL server information")
            # print(connection.get_dsn_parameters(), "\n")
            # Executing a SQL query
            cursor.execute("SELECT version();")
            # Fetch result
            record = cursor.fetchone()
            # print("You are connected to - ", record, "\n")

            # return connection

        except (Exception, Error) as error:
            print("Error while connecting to PostgreSQL", error)
        # finally:
        #     if (connection):
        #         cursor.close()
        #         connection.close()
        #         print("PostgreSQL connection is closed")
        return connection

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
            # print("You are connected to - ", record, "\n")
            # print("record: {}".format(record))
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
        # print(__name__)
        # print("__getAllConnexionsDB__")

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

    def updateDeclencheur(self, id_requete_declencheur, status):
        # print(__name__)
        # print("__updateDeclencheur__")

        connection = psycopg2.connect(user=self.user,
                                          password=self.password,
                                          host=self.host,
                                          port=self.port,
                                          database=self.database)

        sqlstr = """
        UPDATE requete_declencheur
        SET id=?, note=?, declencheur_id=?, requete_id=?, dern_date_heur_exec=?, proch_date_exec=?, proch_heure_exec=?, status=?
        WHERE id=;
        """

        sqlstr = """
        UPDATE requete_declencheur
        SET status = %s
        WHERE id = %s;
        """
        
        updated_rows = 0

        try:
            cursor = connection.cursor()
            cursor.execute(sqlstr, (status,id_requete_declencheur))
            updated_rows = cursor.rowcount
            connection.commit()
            cursor.close()

        except (Exception, psycopg2.DatabaseError) as erreur:
            print(erreur)
        finally:
            if connection is not None:
                connection.close()
        
        return updated_rows



    def updateDeclencheurV2(self, connection, sets, wheres):
        # print(__name__)
        # print("__updateDeclencheur__")

        # connection = psycopg2.connect(user=self.user,
        #                                   password=self.password,
        #                                   host=self.host,
        #                                   port=self.port,
        #                                   database=self.database)
        sqlstr = """
        UPDATE requete_declencheur
        SET status = %s
        WHERE id = %s;
        """

        sqlstr = """
        UPDATE requete_declencheur
        SET """+sets+"""
        WHERE """+wheres+""";
        """

        print("sqlstr: {}".format(sqlstr))
        # exit(1)

        
        updated_rows = 0

        try:
            cursor = connection.cursor()
            cursor.execute(sqlstr)
            updated_rows = cursor.rowcount
            connection.commit()
            cursor.close()

        except (Exception, psycopg2.DatabaseError) as erreur:
            print(erreur)
        finally:
            if connection is not None:
                connection.close()
        
        return updated_rows

   
    def updateTable(self, connection, table, sets, wheres):
        # print(__name__)
        # print("__updateTable__")

        

        sqlstr = """
        UPDATE """+table+"""
        SET """+sets+"""
        WHERE """+wheres+""";
        """

        print("sqlstr: {}".format(sqlstr))
        # exit(1)

        
        updated_rows = 0

        try:
            cursor = connection.cursor()
            cursor.execute(sqlstr)
            updated_rows = cursor.rowcount
            connection.commit()
            cursor.close()

        except (Exception, psycopg2.DatabaseError) as erreur:
            print(erreur)
        # finally:
        #     if connection is not None:
        #         connection.close()
        
        return updated_rows
   
    def getDeclencheurs(self):
        # print(__name__)
        # print("__getDeclencheur__")

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
        SELECT rd.id, rd.requete_id, rd.declencheur_id, d.date_exec, d.heure_exec, d.frequence, d.mois, d.jours_du_mois,
            r.libelle libelle_req, r.rep_destination, r.sqlstr,
            c.serveur, c.port, c.basedonnees, c.utidb, c.passdb, c.nom connexion_nom,
            tc.code typeconnexion_code, tc.libelle typeconnexion_libelle , p.nom processus_nom
        FROM requete_declencheur rd, declencheur d, requete r, connexion c, type_connexion tc, processus p
        WHERE d.id=rd.declencheur_id AND r.id=rd.requete_id AND c.id=r.connexion_id
            AND tc.id=c.type_connexion_id AND p.id=r.processus_id
        ORDER BY d.frequence
        FOR UPDATE
        """

        sqlstr="""
        SELECT rd.date_proch_exec,
            EXTRACT (YEAR FROM NOW()) AS YEAR,
            EXTRACT (MONTH FROM NOW()) AS MONTH,
            EXTRACT (DAY FROM NOW()) AS DAY,
            rd.id, rd.requete_id, rd.declencheur_id, d.date_exec, d.heure_exec, d.frequence, d.mois, d.jours_du_mois,
                r.libelle libelle_req, r.rep_destination, r.sqlstr,
                c.serveur, c.port, c.basedonnees, c.utidb, c.passdb, c.nom connexion_nom,
                tc.code typeconnexion_code, tc.libelle typeconnexion_libelle , p.nom processus_nom
        FROM requete_declencheur rd, declencheur d, requete r, connexion c, type_connexion tc, processus p
        WHERE d.id=rd.declencheur_id AND r.id=rd.requete_id AND c.id=r.connexion_id
            AND tc.id=c.type_connexion_id AND p.id=r.processus_id
            AND d.frequence='MENSUEL'
            AND (
                    (d.date_exec = TO_CHAR(NOW() :: DATE, 'yyyy-mm-dd') and rd.date_proch_exec is null) or
                    ( rd.date_proch_exec = TO_CHAR(NOW() :: DATE, 'yyyy-mm-dd')  ) 
                )
            AND (rd.status is null or rd.status=0)
        ORDER BY d.frequence
        FOR UPDATE
        """


        cursor.execute(sqlstr)

        record = cursor.fetchall()
        nbre = len(record)
        # print("nbre de declencheurs: {}".format(nbre))
        # print(self.user,self.password,self.host,self.port,self.database)
        # exit(1)
        return record
    
    
    def getDeclencheursMensuel(self, connection):
        print(__name__)
        print("__getDeclencheursMensuel__")

        cursor = connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
        # cursor = connection.cursor()

       

        sqlstr="""
        SELECT  cast(EXTRACT (YEAR FROM NOW()) as varchar) AS anneedb, 
            cast(EXTRACT (MONTH FROM NOW()) as varchar) AS moisdb, 
            cast(EXTRACT (DAY FROM NOW()) as varchar) AS jourdb, 
            rd.date_proch_exec,
            TO_CHAR(NOW() , 'yyyy-mm-dd HH24:MI:SS') now_str,
            rd.id id_rd, rd.requete_id, rd.declencheur_id, d.date_exec, d.heure_exec, d.frequence, 
            d.mois,  array[d.mois] mois_tab,  d.jours_du_mois, array[d.jours_du_mois] jours_du_mois_tab,
            r.libelle libelle, r.rep_destination, r.sqlstr,
            c.serveur, c.port, c.basedonnees, c.utidb, c.passdb, c.nom connexion_nom,
            tc.code, tc.libelle typeconnexion_libelle , p.nom processus
        FROM requete_declencheur rd, declencheur d, requete r, connexion c, type_connexion tc, processus p
        WHERE d.id=rd.declencheur_id AND r.id=rd.requete_id AND c.id=r.connexion_id
            AND tc.id=c.type_connexion_id AND p.id=r.processus_id
            AND d.frequence='MENSUEL'
            AND ( (d.date_exec = TO_CHAR(NOW() :: DATE, 'yyyy-mm-dd') and rd.date_proch_exec is null   AND  
                        TO_CHAR(d.heure_exec::TIME,'HH24:MI') <= TO_CHAR(now()::TIME,'HH24:MI')
                    ) or ( rd.date_proch_exec <= TO_CHAR(NOW(), 'yyyy-mm-dd HH24:MI:SS')  ) )
            AND (rd.status is null or rd.status=0)
        ORDER BY d.frequence
        FOR UPDATE
        """


        cursor.execute(sqlstr)

        record = cursor.fetchall()
        # nbre = len(record)
        # print("nbre de declencheurs: {}".format(nbre))
        # print(self.user,self.password,self.host,self.port,self.database)
        # exit(1)
        return record


    def getDeclencheursJournalier(self, connection):
        print(__name__)
        print("__getDeclencheursJournalier__")

        # from datetime import datetime
        # import psycopg2.extras

        # connection = psycopg2.connect(user=self.user,
        #                                   password=self.password,
        #                                   host=self.host,
        #                                   port=self.port,
        #                                   database=self.database)

        cursor = connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
        # cursor = connection.cursor()

       

        sqlstr="""
        SELECT rd.id id_rd, rd.date_proch_exec, now(),
        TO_TIMESTAMP(rd.date_proch_exec, 'yyyy-mm-dd HH24:MI:SS') date_proch_exec,
        r.libelle, r.rep_destination, r.processus_id, r.sqlstr, r.id id_r,
        d.date_exec, d.heure_exec, d.frequence, repeat_jours,
        c.passdb, c.utidb, c.serveur, c.port, c.basedonnees, tc.code,
        p.nom processus
        FROM public.requete_declencheur rd, requete r, declencheur d, connexion c, type_connexion tc, processus p
        WHERE (rd.status=0 or rd.status is NULL)
        AND d.frequence='JOURNALIER'
        AND ( 
            (rd.date_proch_exec is null AND (d.date_exec = TO_CHAR(NOW() :: DATE, 'yyyy-mm-dd')  
											AND TO_CHAR(d.heure_exec::TIME,'HH24:MI') <= TO_CHAR(now()::TIME,'HH24:MI')
											) ) or 
            ( TO_TIMESTAMP(rd.date_proch_exec, 'yyyy-mm-dd HH24:MI:SS')  <= NOW() ) 
        )
        AND d.id=rd.declencheur_id
        AND r.id=rd.requete_id
		AND c.id=r.connexion_id AND tc.id=c.type_connexion_id
        AND p.id=r.processus_id
        FOR UPDATE
        """


        cursor.execute(sqlstr)

        record = cursor.fetchall()
        # nbre = len(record)
        # print("nbre de declencheurs: {}".format(nbre))
        # print(self.user,self.password,self.host,self.port,self.database)
        # exit(1)
        return record

    def getDeclencheursHebdomadaire(self, connection):
        print(__name__)
        print("__getDeclencheursHebdomadaire__")


        cursor = connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
        # cursor = connection.cursor()

       

        sqlstr="""
        SELECT 
        array[d.jours] jours_tab,
        cast(EXTRACT (DOW FROM NOW()) as int) AS dowdb, 
        EXTRACT (YEAR FROM NOW()) AS anneedb, 
        EXTRACT (MONTH FROM NOW()) AS moisdb, 
        EXTRACT (DAY FROM NOW()) AS jourdb,
            d.jours, d.repeat_semaines,
            rd.id id_rd, rd.date_proch_exec, now(),
            TO_TIMESTAMP(rd.date_proch_exec, 'yyyy-mm-dd HH24:MI:SS') date_proch_exec,
            r.libelle, r.rep_destination, r.processus_id, r.sqlstr, r.id id_r,
            d.date_exec, d.heure_exec, d.frequence, repeat_jours,
            c.passdb, c.utidb, c.serveur, c.port, c.basedonnees, tc.code,
            p.nom processus
            FROM public.requete_declencheur rd, requete r, declencheur d, connexion c, type_connexion tc, processus p
            WHERE (rd.status=0 or rd.status is NULL)
            AND d.frequence='HEBDOMADAIRE'
            AND ( 
                    (
                        rd.date_proch_exec is null 
                        AND (
                                d.date_exec = TO_CHAR(NOW() :: DATE, 'yyyy-mm-dd')  
                                AND TO_CHAR(d.heure_exec::TIME,'HH24:MI') <= TO_CHAR(now()::TIME,'HH24:MI')
                        ) 
                    ) 
                or ( TO_TIMESTAMP(rd.date_proch_exec, 'yyyy-mm-dd HH24:MI:SS')  <= NOW() ) 
            )
            AND d.id=rd.declencheur_id 
            AND r.id=rd.requete_id
            AND c.id=r.connexion_id 
            AND tc.id=c.type_connexion_id
            AND p.id=r.processus_id
            FOR UPDATE
        """


        cursor.execute(sqlstr)

        record = cursor.fetchall()
        # nbre = len(record)
        # print("nbre de declencheurs: {}".format(nbre))
        # print(self.user,self.password,self.host,self.port,self.database)
        # exit(1)
        return record


    def getDeclencheursUnefois(self, connection):
        print(__name__)
        print("__getDeclencheursUnefois__")

        cursor = connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
        # cursor = connection.cursor()

       

        sqlstr="""
        SELECT rd.id id_rd, rd.date_proch_exec, now(),
        TO_TIMESTAMP(rd.date_proch_exec, 'yyyy-mm-dd HH24:MI:SS') date_proch_exec,
        r.libelle, r.rep_destination, r.processus_id, r.sqlstr, r.id id_r,
        d.date_exec, d.heure_exec, d.frequence, repeat_jours,
        c.passdb, c.utidb, c.serveur, c.port, c.basedonnees, tc.code,
        p.nom processus
        FROM public.requete_declencheur rd, requete r, declencheur d, connexion c, type_connexion tc, processus p
        WHERE (rd.status=0 or rd.status is NULL)
        AND d.frequence='UNEFOIS'
        AND (
			rd.date_proch_exec is null
			AND d.date_exec = TO_CHAR(NOW() :: DATE, 'yyyy-mm-dd')  
			AND TO_CHAR(d.heure_exec::TIME,'HH24:MI') <= TO_CHAR(now()::TIME,'HH24:MI')
		) 
        AND d.id=rd.declencheur_id 
        AND r.id=rd.requete_id
		AND c.id=r.connexion_id 
		AND tc.id=c.type_connexion_id
        AND p.id=r.processus_id
        FOR UPDATE
        """


        cursor.execute(sqlstr)

        record = cursor.fetchall()
        # nbre = len(record)
        # print("nbre de declencheurs: {}".format(nbre))
        # print(self.user,self.password,self.host,self.port,self.database)
        # exit(1)
        return record

    
    def renitialiserRequeteDeclencheur(self, connection):
        # print(__name__)
        # print("__renitialiserRequeteDeclencheur__")
        # print(connection)        


        updated_rows = 0

        try:

            sqlstr = """
            UPDATE requete_declencheur
            SET status = %s
            WHERE status is not null;
            """

            status = '0'

            cursor = connection.cursor()
            cursor.execute(sqlstr, (status))
            updated_rows = cursor.rowcount
            connection.commit()
            cursor.close()

        except (Exception, psycopg2.DatabaseError) as erreur:
            print(erreur)
        # finally:
        #     if connection is not None:
        #         connection.close()

        return updated_rows

    
    def closeConnexion(self, connection):
        print(__name__)
        print("__closeConnexion__")
        if connection:
            connection.close()

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
