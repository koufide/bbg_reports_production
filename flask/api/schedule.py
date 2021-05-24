# coding: utf-8

from datetime import timedelta
from datetime import datetime
from pathlib import Path
from mesmodules.MesFonctions import genererFichier
from mesmodules.ConnexionPostgres import ConnexionPostgres
from dateutil.relativedelta import relativedelta
import sys
import time


import configparser


config = configparser.ConfigParser()
config.read('/var/www/html/bbg-reports/flask/api/mesmodules/config.ini')

Courant = (config['Paths']['Courant'])
# Archives = (config['Paths']['Archives'])
Extension = config['Fichiers']['Extension']
SheetName = config['Fichiers']['SheetName']
Engine = config['Fichiers']['Engine']



this_python = sys.version_info[:2]
print("this_python",this_python)

# min_version = (3, 6)
min_version = (2, 7)
if this_python < min_version:
    message_parts = [
        "This script does not work on Python {}.{}".format(*this_python),
        "The minimum supported Python version is {}.{}.".format(*min_version),
        "Please use https://bootstrap.pypa.io/pip/{}.{}/get-pip.py instead.".format(*this_python),
    ]
    print("ERROR: " + " ".join(message_parts))
    sys.exit(1)


def declencheurMensuel(jour_str, jours_du_mois,  mois_str, mois):
    print(__name__)
    print("__declencheurMensuel__")
    res = False

    jours_du_moisTab = str(jours_du_mois).split(',')

    moisTab = (mois).split(',')

    if mois_str in moisTab:

        if jour_str in jours_du_moisTab:
            # print("=> trouve => jour ", jour_str)
            res = True

        else:
            print("<= nontrouve jour x{}x".format(jour_str))

    else:
        print("=> nontrouve mois ",mois_str)
    
    return res

# def declencheurJournalier():
#     print(__name__) 
#     print("__declencheurJournalier__") 

#     Courant = (config['Paths']['Courant'])

#     Archives = (config['Paths']['Archives'])

#     Extension = config['Fichiers']['Extension']

#     SheetName = config['Fichiers']['SheetName']

#     Engine = config['Fichiers']['Engine']





def main():
    

    Courant = (config['Paths']['Courant'])

    Archives = (config['Paths']['Archives'])

    Extension = config['Fichiers']['Extension']

    SheetName = config['Fichiers']['SheetName']

    Engine = config['Fichiers']['Engine']

    

    serveur = config['ConnexionPostgres']['Serveur']
    port = config['ConnexionPostgres']['Port']
    basedonnees = config['ConnexionPostgres']['BaseDonnees']
    utidb = config['ConnexionPostgres']['Username']
    passdb = config['ConnexionPostgres']['Password']

    # print(serveur, port, basedonnees, utidb, passdb)
    # exit(1)

    cnxpostgres = ConnexionPostgres(
        utidb, passdb, serveur, port, basedonnees)

    # connection = cnxpostgres.connexion()

    if(cnxpostgres):
        

        getDeclencheurMensuel(cnxpostgres)
        getDeclencheurHebdomadaire(cnxpostgres)
        getDeclencheurJournalier(cnxpostgres)
        getDeclencheurUnefois(cnxpostgres)

        # cnxpostgres.closeConnexion(connection)
        # exit(1)


    else:
        print("Impossible de se connecter: {},{},{},{}".format(serveur, port, basedonnees, utidb))


def getDeclencheurMensuel(cnxpostgres):
    print(__name__)
    print("__getDeclencheurMensuel__")


    connection = cnxpostgres.connexion()

    record = cnxpostgres.getDeclencheursMensuel(connection)
    # print("getDeclencheurJournalier - record: {}".format(record))
    # for i in range(len(record)):
    #     rd_id = (record[i]['id'])
    for r in record:
        print("r: {}".format(r))

        debut = datetime.now()
        # print("now: {} ".format(now))
        dt_string = debut.strftime("%d/%m/%Y %H:%M:%S")
        print("==== debut dt_string: {} ".format(dt_string))
        # today_str = now.strftime("%d/%m/%Y")
        # print("today_str: {} ".format(today_str))
        # time_str = now.strftime("%H:%M:%S")
        # time_str = now.strftime("%H:%M")
        # print("time_str: {} ".format(time_str))

        date_exec=r['date_exec']
        heure_exec=r['heure_exec']
        print("Date execution: {} {}".format(date_exec,heure_exec))
        # time.sleep(60)
        datetime_exec_obj = datetime.strptime(date_exec + " "+heure_exec, "%Y-%m-%d %H:%M")
        print("datetime_exec_obj: ",datetime_exec_obj)

        serveur=r['serveur']
        port=r['port']
        basedonnees=r['basedonnees']
        utidb=r['utidb']
        passdb=r['passdb']
        sqlstr=r['sqlstr']
        processus=r['processus']
        libelle=r['libelle']
        rep_destination=r['rep_destination']
        mois=r['mois']
        jours_du_mois=r['jours_du_mois']
        frequence=r['frequence']
        date_proch_exec=r['date_proch_exec']
        id_rd=r['id_rd']
        code=r['code']
        jourdb=r['jourdb']
        moisdb=r['moisdb']
        anneedb=r['anneedb']

        updated_rows = cnxpostgres.updateTable(connection, "requete_declencheur",  "status='{}'".format(1), "id={}".format(id_rd)  )
        print("updated_rows: {}".format(updated_rows))

        connexion = {
                        'code': code,
                        'utidb': utidb,
                        'passdb': passdb,
                        'serveur':serveur,
                        'port': port,
                        'basedonnees': basedonnees,
                        'sqlstr':sqlstr,
                        'processus':processus,
                        'libelle':libelle,
                        'repDestination': rep_destination
                    }

        destination = Courant+"/" + processus+"/" + rep_destination

        try:
            Path(destination).mkdir(parents=True, exist_ok=True)
	
            now_str = debut.strftime("%Y%m%d_%H%M%S")

            nomfichier = libelle+ '_'+now_str+'.'+Extension
            nomfichier = str(nomfichier).replace(' ','_')

            res_gen = genererFichier(connexion, destination, nomfichier, SheetName, Engine)
            print("res_gen: {} ".format(res_gen))

            if res_gen['generation']=='OK':
                ## --- calculer la prochaine date de regeneraton
                print("res_gen OK ")
                print(datetime_exec_obj)
                dt =  relativedelta(months=+int(1))  # a revoir
                datetime_exec_obj_modif=(debut+dt)
                print(datetime_exec_obj_modif)
                datetime_exec_obj_modif_str=datetime_exec_obj_modif.strftime("%Y-%m-%d %H:%M:%S")
                print(datetime_exec_obj_modif_str)


                updated_rows = cnxpostgres.updateTable(connection, "requete_declencheur",  "status='{}', date_proch_exec='{}' ".format(0, datetime_exec_obj_modif_str ), "id={}".format(id_rd)  )
                print("updated_rows: {} ".format(updated_rows))
            else:
                print("res_gen NOK ")



        except OSError as erreur:
            print("<= erreur: {}: ".format(erreur))
        except:
            # exit("quite")
            print("<= erreur: ",str(sys.exc_info()))
            # print ("<= erreur: Unexpected error:", sys.exc_info()[0])
            # raise
        



        fin = datetime.now()
        # print("now: {} ".format(now))
        dt_string = fin.strftime("%d/%m/%Y %H:%M:%S")
        print("==== fin dt_string: {} ".format(dt_string))
        temps_ecoule = fin - debut
        print("==== temps_ecoule: {} seconde(s) ".format(temps_ecoule.seconds))

        updated_rows = cnxpostgres.updateTable(connection, "requete_declencheur",  
        "log_debut_exec='{}', log_fin_exec='{}', log_duree_exec='{}' ".format(debut,fin,temps_ecoule.seconds ), 
        "id={}".format(id_rd)  )
        print("updated_rows: {} ".format(updated_rows))

    cnxpostgres.closeConnexion(connection)

def getDeclencheurJournalier(cnxpostgres):
    print(__name__)
    print("__getDeclencheurJournalier__")
    # import time


    connection = cnxpostgres.connexion()

    record = cnxpostgres.getDeclencheursJournalier(connection)
    # print("getDeclencheurJournalier - record: {}".format(record))
    # for i in range(len(record)):
    #     rd_id = (record[i]['id'])
    for r in record:
        print("r: {}".format(r))

        debut = datetime.now()
        # print("now: {} ".format(now))
        dt_string = debut.strftime("%d/%m/%Y %H:%M:%S")
        print("==== debut dt_string: {} ".format(dt_string))
        # today_str = now.strftime("%d/%m/%Y")
        # print("today_str: {} ".format(today_str))
        # time_str = now.strftime("%H:%M:%S")
        # time_str = now.strftime("%H:%M")
        # print("time_str: {} ".format(time_str))

        date_exec=r['date_exec']
        heure_exec=r['heure_exec']
        print("Date execution: {} {}".format(date_exec,heure_exec))
        # time.sleep(60)
        datetime_exec_obj = datetime.strptime(date_exec + " "+heure_exec, "%Y-%m-%d %H:%M")
        print("datetime_exec_obj: ",datetime_exec_obj)

        serveur=r['serveur']
        port=r['port']
        basedonnees=r['basedonnees']
        utidb=r['utidb']
        passdb=r['passdb']
        sqlstr=r['sqlstr']
        processus=r['processus']
        libelle=r['libelle']
        rep_destination=r['rep_destination']
        repeat_jours=r['repeat_jours']
        frequence=r['frequence']
        date_proch_exec=r['date_proch_exec']
        id_rd=r['id_rd']
        code=r['code']

        updated_rows = cnxpostgres.updateTable(connection, "requete_declencheur",  "status='{}'".format(1), "id={}".format(id_rd)  )
        print("updated_rows: {}".format(updated_rows))

        connexion = {
                        'code': code,
                        'utidb': utidb,
                        'passdb': passdb,
                        'serveur':serveur,
                        'port': port,
                        'basedonnees': basedonnees,
                        'sqlstr':sqlstr,
                        'processus':processus,
                        'libelle':libelle,
                        'repDestination': rep_destination
                    }

        destination = Courant+"/" + processus+"/" + rep_destination

        try:
            Path(destination).mkdir(parents=True, exist_ok=True)
	
            now_str = debut.strftime("%Y%m%d_%H%M%S")

            nomfichier = libelle+ '_'+now_str+'.'+Extension
            nomfichier = str(nomfichier).replace(' ','_')

            res_gen = genererFichier(connexion, destination, nomfichier, SheetName, Engine)
            print("res_gen: {} ".format(res_gen))

            if res_gen['generation']=='OK':
                ## --- calculer la prochaine date de regeneraton
                print("res_gen OK ")
                print(datetime_exec_obj)
                print(debut)
                dt =  relativedelta(days=+int(repeat_jours))
                # datetime_exec_obj_modif=(datetime_exec_obj+dt)
                datetime_exec_obj_modif=(debut+dt)
                print(datetime_exec_obj_modif)
                datetime_exec_obj_modif_str=datetime_exec_obj_modif.strftime("%Y-%m-%d %H:%M:%S")
                print(datetime_exec_obj_modif_str)


                updated_rows = cnxpostgres.updateTable(connection, "requete_declencheur",  "status='{}', date_proch_exec='{}' ".format(0, datetime_exec_obj_modif_str ), "id={}".format(id_rd)  )
                print("updated_rows: {} ".format(updated_rows))
            else:
                print("res_gen NOK ")



        except OSError as erreur:
            print("<= erreur: {}: ".format(erreur))
        except:
            # exit("quite")
            print("<= erreur: ",str(sys.exc_info()))
            # print ("<= erreur: Unexpected error:", sys.exc_info()[0])
            # raise
        
        
        



        fin = datetime.now()
        # print("now: {} ".format(now))
        dt_string = fin.strftime("%d/%m/%Y %H:%M:%S")
        print("==== fin dt_string: {} ".format(dt_string))
        temps_ecoule = fin - debut
        print("==== temps_ecoule: {} seconde(s) ".format(temps_ecoule.seconds))

        updated_rows = cnxpostgres.updateTable(connection, "requete_declencheur",  
        "log_debut_exec='{}', log_fin_exec='{}', log_duree_exec='{}' ".format(debut,fin,temps_ecoule.seconds ), 
        "id={}".format(id_rd)  )
        print("updated_rows: {} ".format(updated_rows))

    cnxpostgres.closeConnexion(connection)


def getDeclencheurUnefois(cnxpostgres):
    print(__name__)
    print("__getDeclencheurUnefois__")
    
    connection = cnxpostgres.connexion()

    record = cnxpostgres.getDeclencheursUnefois(connection)
    # print("getDeclencheurJournalier - record: {}".format(record))
    for r in record:
        print("r: {}".format(r))

        debut = datetime.now()
        # print("now: {} ".format(now))
        dt_string = debut.strftime("%d/%m/%Y %H:%M:%S")
        print("==== debut dt_string: {} ".format(dt_string))
        # today_str = now.strftime("%d/%m/%Y")
        # print("today_str: {} ".format(today_str))
        # time_str = now.strftime("%H:%M:%S")
        # time_str = now.strftime("%H:%M")
        # print("time_str: {} ".format(time_str))

        date_exec=r['date_exec']
        heure_exec=r['heure_exec']
        print("Date execution: {} {}".format(date_exec,heure_exec))
        # time.sleep(60)
        datetime_exec_obj = datetime.strptime(date_exec + " "+heure_exec, "%Y-%m-%d %H:%M")
        print("datetime_exec_obj: ",datetime_exec_obj)

        serveur=r['serveur']
        port=r['port']
        basedonnees=r['basedonnees']
        utidb=r['utidb']
        passdb=r['passdb']
        sqlstr=r['sqlstr']
        processus=r['processus']
        libelle=r['libelle']
        rep_destination=r['rep_destination']
        repeat_jours=r['repeat_jours']
        repeat_jours=r['repeat_jours']
        frequence=r['frequence']
        date_proch_exec=r['date_proch_exec']
        id_rd=r['id_rd']
        code=r['code']

        updated_rows = cnxpostgres.updateTable(connection, 
        "requete_declencheur",  "status='{}'".format(1), 
        "id={}".format(id_rd)  )
        print("updated_rows: {}".format(updated_rows))

        connexion = {
                        'code': code,
                        'utidb': utidb,
                        'passdb': passdb,
                        'serveur':serveur,
                        'port': port,
                        'basedonnees': basedonnees,
                        'sqlstr':sqlstr,
                        'processus':processus,
                        'libelle':libelle,
                        'repDestination': rep_destination
                    }

        destination = Courant+"/" + processus+"/" + rep_destination

        try:
            Path(destination).mkdir(parents=True, exist_ok=True)
	
            now_str = debut.strftime("%Y%m%d_%H%M%S")

            nomfichier = libelle+ '_'+now_str+'.'+Extension
            nomfichier = str(nomfichier).replace(' ','_')

            res_gen = genererFichier(connexion, destination, nomfichier, SheetName, Engine)
            print("res_gen: {} ".format(res_gen))

            if res_gen['generation']=='OK':
                ## --- calculer la prochaine date de regeneraton
                print("res_gen OK ")
                # print(datetime_exec_obj)
                # print(debut)
                # dt =  relativedelta(days=+int(repeat_jours))
                # # datetime_exec_obj_modif=(datetime_exec_obj+dt)
                # datetime_exec_obj_modif=(debut+dt)
                # print(datetime_exec_obj_modif)
                # datetime_exec_obj_modif_str=datetime_exec_obj_modif.strftime("%Y-%m-%d %H:%M:%S")
                # print(datetime_exec_obj_modif_str)

                updated_rows = cnxpostgres.updateTable(connection, "requete_declencheur",  
                "status='{}' ".format(2), 
                "id={}".format(id_rd)  )
                print("updated_rows: {} ".format(updated_rows))
            else:
                print("res_gen NOK ")

        except OSError as erreur:
            print("<= erreur: {}: ".format(erreur))
        except:
            # exit("quite")
            print("<= erreur: ",str(sys.exc_info()))
            # print ("<= erreur: Unexpected error:", sys.exc_info()[0])
            # raise
        


        fin = datetime.now()
        # print("now: {} ".format(now))
        dt_string = fin.strftime("%d/%m/%Y %H:%M:%S")
        print("==== fin dt_string: {} ".format(dt_string))
        temps_ecoule = fin - debut
        print("==== temps_ecoule: {} seconde(s) ".format(temps_ecoule.seconds))

        updated_rows = cnxpostgres.updateTable(connection, "requete_declencheur",  
        "log_debut_exec='{}', log_fin_exec='{}', log_duree_exec='{}' ".format(debut,fin,temps_ecoule.seconds ), 
        "id={}".format(id_rd)  )
        print("updated_rows: {} ".format(updated_rows))

    cnxpostgres.closeConnexion(connection)


def getDeclencheurHebdomadaire(cnxpostgres):
    print(__name__)
    print("__getDeclencheurHebdomadaire__")

    connection = cnxpostgres.connexion()

    record = cnxpostgres.getDeclencheursHebdomadaire(connection)
    # print("getDeclencheurJournalier - record: {}".format(record))
    for r in record:
        print("r: {}".format(r))

        debut = datetime.now()
        # print("now: {} ".format(now))
        dt_string = debut.strftime("%d/%m/%Y %H:%M:%S")
        print("==== debut dt_string: {} ".format(dt_string))
        # today_str = now.strftime("%d/%m/%Y")
        # print("today_str: {} ".format(today_str))
        # time_str = now.strftime("%H:%M:%S")
        # time_str = now.strftime("%H:%M")
        # print("time_str: {} ".format(time_str))

        date_exec=r['date_exec']
        heure_exec=r['heure_exec']
        print("Date execution: {} {}".format(date_exec,heure_exec))
        # time.sleep(60)
        datetime_exec_obj = datetime.strptime(date_exec + " "+heure_exec, "%Y-%m-%d %H:%M")
        print("datetime_exec_obj: ",datetime_exec_obj)

        serveur=r['serveur']
        port=r['port']
        basedonnees=r['basedonnees']
        utidb=r['utidb']
        passdb=r['passdb']
        sqlstr=r['sqlstr']
        processus=r['processus']
        libelle=r['libelle']
        rep_destination=r['rep_destination']
        repeat_semaines=r['repeat_semaines']
        jours=r['jours']
        frequence=r['frequence']
        date_proch_exec=r['date_proch_exec']
        id_rd=r['id_rd']
        code=r['code']
        jourdb=r['jourdb']
        moisdb=r['moisdb']
        anneedb=r['anneedb']
        dowdb=r['dowdb']
        jours_tab=r['jours_tab']

        updated_rows = cnxpostgres.updateTable(connection, 
        "requete_declencheur",  "status='{}'".format(1), 
        "id={}".format(id_rd)  )
        print("updated_rows: {}".format(updated_rows))
       

        connexion = {
                        'code': code,
                        'utidb': utidb,
                        'passdb': passdb,
                        'serveur':serveur,
                        'port': port,
                        'basedonnees': basedonnees,
                        'sqlstr':sqlstr,
                        'processus':processus,
                        'libelle':libelle,
                        'repDestination': rep_destination
                    }

        destination = Courant+"/" + processus+"/" + rep_destination

        try:
            # print("destination",destination)
            Path(destination).mkdir(parents=True, exist_ok=True)
            # exit(1)
	
            now_str = debut.strftime("%Y%m%d_%H%M%S")

            nomfichier = libelle+ '_'+now_str+'.'+Extension
            nomfichier = str(nomfichier).replace(' ','_')

            res_gen = genererFichier(connexion, destination, nomfichier, SheetName, Engine)
            print("res_gen: {} ".format(res_gen))

            if res_gen['generation']=='OK':
                ## --- calculer la prochaine date de regeneraton
                print("res_gen OK ")
                print(datetime_exec_obj)
                print(debut)
                dt =  relativedelta(weeks=+int(repeat_semaines))
                # datetime_exec_obj_modif=(datetime_exec_obj+dt)
                datetime_exec_obj_modif=(debut+dt)
                print(datetime_exec_obj_modif)
                datetime_exec_obj_modif_str=datetime_exec_obj_modif.strftime("%Y-%m-%d %H:%M:%S")
                print(datetime_exec_obj_modif_str)


                updated_rows = cnxpostgres.updateTable(connection, 
                "requete_declencheur",  
                "status='{}', date_proch_exec='{}' ".format(0, datetime_exec_obj_modif_str ),
                 "id={}".format(id_rd)  )
                print("updated_rows: {} ".format(updated_rows))
            else:
                print("res_gen NOK ")



        except OSError as erreur:
            print("<= erreur: {}: ".format(erreur))
        except:
            # exit("quite")
            print("<= erreur: ",str(sys.exc_info()))
            # print ("<= erreur: Unexpected error:", sys.exc_info()[0])
            # raise
        


        fin = datetime.now()
        # print("now: {} ".format(now))
        dt_string = fin.strftime("%d/%m/%Y %H:%M:%S")
        print("==== fin dt_string: {} ".format(dt_string))
        temps_ecoule = fin - debut
        print("==== temps_ecoule: {} seconde(s) ".format(temps_ecoule.seconds))

        updated_rows = cnxpostgres.updateTable(connection, "requete_declencheur",  
        "log_debut_exec='{}', log_fin_exec='{}', log_duree_exec='{}' ".format(debut,fin,temps_ecoule.seconds ), 
        "id={}".format(id_rd)  )
        print("updated_rows: {} ".format(updated_rows))

    cnxpostgres.closeConnexion(connection)


if(__name__=='__main__'):
    print("==================== DEMARRAGE ====================")
    # print(sys.version_info)
    # print(this_python)
    main()
    print("==================== FIN ==========================")
