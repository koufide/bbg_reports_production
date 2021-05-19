# coding: utf-8

from datetime import timedelta
from datetime import datetime
from pathlib import Path
from mesmodules.MesFonctions import genererFichier
from mesmodules.ConnexionPostgres import ConnexionPostgres
from dateutil.relativedelta import relativedelta
import sys


import configparser


config = configparser.ConfigParser()
config.read('/var/www/html/bbg-reports/flask/api/mesmodules/config.ini')



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

    connection = cnxpostgres.connexion()

    if(cnxpostgres):
        
        now = datetime.now()
        # print("now: {} ".format(now))
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        # print("dt_string: {} ".format(dt_string))
        today_str = now.strftime("%d/%m/%Y")
        # print("today_str: {} ".format(today_str))
        # time_str = now.strftime("%H:%M:%S")
        time_str = now.strftime("%H:%M")
        # print("time_str: {} ".format(time_str))
        
        jour_str = now.strftime("%d")
        # print("jour_str: {} ".format(jour_str))
        jour_str = str(int(jour_str))
        # print("jour_str: {} ".format(jour_str))

        mois_str = now.strftime("%m")
        mois_int = int(mois_str)
        mois_str = str(mois_int)
        # print("mois_str: {} ".format(mois_str))
        # print("mois_int: {} ".format(mois_int))

        annee_str = now.strftime("%Y")
        # print("annee_str: {} ".format(annee_str))
        # exit(1)


        getDeclencheurJournalier(cnxpostgres)
        exit(1)


        

        record = cnxpostgres.getDeclencheurs()

        for i in range(len(record)):
            rd_id = (record[i]['id'])

            updated_rows = cnxpostgres.updateDeclencheur(rd_id, 1)
            print("updated_rows: {} ".format(updated_rows))

            date_exec = (record[i]['date_exec'])
            heure_exec = (record[i]['heure_exec'])
            frequence = (record[i]['frequence'])
            mois = (record[i]['mois'])

            jours_du_mois =  (record[i]['jours_du_mois'])

            libelle_req = (record[i]['libelle_req'])
            rep_destination = (record[i]['rep_destination'])
            sqlstr = (record[i]['sqlstr'])
            serveur = (record[i]['serveur'])
            port = (record[i]['port'])
            basedonnees = (record[i]['basedonnees'])
            utidb = (record[i]['utidb'])
            passdb = (record[i]['passdb'])
            connexion_nom = (record[i]['connexion_nom'])
            processus_nom = (record[i]['processus_nom'])
            typeconnexion_code = (record[i]['typeconnexion_code'])
            typeconnexion_libelle = (record[i]['typeconnexion_libelle'])


            datetime_exec_obj = datetime.strptime(date_exec + " "+heure_exec, "%Y-%m-%d %H:%M")
            print("datetime_exec_obj: ",datetime_exec_obj)
            print("now: ",now)

            if(datetime_exec_obj <= now  ):

                res = False

                if(frequence=='MENSUEL'):
                    # print("\n date_exec: {} - heure_exec: {} - frequence: {} - libelle_req: {} - rep_destination: {} - connexion_nom: {} \n".format(date_exec, heure_exec, frequence, libelle_req,rep_destination, connexion_nom))
                    res = declencheurMensuel(jour_str, jours_du_mois,  mois_str, mois)
                
                # if frequence=='JOURNALIER':
                #     res = declencheurJournalier()
                

                if res == True:
                    connexion = {
                        'code': typeconnexion_code,
                        'utidb': utidb,
                        'passdb': passdb,
                        'serveur':serveur,
                        'port': port,
                        'basedonnees': basedonnees,
                        'sqlstr':sqlstr,
                        'processus':processus_nom,
                        'libelle':libelle_req,
                        'repDestination': rep_destination
                    }

                    destination = Courant+"/" + processus_nom+"/" + rep_destination

                    try:
                        Path(destination).mkdir(parents=True, exist_ok=True)
                        
                        now_str = now.strftime("%Y%m%d_%H%M%S")

                        nomfichier = libelle_req+ '_'+now_str+'.'+Extension
                        nomfichier = str(nomfichier).replace(' ','_')

                        res_gen = genererFichier(connexion, destination, nomfichier, SheetName, Engine)
                        print("res_gen: {} ".format(res_gen))


                        if res_gen['generation']=='OK':

                            # updated_rows = cnxpostgres.updateDeclencheur(id_requete_declencheur=rd_id, status=3)
                            # # updated_rows = cnxpostgres.updateDeclencheur(id_requete_declencheur=rd_id, status=0)
                            # print("updated_rows: {} ".format(updated_rows))


                            ## --- calculer la prochaine date de regeneraton
                            # 2021-05-17 13:39  ==> 2021-06-17 13:39
                            print("datetime_exec_obj => ".format(datetime_exec_obj))
                            date_exec_mois_str = datetime_exec_obj.strftime("%m")
                            date_exec_mois_int = int(date_exec_mois_str)
                            date_exec_mois_int =  date_exec_mois_int+1
                            dt =  relativedelta(months=+1)
                            print(datetime_exec_obj+ dt)


                            # updated_rows = cnxpostgres.updateDeclencheur(id_requete_declencheur=rd_id, status=3)
                            updated_rows = cnxpostgres.updateDeclencheurV2(connection,  "status='{}', date_proch_exec='{}'".format(3, (datetime_exec_obj+dt) ), "id={}".format(rd_id)  )
                            print("updated_rows: {} ".format(updated_rows))
                            

                            exit(1)


                        else:
                            print("Fichier non généré: {}/{}".format((destination,nomfichier)))
                    
                    except OSError as erreur:
                        print("<= erreur: {}: ".format(erreur))

    else:
        print("Impossible de se connecter: {},{},{},{}".format(serveur, port, basedonnees, utidb))



def getDeclencheurJournalier(cnxpostgres):
    print(__name__)
    print("__getDeclencheurJournalier__")
    import time

    Courant = (config['Paths']['Courant'])
    # Archives = (config['Paths']['Archives'])
    Extension = config['Fichiers']['Extension']
    SheetName = config['Fichiers']['SheetName']
    Engine = config['Fichiers']['Engine']
    # serveur = config['ConnexionPostgres']['Serveur']
    # port = config['ConnexionPostgres']['Port']
    # basedonnees = config['ConnexionPostgres']['BaseDonnees']
    # utidb = config['ConnexionPostgres']['Username']
    # passdb = config['ConnexionPostgres']['Password']

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
                dt =  relativedelta(days=+int(repeat_jours))
                datetime_exec_obj_modif=(datetime_exec_obj+dt)
                print(datetime_exec_obj_modif)


                updated_rows = cnxpostgres.updateTable(connection, "requete_declencheur",  "status='{}', date_proch_exec='{}' ".format(3, datetime_exec_obj_modif ), "id={}".format(id_rd)  )
                print("updated_rows: {} ".format(updated_rows))
            else:
                print("res_gen NOK ")



        except OSError as erreur:
            print("<= erreur: {}: ".format(erreur))
        
        
        



        fin = datetime.now()
        # print("now: {} ".format(now))
        dt_string = fin.strftime("%d/%m/%Y %H:%M:%S")
        print("==== fin dt_string: {} ".format(dt_string))
        temps_ecoule = fin - debut
        print("==== temps_ecoule: {} seconde(s) ".format(temps_ecoule.seconds))




if(__name__=='__main__'):
    print("==================== DEMARRAGE ====================")
    # print(sys.version_info)
    # print(this_python)
    main()
    print("==================== FIN ==========================")
