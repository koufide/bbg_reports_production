# coding: utf-8

import sys

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
    # print(jour_str, jours_du_mois,  mois_str, mois)

    jours_du_moisTab = str(jours_du_mois).split(',')



    # print("\n date_exec: {} - heure_exec: {} - frequence: {} - libelle_req: {} - rep_destination: {} - connexion_nom: {} - mois: {} \n"
    #     .format(date_exec, heure_exec, frequence, libelle_req,rep_destination, connexion_nom, mois ))

    # print("mois: {}".format(mois))
    # print(type(mois))
    # moisTab = (mois).split()
    # print("moisTab: {}".format(moisTab))
    moisTab = (mois).split(',')
    # print(type(moisTab))
    # print("++++ mois_str: {}".format(moisTab))
    # print("++++ moisTab: {}".format(moisTab))

    # print("mois_str: {}".format(mois_str))
    # print("moisTab: {}".format(moisTab))

    # for m in moisTab:
    #     print(m)
    #     print(type(m))

    # for j in jours_du_moisTab:
    #     print(j)
    #     print(type(j))

    #     print((jour_str))
    #     print(type(jour_str))
    # exit(1)

    if mois_str in moisTab:
        # print("=> trouve mois", mois_str)

        # print("jour_str searched: x{}x".format(jour_str))

        # print("jour_str: {}".format(jour_str))
        # print("jours_du_moisTab: {}".format(jours_du_moisTab))

        if jour_str in jours_du_moisTab:
            print("=> trouve => jour ", jour_str)
            res = True



        else:
            print("<= nontrouve jour x{}x".format(jour_str))
            # print(jours_du_moisTab)
            # exit(1)

    else:
        print("=> nontrouve mois ",mois_str)
        # print(":{}:".format(mois_str))
    
    return res
    

def main():
    from datetime import datetime
    from pathlib import Path
    from mesmodules.ConnexionPostgres import ConnexionPostgres
    from mesmodules.MesFonctions import genererFichier
    import configparser



    config = configparser.ConfigParser()
    config.read('/var/www/html/bbg-reports/flask/api/mesmodules/config.ini')

    # print(config['Paths']['Courant'])
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

    cnx = ConnexionPostgres(
        utidb, passdb, serveur, port, basedonnees)

    res_cnx = cnx.testconnexion(
        utidb, passdb, serveur, port, basedonnees)
    
    print("cnx.testconnexion: ", res_cnx)

    if(res_cnx['connexion']=='OK'):
        record = cnx.getDeclencheurs()

        # print(record)
        # for r in record:
        #     print(r)
        
        # i = 0
        # while i < len(record):
        #     print(record[i])
            # i += 1
        
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
        print("annee_str: {} ".format(annee_str))
        # exit(1)
        
        for i in range(len(record)):
            # print(record[i])
            # print(record[i]['date_exec'])
            # print(record[i][3])
            # print(record[i][4])
            # print(record[i][4])
            date_exec = (record[i]['date_exec'])
            heure_exec = (record[i]['heure_exec'])
            frequence = (record[i]['frequence'])
            mois = (record[i]['mois'])
            # # moisTab = (mois).split(',', mois)
            # # print("moisTab: {}".format(moisTab))
            # print("mois: {}".format(mois))
            # exit(1)

            jours_du_mois =  (record[i]['jours_du_mois'])
            # jours_du_moisTab = str(jours_du_mois).split(',')




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

            
            

            # print("heure_exec:",heure_exec)
            # print("frequence:",frequence)

            datetime_exec_obj = datetime.strptime(date_exec + " "+heure_exec, "%Y-%m-%d %H:%M")
            print("datetime_exec_obj: ",datetime_exec_obj)
            print("now: ",now)

            if(datetime_exec_obj <= now  ):
                # print("continue => ")

                res = False
                if(frequence=='MENSUEL'):
                    # print("\n date_exec: {} - heure_exec: {} - frequence: {} - libelle_req: {} - rep_destination: {} - connexion_nom: {} \n".format(date_exec, heure_exec, frequence, libelle_req,rep_destination, connexion_nom))
                    res = declencheurMensuel(jour_str, jours_du_mois,  mois_str, mois)
                
                # print("res: {} ".format(res))

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
                        
                        # now = datetime.now()
                        # print(now)
                        now_str = now.strftime("%Y%m%d_%H%M%S")
                        # print(now_str)
                        # print(libelle_req)

                        nomfichier = libelle_req+ '_'+now_str+'.'+Extension
                        nomfichier = str(nomfichier).replace(' ','_')

                        # df.to_excel(repDestination+"/"+libelle+'.xlsx', sheet_name='Sheet_name_1', engine='xlsxwriter')
                        # df.to_excel(destination+"/" + libelle + '_'+now_str+'.'+Extension,                                sheet_name=SheetName, engine=Engine)

                        genererFichier(connexion, destination, nomfichier, SheetName, Engine)
                    except OSError as erreur:
                        print("<= erreur: {}: ".format(erreur))

                    


            # else:
            #     print("saute <= ")
            # exit(1)




    else:
        print("Impossible de se connecter: {},{},{},{}".format(serveur, port, basedonnees, utidb))




if(__name__=='__main__'):
    print("==================== DEMARRAGE ====================")
    # print(sys.version_info)
    # print(this_python)
    main()
    print("==================== FIN ==========================")
