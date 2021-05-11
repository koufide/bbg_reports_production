# coding: utf-8

import sys
import os
import cx_Oracle
import pandas as pd
from pathlib import Path
from datetime import datetime
import configparser
config = configparser.ConfigParser()
config.read('/var/www/html/bbg-reports/flask/api/mesmodules/config.ini')
from mesmodules.ConnexionMysql import ConnexionMysql
from mesmodules.ConnexionPostgres import ConnexionPostgres
from mesmodules.ConnexionOracle import ConnexionOracle
from mesmodules.ConnexionSqlServer import ConnexionSqlServer


def testerLaConnexion(data):
    print("__testerLaConnexion__3__")

    try:
        res = None

        if(data):
            code = data['code']
            # nom = data['nom']
            serveur = data['serveur']
            port = data['port']
            utidb = data['utidb']
            passdb = data['passdb']
            basedonnees = data['basedonnees']

            print("code: {}".format(code))
            # print("nom: {}".format(nom))
            print("serveur: {}".format(serveur))
            print("port: {}".format(port))
            print("utidb: {}".format(utidb))
            # print("passdb: {}".format(passdb))
            print("basedonnees: {}".format(basedonnees))

            if(code == 'CNX_MYSQL'):
                # print("testconnexion MYSQL")
                cnxMysql = ConnexionMysql(
                    utidb, passdb, serveur, port, basedonnees)
                res = cnxMysql.testconnexion()
                # return json.dumps(res)

            elif(code == 'CNX_ORACLE'):
                cnx = ConnexionOracle(
                    utidb, passdb, serveur, port, basedonnees)
                res = cnx.testconnexion()
                # return json.dumps(res)

            elif(code == 'CNX_SQLSERVER'):  # OK
                cnx = ConnexionSqlServer(
                    utidb, passdb, serveur, port, basedonnees)
                res = cnx.testconnexion()
                # return json.dumps(res)

            elif(code == 'CNX_POSTGRES'):
                cnx = ConnexionPostgres(
                    utidb, passdb, serveur, port, basedonnees)
                res = cnx.testconnexion(
                    utidb, passdb, serveur, port, basedonnees)
                # print("res: {}".format(res))
                # return json.dumps(res)

            else:
                res = {"connexion": "Non defini"}
                # return json.dumps(res)

        # return json.dumps(res)
        print("__res__", res)
        return res

    except:
        print("__testerLaConnexion__except__")
        print(sys.exc_info())
        print("sys.exc_info()[0] :{}".format(str(sys.exc_info()[1])))
        res = {"erreur": str(sys.exc_info()[1]).replace("\'", "")}
        # return res

        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        erreur = "".format(str(sys.exc_info()[1]).replace(
            "\'", ""), fname, exc_tb.tb_lineno)
        return res


def genererLeFichierV2(data):
    print(__name__)
    print("__genererLeFichierV2__")

    try:

        print(data)
        code = data['code']
        processus = data['processus']
        # nom = data['nom']
        serveur = data['serveur']
        port = data['port']
        utidb = data['utidb']
        passdb = data['passdb']
        basedonnees = data['basedonnees']
        sqlstr = data['sqlstr']
        libelle = data['libelle']
        repDestination = data['repDestination']

        cnx = None
        res = None

        if(code == 'CNX_MYSQL'):
            # print("testconnexion MYSQL")
            cnx = ConnexionMysql(
                utidb, passdb, serveur, port, basedonnees).connexion()
            # res = cnxMysql.testconnexion()
            # return json.dumps(res)

        elif(code == 'CNX_ORACLE'):
            cnx = ConnexionOracle(
                utidb, passdb, serveur, port, basedonnees)
            # res = cnx.testrequete(sqlstr)
            # return json.dumps(res)
            dsn_tns = cx_Oracle.makedsn(serveur, port, basedonnees)
            cnx = cx_Oracle.connect(utidb, passdb, dsn_tns)

        elif(code == 'CNX_SQLSERVER'):
            cnx = ConnexionSqlServer(
                utidb, passdb, serveur, port, basedonnees).connexion()
            # res = cnx.testrequete(sqlstr)

        elif(code == 'CNX_POSTGRES'):
            cnx = ConnexionPostgres(
                utidb, passdb, serveur, port, basedonnees).connexion()
            # cnx = res.connexion()

            # res = cnx.testconnexion(utidb, passdb, serveur, port, basedonnees)
            # cnx = res
            # print("res: {}".format(res))
            # return json.dumps(res)

        else:
            res = {"connexion": "Non defini"}
            # return json.dumps(res)

        if(cnx):
            df = pd.read_sql(sqlstr, con=cnx)
            print(df)
            cnx.close()

            print(config['Paths']['Courant'])
            Courant = (config['Paths']['Courant'])
            # os.mkdir(Courant)

            print(config['Paths']['Archives'])
            Archives = (config['Paths']['Archives'])
            # os.mkdir(Archives)

            # os.mkdir(processus)
            # os.mkdir(repDestination)

            Extension = config['Fichiers']['Extension']
            print(Extension)

            SheetName = config['Fichiers']['SheetName']
            print(SheetName)

            Engine = config['Fichiers']['Engine']
            print(Engine)

            destination = Courant+"/" + processus+"/" + repDestination

            Path(destination).mkdir(parents=True, exist_ok=True)

            now = datetime.now()
            print(now)
            now_str = now.strftime("%Y%m%d_%H%M%S")
            print(now_str)
            print(libelle)
            nomfichier = str(libelle + '_'+now_str+'.'+Extension).replace(' ','_');

            # df.to_excel(repDestination+"/"+libelle+'.xlsx', sheet_name='Sheet_name_1', engine='xlsxwriter')
            df.to_excel(destination+"/" + nomfichier,  sheet_name=SheetName, engine=Engine)

            number_of_rows = len(df.index)
            if(number_of_rows > 0):
                res = {"generation": "OK"}
            else:
                res = {"generation": "NOK"}

        return res

    except:
        print("__genererLeFichier__except")
        print(sys.exc_info())
        print("sys.exc_info()[0] :{}".format(str(sys.exc_info()[1])))
        # res = {"erreur": str(sys.exc_info()[1]).replace("\'", "")}
        # return res

        # exc_type, exc_obj, exc_tb = sys.exc_info()
        # fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        # print(exc_type, fname, exc_tb.tb_lineno)
        # erreur = "{} , {}, {} ".format(exc_type, fname, exc_tb.tb_lineno)

        # exc_type, exc_obj, exc_tb = sys.exc_info()
        # fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        # print(exc_type, fname, exc_tb.tb_lineno)
        # erreur = "{}, {}, {}".format(exc_type, fname, exc_tb.tb_lineno)
        # erreur = "{}, {}, {}".format(
        #     str(sys.exc_info()[1]), fname, exc_tb.tb_lineno)
        # # return json.dumps({"erreur": erreur})
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print(sys.exc_info())
        print("sys.exc_info()[0] :{}".format(str(sys.exc_info()[1])))
        erreur = {
            "erreur": "Programme: {} line: {}, Erreur: {}, Message: {}".format(fname, exc_tb.tb_lineno, exc_type, sys.exc_info()[1])}

        return erreur


def genererLeFichier(data):
    print(__name__)
    print("__genererLeFichier__")

    try:

        print(data)
        code = data['code']
        processus = data['processus']
        nom = data['nom']
        serveur = data['serveur']
        port = data['port']
        utidb = data['utidb']
        passdb = data['passdb']
        basedonnees = data['basedonnees']
        sqlstr = data['sqlstr']
        libelle = data['libelle']
        repDestination = data['repDestination']

        cnx = None
        res = None

        if(code == 'CNX_MYSQL'):
            # print("testconnexion MYSQL")
            cnx = ConnexionMysql(
                utidb, passdb, serveur, port, basedonnees).connexion()
            # res = cnxMysql.testconnexion()
            # return json.dumps(res)

        elif(code == 'CNX_ORACLE'):
            cnx = ConnexionOracle(
                utidb, passdb, serveur, port, basedonnees)
            # res = cnx.testrequete(sqlstr)
            # return json.dumps(res)
            dsn_tns = cx_Oracle.makedsn(serveur, port, basedonnees)
            cnx = cx_Oracle.connect(utidb, passdb, dsn_tns)

        elif(code == 'CNX_SQLSERVER'):
            cnx = ConnexionSqlServer(
                utidb, passdb, serveur, port, basedonnees).connexion()
            # res = cnx.testrequete(sqlstr)

        elif(code == 'CNX_POSTGRES'):
            cnx = ConnexionPostgres(
                utidb, passdb, serveur, port, basedonnees).connexion()
            # cnx = res.connexion()

            # res = cnx.testconnexion(utidb, passdb, serveur, port, basedonnees)
            # cnx = res
            # print("res: {}".format(res))
            # return json.dumps(res)

        else:
            res = {"connexion": "Non defini"}
            # return json.dumps(res)

        if(cnx):
            df = pd.read_sql(sqlstr, con=cnx)
            print(df)
            cnx.close()

            print(config['Paths']['Courant'])
            Courant = (config['Paths']['Courant'])
            # os.mkdir(Courant)

            print(config['Paths']['Archives'])
            Archives = (config['Paths']['Archives'])
            # os.mkdir(Archives)

            # os.mkdir(processus)
            # os.mkdir(repDestination)

            Extension = config['Fichiers']['Extension']
            print(Extension)

            SheetName = config['Fichiers']['SheetName']
            print(SheetName)

            Engine = config['Fichiers']['Engine']
            print(Engine)

            destination = Courant+"/" + processus+"/" + repDestination

            Path(destination).mkdir(parents=True, exist_ok=True)

            now = datetime.now()
            print(now)
            now_str = now.strftime("%Y%m%d_%H%M%S")
            print(now_str)
            print(libelle)

            # df.to_excel(repDestination+"/"+libelle+'.xlsx', sheet_name='Sheet_name_1', engine='xlsxwriter')
            df.to_excel(destination+"/" + libelle + '_'+now_str+'.'+Extension,
                        sheet_name=SheetName, engine=Engine)

            number_of_rows = len(df.index)
            if(number_of_rows > 0):
                res = {"generation": "OK"}
            else:
                res = {"generation": "NOK"}

        return res

    except:
        print("__genererLeFichier__except")
        print(sys.exc_info())
        print("sys.exc_info()[0] :{}".format(str(sys.exc_info()[1])))
        # res = {"erreur": str(sys.exc_info()[1]).replace("\'", "")}
        # return res

        # exc_type, exc_obj, exc_tb = sys.exc_info()
        # fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        # print(exc_type, fname, exc_tb.tb_lineno)
        # erreur = "{} , {}, {} ".format(exc_type, fname, exc_tb.tb_lineno)

        # exc_type, exc_obj, exc_tb = sys.exc_info()
        # fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        # print(exc_type, fname, exc_tb.tb_lineno)
        # erreur = "{}, {}, {}".format(exc_type, fname, exc_tb.tb_lineno)
        # erreur = "{}, {}, {}".format(
        #     str(sys.exc_info()[1]), fname, exc_tb.tb_lineno)
        # # return json.dumps({"erreur": erreur})
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print(sys.exc_info())
        print("sys.exc_info()[0] :{}".format(str(sys.exc_info()[1])))
        erreur = {
            "erreur": "Programme: {} line: {}, Erreur: {}, Message: {}".format(fname, exc_tb.tb_lineno, exc_type, sys.exc_info()[1])}

        return erreur


def testerLaRequete(data):
    print(__name__)
    print("__testerLaRequete2__")

    



    try:

        code = data['code']
        sqlstr = data['sqlstr']
        # nom = data['nom']
        serveur = data['serveur']
        port = data['port']
        utidb = data['utidb']
        passdb = data['passdb']
        basedonnees = data['basedonnees']

        print("__testerLaRequete2__ , __code__ : {}".format(code))
        print("__testerLaRequete2__ , sqlstr : {}".format(sqlstr))

        if(code == 'CNX_MYSQL'):
            # print("testconnexion MYSQL")
            cnxMysql = ConnexionMysql(
                utidb, passdb, serveur, port, basedonnees)
            res = cnxMysql.testrequete(sqlstr)
            # return json.dumps(res)

        elif(code == 'CNX_ORACLE'):
            cnx = ConnexionOracle(
                utidb, passdb, serveur, port, basedonnees)
            res = cnx.testrequete(sqlstr)
            # return json.dumps(res)

        elif(code == 'CNX_SQLSERVER'):
            cnx = ConnexionSqlServer(utidb, passdb, serveur, port, basedonnees)
            res = cnx.testrequete(sqlstr)

            # return json.dumps(res)

        # elif(code == 'CNX_SQLSERVER2'):  # OK
        #     cnx = ConnexionSqlServer()
        #     res = cnx.testconnexion2(
        #         utidb, passdb, serveur, port, basedonnees)
        #     # return json.dumps(res)

        elif(code == 'CNX_POSTGRES'):
            cnx = ConnexionPostgres(utidb, passdb, serveur, port, basedonnees)
            res = cnx.testrequete(sqlstr)
            # print("res: {}".format(res))
            # return json.dumps(res)

        else:
            res = {"connexion": "Non defini"}
            # return json.dumps(res)

        return res

    except:
        print("__testerLaRequete__testerLaRequete__except")
        print(sys.exc_info())
        print("sys.exc_info()[0] :{}".format(str(sys.exc_info()[1])))
        res = {"erreur": str(sys.exc_info()[1]).replace("\'", "")}
        # return res

        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        erreur = "{} , {}, {} ".format(exc_type, fname, exc_tb.tb_lineno)
        return {"erreur": erreur}


def genererFichier(data, repertoire, nomfichier, SheetName, Engine):
    print(__name__)
    print("__genererFichier__")
    # print(connexion, repertoire, nomfichier, SheetName, Engine)

    import pandas as pd

    # print("connexion", connexion)
    print("repertoire", repertoire)
    print("nomfichier", nomfichier)

    res = testerLaConnexion(data)
    print("testerLaConnexion:: {}".format(res))
        # print("res:: {}".format(res['connexion']))

    if(res['connexion'] == 'OK'):
        res = testerLaRequete(data)
        print("testerLaRequete:: {}".format(res))

        if(res['nombreligne'] > 0):
            # print("OKOKO")
            print("genererLeFichierV2(data) data: {}".format(data))
            genererLeFichierV2(data)
            # res = genererLeFichier(data)

            # cnx=testerLaRequete(data)

            # df = pd.read_sql(data['sqlstr'], con=cnx)
            # df.to_excel(repertoire+"/" + nomfichier, sheet_name=SheetName, engine=Engine)


