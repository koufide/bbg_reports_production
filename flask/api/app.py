# coding: utf-8

from re import error
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS, cross_origin

from mesmodules.ConnexionMysql import ConnexionMysql
from mesmodules.ConnexionPostgres import ConnexionPostgres
from mesmodules.ConnexionOracle import ConnexionOracle
from mesmodules.ConnexionSqlServer import ConnexionSqlServer

import numpy as np
import pandas as pd

import cx_Oracle

import json
import sys
import os

# import xml.etree.ElementTree as ET
from xml.dom import minidom

import configparser

from pathlib import Path

from datetime import datetime


# -------------------------------------------------------
# print("KF GETCWD : {}".format(os.getcwd()))
# print("KF GETCWDB : {}".format(os.getcwdb()))
# print("KF LISTDIR : {}".format(os.listdir()))

# -------------------------------------------------------

config = configparser.ConfigParser()
# print("sections1: {}".format(config.sections()))

config.read('/var/www/html/bbg-reports/flask/api/mesmodules/config.ini')

# print("sections3: {}".format(config.read_file(open('config.ini'))))
# print("sections2: {}".format(config.sections()))
# print("config: {}".format(config))
# print(__name__)


app = Flask(__name__)

listorigins = config['Cors']['Origins']
print("listorigins: {}".format(listorigins))
# cors = CORS(app)
# cors = CORS(app, resources={r"/api/pyflask/*": {"origins": "http://192.168.56.220:4200"}})
cors = CORS(app, resources={r"/api/pyflask/*": {"origins": listorigins}})
app.config['CORS_HEADERS'] = 'Content-Type'




@app.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>La ressource est introuvable</p>", 404


@app.route('/')
def home():
    return 'API - PYTHON FLASK'



def testerLaConnexion(data):
    print("__testerLaConnexion__3__")

    try:

        res = None

        if(data):
            code = data['code']
            nom = data['nom']
            serveur = data['serveur']
            port = data['port']
            utidb = data['utidb']
            passdb = data['passdb']
            basedonnees = data['basedonnees']

            print("code: {}".format(code))
            print("nom: {}".format(nom))
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


def testerLaRequete(data):
    print("__testerLaRequete2__")
    try:

        code = data['code']
        sqlstr = data['sqlstr']
        nom = data['nom']
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


def genererLeFichier(data):
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


@app.route('/api/pyflask/testconnection', methods=['POST'])
# @cross_origin()
def testconnection():
    print("__testconnection__")
    try:

        print("testconnection data: {}".format(request.json))
        data = request.json
        print("testconnection data: {}".format(data))

        # retour_json = []
        # erreur_json = []

        res = testerLaConnexion(data)
        print("testconnection res: {}".format(res))
        return json.dumps(res)

        # if(data):

        #     code = data['code']
        #     nom = data['nom']

        #     serveur = data['serveur']
        #     port = data['port']
        #     utidb = data['utidb']
        #     passdb = data['passdb']
        #     basedonnees = data['basedonnees']

        #     print("code: {}".format(code))
        #     print("nom: {}".format(nom))
        #     print("serveur: {}".format(serveur))
        #     print("port: {}".format(port))
        #     print("utidb: {}".format(utidb))
        #     # print("passdb: {}".format(passdb))
        #     print("basedonnees: {}".format(basedonnees))

        #     if(code == 'CNX_MYSQL'):
        #         # print("testconnexion MYSQL")
        #         cnxMysql = ConnexionMysql(
        #             utidb, passdb, serveur, port, basedonnees)
        #         res = cnxMysql.testconnexion()
        #         return json.dumps(res)

        #     elif(code == 'CNX_ORACLE'):
        #         cnx = ConnexionOracle(
        #             utidb, passdb, serveur, port, basedonnees)
        #         res = cnx.testconnexion()
        #         return json.dumps(res)

        #     elif(code == 'CNX_SQLSERVER'):
        #         cnx = ConnexionSqlServer()
        #         res = cnx.testconnexion2(
        #             utidb, passdb, serveur, port, basedonnees)
        #         return json.dumps(res)

        #     elif(code == 'CNX_SQLSERVER2'):  # OK
        #         cnx = ConnexionSqlServer()
        #         res = cnx.testconnexion2(
        #             utidb, passdb, serveur, port, basedonnees)
        #         return json.dumps(res)

        #     elif(code == 'CNX_POSTGRES'):
        #         cnx = ConnexionPostgres()
        #         res = cnx.testconnexion(
        #             utidb, passdb, serveur, port, basedonnees)
        #         print("res: {}".format(res))
        #         return json.dumps(res)

        #     else:
        #         res = {"connexion": "Non defini"}
        #         return json.dumps(res)

        # return jsonify(res)
        # return json.dumps(res)
    except:
        print("__testconnection__c___")
        print(sys.exc_info())
        print("sys.exc_info()[0] :{}".format(str(sys.exc_info()[1])))
        res = {"erreur": str(sys.exc_info()[1])}
        # return json.dumps(res)

        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        erreur = "{}, {}, {}".format(exc_type, fname, exc_tb.tb_lineno)
        return json.dumps({"erreur": erreur})




@app.route('/api/pyflask/testrequete', methods=['POST'])
# @cross_origin()
def testrequete():
    print("__testrequete___")
    try:
        # print("testrequete data: {}".format(request.json))
        data = request.json
        print("testrequete data: {}".format(data))

        # res = testerLaConnexion(data['connexion'])
        res = testerLaConnexion(data)
        print("testrequete res:: {}".format(res))
        # print("res:: {}".format(res['connexion']))

        if(res['connexion'] == 'OK'):
            # res = testerLaRequete(data['connexion'], data['sqlstr'])
            res = testerLaRequete(data)
        # else:
        #     res = {"erreur": str(sys.exc_info()[1]).replace("\'", "")}

        return json.dumps(res)

    except:
        print("__testrequete__c2___")
        print(sys.exc_info())
        print("sys.exc_info()[0] :{}".format(str(sys.exc_info()[1])))
        res = {"erreur": str(sys.exc_info()[1])}
        # return json.dumps(res)

        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        erreur = "{}, {}, {}".format(exc_type, fname, exc_tb.tb_lineno)
        erreur = "{}, {}, {}".format(
            str(sys.exc_info()[1]), fname, exc_tb.tb_lineno)
        return json.dumps({"erreur": erreur})


@app.route('/api/pyflask/generatefile', methods=['POST'])
# @cross_origin()
def generatefile():
    print(__name__)
    print("generatefile")
    retour = None

    data = request.json
    print("data: {}".format(data))

    try:

        # res = testerLaConnexion(data['connexion'])
        res = testerLaConnexion(data)
        print("testerLaConnexion:: {}".format(res))
        # print("res:: {}".format(res['connexion']))

        if(res['connexion'] == 'OK'):
            res = testerLaRequete(data)
            print("testerLaRequete:: {}".format(res))

            if(res['nombreligne'] > 0):
                # print("OKOKO")
                print("genererLeFichier(data) data: {}".format(data))
                res = genererLeFichier(data)
        # else:
        retour = res

        res = {"generation": "OK"}
    except:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print(sys.exc_info())
        print("sys.exc_info()[0] :{}".format(str(sys.exc_info()[1])))
        retour = {
            "erreur": "Programme: {} line: {}, Erreur: {}, Message: {}".format(fname, exc_tb.tb_lineno, exc_type, sys.exc_info()[1])}

    print(retour)
    return json.dumps(retour)


@app.route('/api/pyflask/createxmlfile', methods=['POST'])
# @cross_origin()
def createxmlfile():
    print(__name__)
    print("createxmlfile")

    print("createxmlfile data: {}".format(request.json))
    data = request.json
    print("createxmlfile data: {}".format(data))

    nom = data['nom']
    serveur = data['serveur']
    port = data['port']
    utidb = data['utidb']
    passdb = data['passdb']
    basedonnees = data['basedonnees']

    print(nom, serveur, port, utidb, passdb, basedonnees)
    cnx = ConnexionPostgres(utidb, passdb, serveur, port, basedonnees)
    res = cnx.testconnexion(utidb, passdb, serveur, port, basedonnees)
    print("testconnexion res: {}".format(res))
    # exit

    # 'nom': 'CONNEXION POSTGRES', 'serveur': '192.168.56.220', 'port': '5432', 'utidb': 'postgres', 'passdb': 'postgres', 'basedonnees': 'reportsdb',

    # res = testerLaConnexion(data)
    # print("testconnection res: {}".format(res))
    # cnx = ConnexionSqlServer(utidb, passdb, serveur, port, basedonnees)

    # sqlstr = "select * from requete"
    # res = cnx.testrequete(sqlstr)
    # print("testrequete res: {}".format(res))

    cnx = ConnexionPostgres(utidb, passdb, serveur, port, basedonnees)
    # res = cnx.testconnexion(utidb, passdb, serveur, port, basedonnees)

    # res = {"createxmlfile": "OK"}

    Courant = (config['Paths']['Courant'])
    Path(Courant).mkdir(parents=True, exist_ok=True)

    nom_fic_droits = (config['Fichiers']['NomFichierDroits'])

    res = cnx.generatexmlfile(Courant, nom_fic_droits)
    # res = createxmlfile()
    return json.dumps(res)


# if __name__ == "__main__":
#     # app.run(host="srvexploit", port=5000, debug=True)
#     # app.run(host="127.0.0.1", port=5000, debug=True)
#     app.run(host="192.168.56.220", port=5000, debug=True)

#     # print("apppp")
