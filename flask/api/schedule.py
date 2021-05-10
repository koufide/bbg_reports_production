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


def main():
    from mesmodules.ConnexionPostgres import ConnexionPostgres
    import configparser

    config = configparser.ConfigParser()
    config.read('/var/www/html/bbg-reports/flask/api/mesmodules/config.ini')

    serveur = config['ConnexionPostgres']['Serveur']
    port = config['ConnexionPostgres']['Port']
    basedonnees = config['ConnexionPostgres']['BaseDonnees']
    utidb = config['ConnexionPostgres']['Username']
    passdb = config['ConnexionPostgres']['Password']

    print(serveur, port, basedonnees, utidb, passdb)

    cnx = ConnexionPostgres(
        utidb, passdb, serveur, port, basedonnees)

    res_cnx = cnx.testconnexion(
        utidb, passdb, serveur, port, basedonnees)
    
    print("cnx.testconnexion: ", res_cnx)

    if(res_cnx['connexion']=='OK'):
        cnx.getDeclencheurs()
    else:
        print("Impossible de se connecter: {},{},{},{}".format(serveur, port, basedonnees, utidb))


if(__name__=='__main__'):
    print("==================== DEMARRAGE ====================")
    # print(sys.version_info)
    # print(this_python)
    main()
    print("==================== FIN ==========================")
