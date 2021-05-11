import sys

sys.path.append("/var/www/html/bbg-reports/flask/venv2")
sys.path.append("/var/www/html/bbg-reports/flask/venv2/lib/python3.6/site-packages")

def query():
    import cx_Oracle
    db = cx_Oracle.connect("fikouame_prod", "@Koufide%2021$", "192.168.2.204/EXP")
    cursor = db.cursor()
    cursor.execute("select typcptlib from cgb.typcpt ")
    # return cursor.fetchone()[0]
    # return cursor.fetchall()
    rows =  cursor.fetchall()
    for row in rows:
        print("KF row: {}".format(row[0]))
        return row[0]

def wsgi_test(environ, start_response):
    output = query()
    output_b =  bytes(output, encoding="utf-8")
    print("KF output: {}".format(output))
    # output = b'TEST'
    status = '200 OK'
    # headers = [('Content-type', 'text/plain'), ('Content-Length', str(len(output)))]
    headers = [('Content-type', 'text/plain'), ('Content-Length', str(len(output_b)))]
    start_response(status, headers)
    # yield output
    # return bytes(output,encoding="utf-8")
    return [output_b]

application = wsgi_test





# def query():
#     import cx_Oracle
#     db = cx_Oracle.connect("fikouame_prod", "@Koufide%2021$", "192.168.2.204/EXP")
#     cursor = db.cursor()
#     cursor.execute("select typcpt from cgb.typcpt")
#     return cursor.fetchone()[0]

# def wsgi_test(environ, start_response):
#     output = query()
#     output ="OK"
#     status = '200 OK'
#     headers = [('Content-type', 'text/plain'), ('Content-Length', str(len(output)))]
#     start_response(status, headers)


#     print("KF {}".format(sys.prefix))
#     print("KF {}".format(sys.path))

#     yield output

# application = wsgi_test