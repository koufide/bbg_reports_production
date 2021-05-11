import sys


def query():
    import cx_Oracle
    db = cx_Oracle.connect("hr", "hr", "DB11G.srvoracle.koufide.com")
    cursor = db.cursor()
    cursor.execute("select city from locations where location_id = 2200")
    return cursor.fetchone()[0]

def wsgi_test(environ, start_response):
    output = query()
    status = '200 OK'
    headers = [('Content-type', 'text/plain'), ('Content-Length', str(len(output)))]
    start_response(status, headers)
    yield output

application = wsgi_test

# def application(environ, start_response):
#     status = '200 OK'
#     output = b'Hello World!'

#     html = "This "
#     html += "is the code"
#     html = bytes(html, encoding= 'utf-8')

#     print("KF PREF : {} " .format(repr(sys.prefix)))
#     print("KF PATH : {}  " .format(repr(sys.path)))

#     response_headers = [('Content-type', 'text/plain'),
#                         ('Content-Length', str(len(output)))]
#     start_response(status, response_headers)

#     # return [output]
#     # return [html, output]
#     yield html


# def application(environ, start_response):
#     status = "200 OK"
#     output = b"Hello World! FIDELIN"

#     response_headers = [
#         ("Content-type", "text/plain"),
#         ("Content-Length", str(len(output))),
#     ]
#     start_response(status, response_headers)

#     return [output]