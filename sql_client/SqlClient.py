from __future__ import print_function
__author__ = 'daniel.ricart'
import pypyodbc
import re


class SqlClient:

    def __init__(self, server, username, password, database, port=1433, auto_connect=True):
        self.server = server
        self.username = username
        self.password = password
        self.database = database
        self.port = port
        self.query = ""
        if auto_connect:
            self.conn = \
                pypyodbc.connect("DRIVER={SQL Server};server=%s;port=%s;uid=%s;pwd=%s;database=%s;readonly=True" %
                                 (self.server, self.port, self.username, self.password, self.database), timeout=60)
        else:
            self.conn = False

    def connect(self):
        if self.conn is False:
            self.conn = \
                pypyodbc.connect("DRIVER={SQL Server};Server=%s;port=%s;uid=%s;pwd=%s;database=%s;readonly=true" %
                                 (self.server, self.port, self.username, self.password, self.database), timeout=60)

    def run_query(self, query, single_value = True):
        self.query = query
        r = re.compile("^select")
        if r.match(query) is None:
            return False
        try:
            if single_value:
                return self.conn.cursor().execute(self.query).fetchone()[0]
            else:
                result = self.conn.cursor().execute(self.query).fetchall()
                return result
        except pypyodbc.ProgrammingError as e:
            return "Cannot execute query. Programming error: %s" % e.value
        except Exception as e:
            print(e)
            raise

    def run_write(self, query):
        # TODO: make sure query IS an update/insert/create/drop
        r = re.compile("update * set")
        if r.match(query) is not None:
            return False
        self.query = query
        try:
            self.conn.cursor().execute(self.query)
            return self.conn.cursor().commit()
        except pypyodbc.ProgrammingError as e:
            if "permission was denied" in e.value:
                return "Insufficient permissions"
            else:
                return "Error type: %s . Programming error: %s" % (type(e), e)
