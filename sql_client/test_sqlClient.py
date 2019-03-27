from unittest import TestCase
import sql_client
import pypyodbc
import nose

__author__ = 'daniel.ricart'


class TestSqlClient(TestCase):
    sql_host = "sql.local"
    sql_user = "test_runner"
    sql_password = "test_password"
    sql_database = "testing_database"
    def setup(self):
        print("SETUP!")

    def teardown(self):
        print("TEAR DOWN!")

    def test_basic(self):
        print("I RAN!")

    def test_auto_connect(self):
        sql = sql_client.SqlClient.SqlClient(self.sql_host, self.sql_user, self.sql_password, self.sql_database)
        self.assertIsInstance(sql.conn, pypyodbc.Connection)

    def test_manual_connect(self):
        sql = sql_client.SqlClient.SqlClient(self.sql_host, self.sql_user, self.sql_password, self.sql_database, auto_connect=False)
        self.assertFalse(sql.conn)
        sql.connect()
        self.assertIsInstance(sql.conn, pypyodbc.Connection)

    def test_connection_failure_invalid_connection(self):
        with self.assertRaises(pypyodbc.DatabaseError):
            sql_client.SqlClient.SqlClient(self.sql_host, "InvalidUser", "InvalidPassword", self.sql_database)

    def test_run_query(self):
        sql = sql_client.SqlClient.SqlClient(self.sql_host, self.sql_user, self.sql_password, self.sql_database, auto_connect=True)
        res, col = sql.run_query("select count(*) from PERSON")
        self.assertEquals(res, 9)

    def test_run_write_insufficient_permissions(self):
        sql = sql_client.SqlClient.SqlClient(self.sql_host, self.sql_user, self.sql_password, self.sql_database, auto_connect=True)
        # with self.assertRaises(pypyodbc.ProgrammingError):
        response = sql.run_write("update PERSON set name = 'Axel' where id = 1")
        print(response)
        self.assertTrue("Insufficient permissions" in response)



# if sql cannot connect to database, sql.conn is not an object (raises AttributeError exception)
# if sql is not connected to the database, sql.conn is False
# if sql is connected to the database, sql.conn is a pypyodbc.Connection object