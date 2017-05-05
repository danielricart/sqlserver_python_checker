import pypyodbc
import stackdriver
__author__ = 'daniel.ricart'
import sql_client
import argparse
import errno
import sys
"""
arguments:
server
username
password
database
query

"""

"""
ArgumentParser.add_argument(name or flags...[, action][, nargs][, const][, default][, type][, choices][, required][, help][, metavar][, dest])
"""


def get_arguments():
    parser = argparse.ArgumentParser(description="Query SQLServer database for fetching some counters.")
    parser.add_argument("--host", nargs="?", required=True)
    parser.add_argument("--username", nargs="?", required=True)
    parser.add_argument("--password", nargs="?", required=True)
    parser.add_argument("--database", nargs="?", required=True)
    parser.add_argument("--metric_name", nargs="?", required=True)
    parser.add_argument("--stackdriver_api", nargs="?", required=True)
    parser.add_argument("query")
    arguments = parser.parse_args()

    return arguments


def main():

    args = get_arguments()

    password = args.password

    sql = sql_client.SqlClient
    try:
        sql = sql_client.SqlClient.SqlClient(args.host, args.username, password, args.database)
    except (pypyodbc.DatabaseError, pypyodbc.DataError) as e:
        print("error connecting")
        print(type(e))
        print(e)
        sys.exit(errno.EACCES)

    metric_value = sql.run_query(args.query)
    print(metric_value)
    # print(sql.run_write("update PERSON set name = 'Axel' where id = 1"))
    try:
        stackdriver.submit_custom_metric(args.metric_name, metric_value, args.stackdriver_api)
    except Exception as e:
        print("ERROR sending to stackdriver: %s" % e)
        sys.exit(errno.EACCES)


if __name__ == '__main__':
    main()