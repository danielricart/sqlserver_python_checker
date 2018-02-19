import pypyodbc
import stackdriver
import datadog_metrics
from query_builder import QueryBuilder

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
    parser.add_argument("--datadog_apikey", nargs="?", required=False, default=None)
    parser.add_argument("--datadog_appkey", nargs="?", required=False, default=None)
    parser.add_argument("query")
    arguments = parser.parse_args()

    return arguments


def main():

    args = get_arguments()

    password = args.password

    try:
        sql = sql_client.SqlClient.SqlClient(args.host, args.username, password, args.database)
    except (pypyodbc.DatabaseError, pypyodbc.DataError) as e:
        print("error connecting")
        print(type(e))
        print(e)
        sys.exit(errno.EACCES)

    query_builder = QueryBuilder.QueryBuilder()
    queries = query_builder.check(sample_query)
    result = []
    for query in queries:
        single_value = "select count(*) from" in query["query"].lower()
        result_query = sql.run_query(query["query"], single_value=single_value)

        if not single_value:
            for k, v in result_query:
                result.append({
                    "namespace": (".".join([query["namespace"], k.replace(".", "_")])).lower(),
                    "value": v
                })
        else:
            result.append({"namespace": query["namespace"].lower(), "value": result_query})


    if args.datadog_apikey:
        try:
            for metric in result:
                    datadog_metrics.submit_custom_metric(
                        metric["namespace"], metric["value"],
                        args.datadog_apikey, args.datadog_appkey)
        except Exception as e:
            print("ERROR sending to datadog: %s" % e)
            sys.exit(errno.EACCES)


if __name__ == '__main__':
    main()
