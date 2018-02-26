import json
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
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--file', nargs="?")
    group.add_argument("query", nargs="?")
    arguments = parser.parse_args()

    return arguments


def main():

    args = get_arguments()

    password = args.password

    query_source = []
    if "file" in args:
        with open(args.file, encoding='utf-8') as json_file:
            query_source = json.load(json_file)
    else:
        query_source.append(
            {
                "namespace": args.metric_name,
                "query": args.query
            }
        )
        
    try:
        sql = sql_client.SqlClient.SqlClient(args.host, args.username, password, args.database)
    except (pypyodbc.DatabaseError, pypyodbc.DataError) as e:
        print("error connecting")
        print(type(e))
        print(e)
        sys.exit(errno.EACCES)

    query_builder = QueryBuilder.QueryBuilder()
    queries = query_builder.check(query_source)
    result = []
    for query in queries:
        try:
            result_query = sql.run_query(query["query"])
        except:
            print("Error executing namespace {} - query: {} ".format(query["namespace"], query["query"]))
            result_query = None

        single_value = isinstance(result_query, int)
        if not single_value:
            for k, v in result_query:
                current_namespace = (".".join([query["namespace"], k.replace(".", "_")])).lower()
                result.append({
                    "namespace": current_namespace,
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
