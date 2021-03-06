from __future__ import print_function

import random
from pprint import pprint


class QueryBuilder:
    def __init__(self):
        return

    @staticmethod
    def check(query):
        results = []

        if isinstance(query, list):
            for item in query:
                results.extend(QueryBuilder.check(item))
        elif not isinstance(query, dict):
            raise TypeError("expected dict{{namespace, query, [parameters]}}. Got {}".format(type(query)))
        elif isinstance(query, dict) and len(query) > 0:
            if "parameters" in query:
                for param_name, parameter in query["parameters"].items():
                    current_query = query["query"].format(parameter)
                    current_namespace = query["namespace"].format(param_name)
                    if current_namespace == query["namespace"]:
                        current_namespace = ".".join([query["namespace"], param_name])
                    # print("{0} {1}".format(current_namespace, current_query))
                    query_format = "namespace"
                    if "format" in query:
                        query_format = query["format"]
                    results.append({"namespace": current_namespace, "query": current_query, "format": query_format})
            else:
                query_format = "namespace"
                if "format" in query:
                    query_format = query["format"]
                results.append({"namespace": query["namespace"], "query": query["query"], "format": query_format})
        return results
