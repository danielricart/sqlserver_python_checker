from pprint import pprint
from unittest import TestCase
import query_builder
import nose

from query_builder.QueryBuilder import QueryBuilder

__author__ = 'daniel.ricart'


class TestQueryBuilder(TestCase):
    query = {
            "namespace": "my.level1.level2.level3",
            "query": "select count(*) from mydb.dbo.myTable where ModifiedDate < GETDATE()-5 and source = {}",
            "parameters": {
                "source1": "15",
                "source2": "49",
                "source3": "74",
                "source4": "836"}
        }
    query_array = [
        {
            "namespace": "my.level1.level2.level3",
            "query": "select count(*) from mydb.dbo.myTable where ModifiedDate < GETDATE()-5 and source = {}",
            "parameters": {
                "source1": "15",
                "source2": "49",
                "source3": "74",
                "source4": "836"}
        },
        {
            "namespace": "my.level1.level2.level3",
            "query": "select id as [name], count(*) from mydb.dbo.myTable1 table1 left join mydb.dbo.myTable2 table2 on table1.id = table2.id  where ModifiedDate < GETDATE()-5 group by table2.source",
        }
    ]
    query_no_parameters = {
            "namespace": "my.level1.level2.level3",
            "query": "select id as [name], count(*) from mydb.dbo.myTable1 table1 left join mydb.dbo.myTable2 table2 on table1.id = table2.id  where ModifiedDate < GETDATE()-5 group by table2.source",
        }
    query_formatted_namespace = {
            "namespace": "my.level1.level2.{}.level3",
            "query": "select count(*) from mydb.dbo.myTable where ModifiedDate < GETDATE()-5 and source = {}",
            "parameters": {
                "source1": "15"}
        }
    empty_query = {}
    empty_query_list = [{}]

    def setup(self):
        print("SETUP!")

    def teardown(self):
        print("TEAR DOWN!")

    def test_basic(self):
        check = QueryBuilder()
        result = check.check(self.query)
        print(result)

    def test_empty_query(self):
        check = QueryBuilder()
        result = check.check(self.empty_query)
        print(result)
        self.assertListEqual(result, [])

    def test_empty_query_list(self):
        check = QueryBuilder()
        result = check.check(self.empty_query_list)
        print(result)

        self.assertListEqual(result, [])

    def test_invalid_document(self):
        check = QueryBuilder()
        self.assertRaises(TypeError, check.check, "select *")

    def test_query_with_no_parameters(self):
        expected = [
            {
                'namespace': 'my.level1.level2.level3',
                'query': 'select id as [name], count(*) from mydb.dbo.myTable1 table1 left join mydb.dbo.myTable2 table2 on table1.id = table2.id  where ModifiedDate < GETDATE()-5 group by table2.source'
            }]
        check = QueryBuilder()
        result = check.check(self.query_no_parameters)
        print(result)
        self.assertListEqual(result, expected)

    def test_query(self):
        expected = [
            {
                'namespace': 'my.level1.level2.level3.source1',
                'query': 'select count(*) from mydb.dbo.myTable where ModifiedDate < GETDATE()-5 and source = 15'
            }, {
                'namespace': 'my.level1.level2.level3.source2',
                'query': 'select count(*) from mydb.dbo.myTable where ModifiedDate < GETDATE()-5 and source = 49'
            }, {
                'namespace': 'my.level1.level2.level3.source3',
                'query': 'select count(*) from mydb.dbo.myTable where ModifiedDate < GETDATE()-5 and source = 74'
            }, {
                'namespace': 'my.level1.level2.level3.source4',
                'query': 'select count(*) from mydb.dbo.myTable where ModifiedDate < GETDATE()-5 and source = 836'
            }]

        check = QueryBuilder()
        result = check.check(self.query)
        print(result)
        self.assertListEqual(result, expected)

    def test_multiple_queries(self):
        expected = [{
            'namespace': 'my.level1.level2.level3.source1',
            'query': 'select count(*) from mydb.dbo.myTable where ModifiedDate < GETDATE()-5 and source = 15'
        }, {
            'namespace': 'my.level1.level2.level3.source2',
            'query': 'select count(*) from mydb.dbo.myTable where ModifiedDate < GETDATE()-5 and source = 49'
        }, {
            'namespace': 'my.level1.level2.level3.source3',
            'query': 'select count(*) from mydb.dbo.myTable where ModifiedDate < GETDATE()-5 and source = 74'
        }, {
            'namespace': 'my.level1.level2.level3.source4',
            'query': 'select count(*) from mydb.dbo.myTable where ModifiedDate < GETDATE()-5 and source = 836'
        }, {
            'namespace': 'my.level1.level2.level3',
            'query': 'select id as [name], count(*) from mydb.dbo.myTable1 table1 left join mydb.dbo.myTable2 table2 on table1.id = table2.id  where ModifiedDate < GETDATE()-5 group by table2.source'
        }]

        check = QueryBuilder()
        result = check.check(self.query_array)
        print(result)
        self.assertListEqual(result, expected)

    def test_query_with_formatted_namespace(self):
        expected = [
            {
                'namespace': 'my.level1.level2.source1.level3',
                'query': 'select count(*) from mydb.dbo.myTable where ModifiedDate < GETDATE()-5 and source = 15'
            }]
        check = QueryBuilder()
        result = check.check(self.query_formatted_namespace)
        self.assertListEqual(result, expected)
