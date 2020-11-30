from unittest import TestCase

import requests

import api
from api import Root, Base, Table
import client


class TestTable(TestCase):
    def setUp(self):
        api.mongo_client = None
        # noinspection DuplicatedCode
        self.tree = Root(children=[
            Base('db1', children=[
                Table(
                    'tb1',
                    raw_schema={'column_index': 1, 'row_index': 2,
                                'columns': {'co1': {'validator_defs': []},
                                            'co2': {'validator_defs': []}}},
                    init_rows_unsafe=[(1, 4), (2, 5), (3, 6)]
                ),
                Table(
                    'tb2',
                    raw_schema={'column_index': 1, 'row_index': 2,
                                'columns': {'co1': {'validator_defs': []},
                                            'co2': {'validator_defs': []}}},
                    init_rows_unsafe=[(1, 4), (2, 5), (3, 7)]
                )
            ]),
            Base('db2')
        ])

    def test_intersect(self):
        assert self.tree.intersect_tables(by_column_id='co2',
                                          table1_path=['db1', 'tb1'],
                                          table2_path=['db1', 'tb2'],
                                          new_table_path=['db2', 'tb1'])
        t = self.tree['db2', 'tb1']
        assert list(t.values()) == [[1, 4], [2, 5]]

    def test_read(self):
        assert list(self.tree.read(['db1', 'tb1', 'co1']).values()) == [1, 2, 3]

    def test_create(self):
        assert self.tree.create(['db1', 'tb3'])
        self.tree.read(['db1', 'tb3'])


class TestRest(TestCase):
    @classmethod
    def setUpClass(cls):
        requests.delete('http://localhost:5000/tree/db_test/')
        requests.delete('http://localhost:5000/tree/db_test1/')

    def test1_create(self):
        assert requests.post('http://localhost:5000/tree/db_test/').json()['success']
        assert requests.get('http://localhost:5000/tree/db_test/').ok
        assert requests.post('http://localhost:5000/tree/db_test/tb_test/').json()['success']
        assert requests.get('http://localhost:5000/tree/db_test/tb_test/rows/').ok
        type_validator_def = {'name': 'TypeValidator', 'params': {'type_descr': 'int'}}
        assert requests.post('http://localhost:5000/tree/db_test/tb_test/columns/',
                             json={'co1': {'validator_defs': [type_validator_def]}, 'co2': {}}).json()['success']
        assert requests.post('http://localhost:5000/tree/db_test/tb_test/rows/',
                             json=[{'co1': 1, 'co2': 2}, {'co1': 3, 'co2': 4}]).json()['success']
        r1 = requests.get('http://localhost:5000/tree/db_test/tb_test/rows/')
        assert list(r1.json().values())[-1] == [3, 4], (r1.ok, r1.json())
        assert requests.post('http://localhost:5000/tree/db_test/tb_test/columns/',
                             json={'co3': {'values': [5, 6]}}).json()['success']
        r2 = requests.get('http://localhost:5000/tree/db_test/tb_test/rows/')
        assert list(r2.json().values()) == [[1, 2, 5], [3, 4, 6]], (r2.ok, r2.json())
        r3 = requests.get('http://localhost:5000/tree/db_test/tb_test/columns/')
        assert list(r3.json().values()) == [[1, 3], [2, 4], [5, 6]], (r3.ok, r3.json())

        assert requests.post('http://localhost:5000/tree/db_test1/').json()['success']
        assert requests.post('http://localhost:5000/tree/db_test1/tb_test1/').json()['success']
        assert requests.post('http://localhost:5000/tree/db_test1/tb_test1/columns/',
                             json={'co1': {}, 'co4': {}}).json()['success']
        assert requests.post('http://localhost:5000/tree/db_test1/tb_test1/rows/',
                             json=[{'co1': 3, 'co4': 4}, {'co1': 5, 'co4': 6}]).json()['success']
        assert requests.post('http://localhost:5000/tree/intersect_tables/',
                             json={'by_column_id': 'co1',
                                   'table1_path': ['db_test', 'tb_test'],
                                   'table2_path': ['db_test1', 'tb_test1'],
                                   'new_table_path': ['db_test1', 'tb_test2']}).json()['success']

        r4 = requests.get('http://localhost:5000/tree/db_test1/tb_test2/rows/')
        assert sorted(list(r4.json().values())[0]) == [3, 4, 4, 6], (r4.ok, r4.json())

    def test2_read(self):
        assert requests.get('http://localhost:5000/tree/').ok
        assert requests.get('http://localhost:5000/tree/db_test/').ok
        assert requests.get('http://localhost:5000/tree/db_test/tb_test/rows/').ok
        assert requests.get('http://localhost:5000/tree/db_test/tb_test/rows/0/').ok
        assert requests.get('http://localhost:5000/tree/db_test/tb_test/rows/0/co1/').ok
        assert requests.get('http://localhost:5000/tree/db_test/tb_test/columns/').ok
        assert requests.get('http://localhost:5000/tree/db_test/tb_test/columns/co1/').ok
        assert requests.get('http://localhost:5000/tree/db_test/tb_test/columns/co1/0/').ok

    def test3_update(self):
        assert requests.put('http://localhost:5000/tree/db_test/tb_test/rows/0/', json={'co1': 7}).json()['success']
        assert requests.get('http://localhost:5000/tree/db_test/tb_test/rows/0/co1/').json() == 7

    def test4_delete(self):
        assert requests.delete('http://localhost:5000/tree/db_test/tb_test/columns/co1/').json()['success']
        assert requests.delete('http://localhost:5000/tree/db_test/tb_test/rows/0/').json()['success']
        assert requests.delete('http://localhost:5000/tree/db_test/tb_test/').json()['success']
        assert requests.delete('http://localhost:5000/tree/db_test/').json()['success']
        assert not requests.get('http://localhost:5000/tree/db_test/').ok


class TestGRPC(TestCase):
    @classmethod
    def setUpClass(cls):
        client.delete_base('db_test')
        client.delete_base('db_test1')

    def test1_post(self):
        assert client.create_base('db_test').success
        assert client.read_base('db_test')
        assert client.create_table('db_test', 'tb_test').success
        assert client.read_rows('db_test', 'tb_test')
        type_validator_def = {'name': 'TypeValidator', 'params': {'type_descr': 'int'}}
        assert client.create_columns('db_test', 'tb_test',
                                     {'co1': {'validator_defs': [type_validator_def]}, 'co2': {}}).success
        assert client.create_rows('db_test', 'tb_test', [{'co1': 1, 'co2': 2}, {'co1': 3, 'co2': 4}]).success

        r1 = client.read_rows('db_test', 'tb_test')
        type_rows = [[[elem.ListFields()[0][1]
                       for elem in type_value_pair.list_value.values]
                      for type_value_pair in row.list.values]
                     for row in r1.children.nodes]

        assert client.from_type_pairs(type_rows[-1]) == [3, 4], r1
        assert client.create_columns('db_test', 'tb_test', {'co3': {'values': [5, 6]}}).success

        r2 = client.read_rows('db_test', 'tb_test')
        assert [client.from_type_pairs(r.list) for r in r2.children.nodes] == [[1, 2, 5], [3, 4, 6]], r2

        assert client.create_base('db_test1').success
        assert client.create_table('db_test1', 'tb_test1').success
        assert client.create_columns('db_test1', 'tb_test1', {'co1': {}, 'co4': {}}).success
        assert client.create_rows('db_test', 'tb_test', [{'co1': 3, 'co4': 4}, {'co1': 5, 'co4': 6}]).success

        assert client.intersect_tables(by_column_id='co1',
                                       table1_path=['db_test', 'tb_test'],
                                       table2_path=['db_test1', 'tb_test1'],
                                       new_table_path=['db_test1', 'tb_test2'])

        r3 = client.read_rows('db_test1', 'tb_test2')
        assert sorted(list(r3.values())[0]) == [3, 4, 4, 6], r3

    def test2_get(self):
        assert client.read_tree()
        assert client.read_base('db_test')
        assert client.read_rows('db_test', 'tb_test')
        assert client.read_row('db_test', 'tb_test', row_id=0)
        assert client.read_columns('db_test', 'tb_test')
        assert client.read_column('db_test', 'tb_test', 'co1')
        assert client.read_value('db_test', 'tb_test', row_id=0, column_id='co1')

    def test3_put(self):
        assert client.update_row('db_test', 'tb_test', row_id=0, sub_row={'co1': 7})
        assert client.read_value('db_test', 'tb_test', row_id=0, column_id='co1') == 7

    def test4_delete(self):
        assert client.delete_column('db_test', 'tb_test', 'co1')
        assert client.delete_row('db_test', 'tb_test', row_id=0)
        assert client.delete_table('db_test', 'tb_test')
        assert client.delete_base('db_test')
