import traceback
from abc import ABC, ABCMeta
from functools import reduce
from operator import getitem
from pydoc import locate
from typing import List, Union, Dict, Optional, Iterable
from pymongo import MongoClient
from pymongo.database import Database as MongoDatabase
from pymongo.database import Collection as MongoCollection


class Node(ABC):
    pass


class IdNode(Node, ABC):
    def __init__(self, id_):
        self.id_ = id_


class CheckChildren(ABCMeta):
    def __init__(cls, name, bases, dct, **_):
        super().__init__(name, bases, dct)

        # noinspection PyDecorator
        @classmethod
        def __init_subclass__(cls_, children_type):
            cls_.children_type = children_type

        cls.__init_subclass__ = __init_subclass__


class Branch(dict, ABC, metaclass=CheckChildren):
    children_type = None

    def __init__(self, children: List):
        super().__init__()
        for child in children:
            self.add(child)

    def add(self, child: IdNode, id_=None):
        assert isinstance(child, self.children_type), type(child)
        if id_ is None:
            id_ = child.id_
        if id_ in self:
            raise Exception('Already exist')
        self[id_] = child

    def __getitem__(self, item):
        if isinstance(item, (tuple, list)):
            child = reduce(getitem, item, self)
        else:
            child = dict.__getitem__(self, item)

        if isinstance(child, Branch):
            child.children = [child[child_id] for child_id in child.keys()]
        return child


class IdBranch(IdNode, Branch, children_type=IdNode, metaclass=CheckChildren):
    def __init__(self, id_, children: Optional[List[IdNode]] = None):
        if not children:
            children = []
        IdNode.__init__(self, id_)
        Branch.__init__(self, children)


mongo_client: Optional[MongoClient] = None


class Schema(dict):
    def __init__(self, raw_schema: Dict, mongo_collection: MongoCollection):
        super().__init__()
        if mongo_client:
            self.mongo_collection = mongo_collection
        self.update(raw_schema)
        if not raw_schema:
            self['columns'] = {}
            self['row_index'] = -1

            if mongo_client:
                self.mongo_collection.update_one(
                    {'id': 'schema'},
                    {'$set':
                        {'columns': self['columns'],
                         'row_index': self['row_index']}
                     },
                    upsert=True
                )

    def idx_to_id(self, column_idx):
        return self.column_ids[column_idx]

    def id_to_idx(self, column_id):
        return self.column_ids.index(column_id)

    def validators(self, column_id):
        validators = []
        for validator_def in self['columns'][column_id]['validator_defs']:
            # the module validators contains safe classes
            validator_type = locate(f'validators.{validator_def["name"]}')
            assert isinstance(validator_type, type)

            validators.append(validator_type(**validator_def['params']))
        return validators

    @property
    def column_ids(self):
        return list(self['columns'].keys())

    @property
    def row_index(self):
        return self['row_index']

    @row_index.setter
    def row_index(self, new_index):
        assert 'row_index' not in self or abs(new_index - self['row_index']) == 1
        self['row_index'] = new_index

        if mongo_client:
            self.mongo_collection.update_one(
                {'id': 'schema'},
                {'$set': {'row_index': self['row_index']}},
                upsert=True
            )

    def add(self, column_id: str, validator_defs: List[Dict]):
        assert column_id not in self['columns']
        self['columns'][column_id] = {'validator_defs': validator_defs}

        if mongo_client:
            self.mongo_collection.update_one(
                {'id': 'schema'},
                {'$set': {f'columns.{column_id}': self['columns'][column_id]}},
                upsert=True
            )

    def pop(self, column_id):
        self['columns'].pop(column_id)

        if mongo_client:
            self.mongo_collection.update_one(
                {'id': 'schema'},
                {'$unset': {f'columns.{column_id}': 1}},
                upsert=True
            )

    def update(self, elems, **kwargs):
        super().update(elems)
        if mongo_client and elems:
            self.mongo_collection.update_one({'id': 'schema'}, {'$set': self}, upsert=True)


# class Value(IdNode):
#     @staticmethod
#     def pack(item):
#         return type(item.__class__.__name__ + 'Value', (item.__class__, Value), {})(item)


class Row(IdNode, list):
    def __init__(self, id_: int, schema: Schema, raw_values: Iterable, mongo_collection: MongoCollection):
        IdNode.__init__(self, id_)
        # self.extend([Value.pack(v) for v in raw_values])
        self.extend(raw_values)
        self.schema = schema
        if mongo_client:
            self.mongo_collection = mongo_collection

    def __getitem__(self, item):
        if isinstance(item, int):
            return list.__getitem__(self, item)
        elif isinstance(item, str):
            return list.__getitem__(self, self.schema.id_to_idx(column_id=item))
        else:
            raise

    def __setitem__(self, item, value):
        if isinstance(item, int):
            column_id, column_idx = self.schema.idx_to_id(item), item
        elif isinstance(item, str):
            column_id, column_idx = item, self.schema.id_to_idx(column_id=item)
        else:
            raise
        list.__setitem__(self, column_idx, value)
        if mongo_client:
            self.mongo_collection.update_one({'id': self.id_}, {'$set': {column_id: value}})

    # noinspection PyMethodOverriding
    def pop(self, item):
        if isinstance(item, int):
            return list.pop(self, item)
        elif isinstance(item, str):
            return list.pop(self, self.schema.id_to_idx(column_id=item))
        else:
            raise

    def insert(self, column_idx: int, value):
        list.insert(self, column_idx, value)
        if mongo_client:
            self.mongo_collection.update_one({'id': self.id_}, {'$set': {self.schema.idx_to_id(column_idx): value}})


class Table(IdBranch, children_type=Row):
    def __init__(self, id_: str,
                 raw_schema: Optional[Dict] = None,
                 init_rows: Optional[List[Dict]] = None,
                 init_rows_unsafe: Optional[List] = None,
                 mongo_collection: Optional[MongoCollection] = None):
        super().__init__(id_)
        self.mongo_collection = mongo_collection

        if mongo_client:
            id_rows = list(mongo_collection.find({}, {'_id': 0}))
            schema_idx = [i for i, row in enumerate(id_rows) if row['id'] == 'schema']
            if len(schema_idx) == 0:
                raw_schema = {}
            elif len(schema_idx) == 1:
                raw_schema = id_rows.pop(schema_idx[0])
                raw_schema.pop('id')
            else:
                raise

        if not raw_schema:
            raw_schema = {}
        self.schema = Schema(raw_schema, mongo_collection)

        if mongo_client:
            for row in id_rows:
                row_id = row.pop('id')
                self.add(row, row_id, init_fill=True)
        if init_rows:
            for r in init_rows:
                self.add(r)
        if init_rows_unsafe:
            for r in init_rows_unsafe:
                self.add(dict(zip(self.schema.column_ids, r)))

    def add(self, raw_row: Dict, row_id=None, init_fill=False):
        if not init_fill:
            assert list(raw_row.keys()) == self.schema.column_ids
            assert all(validator(val) for column_id, val in raw_row.items()
                       for validator in self.schema.validators(column_id))
            if mongo_client:
                self.mongo_collection.insert_one({'id': self.schema.row_index + 1, **raw_row})

        if row_id is None:
            row = Row(self.schema.row_index + 1, self.schema, raw_row.values(), self.mongo_collection)
            self.schema.row_index += 1
        else:
            row = Row(row_id, self.schema, raw_row.values(), self.mongo_collection)
        IdBranch.add(self, row)

    def add_column(self, column_id: str, description: Dict):
        assert column_id not in self.schema.column_ids

        if 'values' in description:
            values = description['values']
            assert len(values) == len(self)
        else:
            values = []

        validator_defs = description['validator_defs'] if 'validator_defs' in description else []
        self.schema.add(column_id, validator_defs)

        # assert idx == self.schema.id_to_idx(column_id)  # todo move to tests

        column_idx = self.schema.id_to_idx(column_id)
        for row, val in zip(self.values(), values):
            row.insert(column_idx, val)

    def __getitem__(self, item):
        if isinstance(item, int):
            return dict.__getitem__(self, item)
        elif isinstance(item, str):
            return {row_id: self[row_id][item] for row_id in self.keys()}
        else:
            raise

    def pop(self, item):
        if isinstance(item, int):
            dict.pop(self, item)
        elif isinstance(item, str):
            for row in self.values():
                row.pop(item)
            self.schema.pop(item)
        else:
            raise


class Base(IdBranch, children_type=Table):
    def __init__(self, id_: str,
                 children: Optional[List[Table]] = None,
                 mongo_base: Optional[MongoDatabase] = None):
        super().__init__(id_, children)
        if mongo_client:
            self.mongo_base = mongo_base

    def __getitem__(self, table_id):
        if mongo_client:
            return Table(id_=table_id, mongo_collection=self.mongo_base[table_id])
        else:
            return dict.__getitem__(self, table_id)

    def pop(self, table_id):
        dict.pop(self, table_id)
        if mongo_client:
            self.mongo_base.drop_collection(table_id)


class Root(Branch, children_type=Base):
    def pop(self, base_id):
        dict.pop(self, base_id)
        if mongo_client:
            mongo_client.drop_database(base_id)

    def create(self, path: List[str]) -> bool:
        try:
            branch = self.read(path[:-1])
            id_ = path[-1]

            if mongo_client:
                if isinstance(branch, Root):
                    mongo = {'mongo_base': mongo_client[id_]}
                elif isinstance(branch, Base):
                    mongo = {'mongo_collection': mongo_client[branch.id_][id_]}
                else:
                    mongo = {}
            else:
                mongo = {}

            branch.add(branch.children_type(id_=id_, **mongo))
            return True
        except Exception:
            print(traceback.format_exc())
            return False

    def create_rows(self, table_path: List[str], rows: List[Dict]) -> bool:
        try:
            table: Table = self.read(table_path)
            for r in rows:
                table.add(r)
            return True
        except Exception:
            print(traceback.format_exc())
            return False

    def create_columns(self, table_path: List[str], columns: Dict[str, Dict]) -> bool:
        try:
            table: Table = self.read(table_path)
            for column_id, description in columns.items():
                table.add_column(column_id, description)
            return True
        except Exception:
            print(traceback.format_exc())
            return False

    def read(self, path: List[str]) -> Union['Root', Table, Row]:
        return self[path]

    def read_columns(self, table_path: List[str]) -> Dict:
        table: Table = self.read(table_path)
        return dict(zip(table.schema.column_ids, zip(*table.values())))

    def read_column(self, table_path: List[str], column_id: str) -> List:
        table: Table = self.read(table_path)
        column_idx = table.schema.id_to_idx(column_id)
        return [row[column_idx] for row in table]

    def update_row(self, table_path: List[str], row_id: int, sub_row: Dict):
        try:
            table: Table = self.read(table_path)
            assert set(sub_row.keys()) <= set(table.schema.column_ids)
            for column_id in sub_row.keys():
                table[row_id][column_id] = sub_row[column_id]
            return True
        except Exception:
            print(traceback.format_exc())
            return False

    def update_column(self, table_path: List[str], column_id: str, column: Dict):
        try:
            table: Table = self.read(table_path)
            assert set(column.keys()) <= set(table.keys())
            for row_id in column.keys():
                table[row_id][column_id] = column[row_id]
            return True
        except Exception:
            print(traceback.format_exc())
            return False

    def delete(self, path: List[str]) -> bool:
        try:
            self.read(path[:-1]).pop(path[-1])
            return True
        except Exception:
            print(traceback.format_exc())
            return False

    def intersect_tables(self, by_column_id: str, table1_path: List[str], table2_path: List[str],
                         new_table_path: List[str]) -> bool:
        try:
            tb1, tb2 = self.read(table1_path), self.read(table2_path)

            tb1_column_ids = tb1.schema.column_ids
            tb2_column_ids = tb2.schema.column_ids

            assert set(tb1_column_ids).intersection(tb2_column_ids) >= {by_column_id}, \
                (by_column_id, tb1_column_ids, tb2_column_ids)

            self.create(new_table_path)
            new_column_ids = tb1_column_ids + tb2_column_ids
            new_column_ids.remove(by_column_id)
            self.create_columns(new_table_path, {column_id: {} for column_id in new_column_ids})

            new_table = self.read(new_table_path)
            for row1 in tb1.values():
                for row2 in tb2.values():
                    if row1[by_column_id] == row2[by_column_id]:
                        new_row = {column_id: row1[column_id] if column_id in tb1_column_ids else row2[column_id]
                                   for column_id in new_column_ids}
                        new_table.add(new_row)

            return True
        except Exception as e:
            print(traceback.format_exc())
            return False


def create_tree() -> Root:
    if mongo_client:
        return Root(children=[
            Base(
                base_id,
                children=[Table(table_id, mongo_collection=mongo_client[base_id][table_id])
                          for table_id in mongo_client[base_id].list_collection_names()],
                mongo_base=mongo_client[base_id],
            ) for base_id in set(mongo_client.list_database_names()) - {'admin', 'config', 'local'}
        ])
    else:
        return Root(children=[
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
                    init_rows_unsafe=[(1, 4), (2, 5), (3, 6)]
                )
            ]),
            Base('db2', children=[
                Table('tb3',
                      raw_schema={'column_index': 0, 'row_index': -1,
                                  'columns': {'co3': {'validator_defs': []}}})
            ])
        ])
