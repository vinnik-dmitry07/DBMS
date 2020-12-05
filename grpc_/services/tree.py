from pydoc import locate
from types import ModuleType

from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct, ListValue

import grpc_.messages.tree_pb2 as tree_messages
import grpc_.messages.tree_pb2_grpc as tree_service


def to_type_pair(value):
    return [type(value).__name__, value]


def to_type_pairs(values):
    return list(map(to_type_pair, values))


def from_type_pair(pair):
    type_, value = pair
    # noinspection PyCallingNonCallable
    return locate(type_)(value)


def from_type_pairs(pairs):
    return list(map(from_type_pair, pairs))


def to_jsonable_rows(rows):
    return {str(row_id): to_type_pairs(row) for row_id, row in rows.items()}


def from_jsonable_rows(rows):
    return {int(row_id): from_type_pairs(pairs) for row_id, pairs in rows.items()}


def to_jsonable_base(base):
    return {table_id: to_jsonable_rows(rows) for table_id, rows in base.items()}


def from_jsonable_base(base):
    return {table_id: from_jsonable_rows(rows) for table_id, rows in base.items()}


def to_jsonable_tree(tree):
    return {base_id: to_jsonable_base(base) for base_id, base in tree.items()}


def from_jsonable_tree(tree):
    return {base_id: from_jsonable_base(base) for base_id, base in tree.items()}


def to_jsonable_row(row):
    return {column_id: to_type_pair(value) for column_id, value in row.items()}


def from_jsonable_row(row):
    return {column_id: from_type_pair(pair) for column_id, pair in row.items()}


def to_jsonable_column(column):
    return {str(row_id): to_type_pair(value) for row_id, value in column.items()}


def from_jsonable_column(column):
    return {int(row_id): from_type_pair(pair) for row_id, pair in column.items()}


def to_jsonable_columns(columns):
    return {column_id: to_type_pairs(values) for column_id, values in columns.items()}


def from_jsonable_columns(columns):
    return {column_id: from_type_pairs(pairs) for column_id, pairs in columns.items()}


class TreeServicer(tree_service.TreeServicer):
    ROW_DEPTH = 2

    def __init__(self, api: ModuleType):
        self.tree = api.create_tree()

    def create(self, path):
        resp = tree_messages.SuccessResponse()
        resp.success = self.tree.create(MessageToDict(path))
        return resp

    def CreateBase(self, request, context):
        return self.create(request.path)

    def CreateTable(self, request, context):
        return self.create(request.path)

    def CreateRows(self, request, context):
        resp = tree_messages.SuccessResponse()
        table_path = list(request.table_path)
        rows = [from_jsonable_row(MessageToDict(row_message)) for row_message in request.rows]
        resp.success = self.tree.create_rows(table_path, rows)
        return resp

    def CreateColumns(self, request, context):
        resp = tree_messages.SuccessResponse()
        columns = MessageToDict(request)['columns']
        for column in columns.values():
            if 'values' in column:
                column['values'] = from_type_pairs(column['values'])
        resp.success = self.tree.create_columns(list(request.table_path), columns)
        return resp

    def _create_jsonable_struct(self, request, converter):
        resp = Struct()
        resp.update(converter(self.tree.read(list(request.path))))
        return resp

    def ReadTree(self, request, context):
        return self._create_jsonable_struct(request, to_jsonable_tree)

    def ReadBase(self, request, context):
        return self._create_jsonable_struct(request, to_jsonable_base)

    def ReadRows(self, request, context):
        return self._create_jsonable_struct(request, to_jsonable_rows)

    def ReadColumns(self, request, context):
        resp = Struct()
        resp.update(to_jsonable_columns(self.tree.read_columns(list(request.path))))
        return resp

    def ReadRow(self, request, context):
        row_path = list(request.path)
        row_path[self.ROW_DEPTH] = int(row_path[self.ROW_DEPTH])

        resp = ListValue()
        resp.extend(to_type_pairs(self.tree.read(row_path)))
        return resp

    def ReadColumn(self, request, context):
        column_path = list(request.path)
        table_path, column_id = column_path[:-1], column_path[-1]

        resp = ListValue()
        resp.extend(to_type_pairs(self.tree.read_column(table_path, column_id)))
        return resp

    def ReadValue(self, request, context):
        value_path = list(request.path)
        value_path[self.ROW_DEPTH] = int(value_path[self.ROW_DEPTH])

        resp = ListValue()
        resp.extend(to_type_pair(self.tree.read(value_path)))
        return resp

    def ReadSchema(self, request, context):
        resp = Struct()
        resp.update(self.tree.read_schema(table_path=list(request.path)))
        return resp

    def UpdateRow(self, request, context):
        resp = tree_messages.SuccessResponse()
        sub_row = from_jsonable_row(MessageToDict(request.sub_row))
        resp.success = self.tree.update_row(list(request.table_path), request.row_id, sub_row)
        return resp

    def UpdateColumn(self, request, context):
        resp = tree_messages.SuccessResponse()
        sub_column = from_jsonable_column(MessageToDict(request.sub_column))
        resp.success = self.tree.update_column(list(request.table_path), request.column_id, sub_column)
        return resp

    def delete(self, path):
        path = list(path)
        resp = tree_messages.SuccessResponse()
        resp.success = self.tree.delete(path)
        return resp

    def DeleteBase(self, request, context):
        return self.delete(request.path)

    def DeleteTable(self, request, context):
        return self.delete(request.path)

    def DeleteRow(self, request, context):
        path = list(request.path)
        path[self.ROW_DEPTH] = int(path[self.ROW_DEPTH])
        return self.delete(path)

    def DeleteColumn(self, request, context):
        return self.delete(request.path)

    def IntersectTables(self, request, context):
        resp = tree_messages.SuccessResponse()
        resp.success = self.tree.intersect_tables(request.by_column_id, list(request.table1_path),
                                                  list(request.table2_path), list(request.new_table_path))
        return resp
