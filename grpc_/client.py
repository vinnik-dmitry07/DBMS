from typing import Dict, List

import grpc
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct

import grpc_.messages.tree_pb2 as tree_messages
import grpc_.messages.tree_pb2_grpc as tree_service
from grpc_.services.tree import to_type_pairs, from_type_pair, from_type_pairs, from_jsonable_rows, \
    from_jsonable_base, from_jsonable_tree, to_jsonable_row, to_jsonable_column, from_jsonable_columns

channel = grpc.insecure_channel('127.0.0.1:50051')
client = tree_service.TreeStub(channel)


def _create(f, *args):
    request = tree_messages.PathRequest()
    request.path.extend(args)
    response = f(request)
    return response


def create_base(base_id: str):
    return _create(client.CreateBase, base_id)


def create_table(base_id: str, table_id: str):
    return _create(client.CreateTable, base_id, table_id)


def create_rows(base_id: str, table_id: str, rows: List[Dict]):
    request = tree_messages.CreateRowsRequest()
    request.table_path.extend([base_id, table_id])
    for row in rows:
        struct = Struct()
        struct.update(to_jsonable_row(row))
        request.rows.append(struct)
    response = client.CreateRows(request)
    return response


def create_columns(base_id: str, table_id: str, columns: Dict[str, Dict]):
    request = tree_messages.CreateColumnsRequest()
    request.table_path.extend([base_id, table_id])
    for column_id, column in columns.items():
        if 'values' in column:
            column['values'] = to_type_pairs(column['values'])
        request.columns[column_id].update(column)
    response = client.CreateColumns(request)
    return response


def _read(f, *args) -> Dict:
    request = tree_messages.PathRequest()
    request.path.extend(args)
    response = f(request)
    return MessageToDict(response)


def read_tree():
    return from_jsonable_tree(_read(client.ReadTree))


def read_base(base_id: str):
    return from_jsonable_base(_read(client.ReadBase, base_id))


def read_rows(base_id: str, table_id: str):
    return from_jsonable_rows(_read(client.ReadRows, base_id, table_id))


def read_row(base_id: str, table_id: str, row_id: int):
    return from_type_pairs(_read(client.ReadRow, base_id, table_id, row_id))


def read_columns(base_id: str, table_id: str):
    return from_jsonable_columns(_read(client.ReadColumns, base_id, table_id))


def read_column(base_id: str, table_id: str, column_id: str):
    return from_type_pairs(_read(client.ReadColumn, base_id, table_id, column_id))


def read_value(base_id: str, table_id: str, row_id: int, column_id: str):
    return from_type_pair(_read(client.ReadValue, base_id, table_id, row_id, column_id))


def read_schema(base_id: str, table_id):
    return _read(client.ReadSchema, base_id, table_id)


def update_row(base_id: str, table_id: str, row_id: int, sub_row: Dict):
    request = tree_messages.UpdateRowRequest()
    request.table_path.extend([base_id, table_id])
    request.row_id = row_id
    request.sub_row.update(to_jsonable_row(sub_row))
    response = client.UpdateRow(request)
    return response


def update_column(base_id: str, table_id: str, column_id: str, sub_column: Dict):
    request = tree_messages.UpdateColumnRequest()
    request.table_path.extend([base_id, table_id])
    request.column_id = column_id
    request.sub_column.update(to_jsonable_column(sub_column))
    response = client.UpdateColumn(request)
    return response


def _delete(f, *args):
    request = tree_messages.PathRequest()
    request.path.extend(args)
    response = f(request)
    return response


def delete_base(base_id: str):
    return _delete(client.DeleteBase, base_id)


def delete_table(base_id: str, table_id: str):
    return _delete(client.DeleteTable, base_id, table_id)


def delete_row(base_id: str, table_id: str, row_id: int):
    return _delete(client.DeleteRow, base_id, table_id, str(row_id))


def delete_column(base_id: str, table_id: str, column_id: str):
    return _delete(client.DeleteColumn, base_id, table_id, column_id)


def intersect_tables(by_column_id: str, table1_path: List[str], table2_path: List[str], new_table_path: List[str]):
    request = tree_messages.IntersectTablesRequest()
    request.by_column_id = by_column_id
    request.table1_path.extend(table1_path)
    request.table2_path.extend(table2_path)
    request.new_table_path.extend(new_table_path)
    response = client.IntersectTables(request)
    return response
