from pydoc import locate

import api
import messages.tree_pb2 as tree_messages
import messages.tree_pb2_grpc as tree_service
from google.protobuf.json_format import MessageToDict


def type_value_pairs(lst):
    return [[type(v).__name__, v] for v in lst]


def build_tree(id_, sub_node):
    response = tree_messages.NodeResponse()
    response.id_ = str(id_)
    if isinstance(sub_node, api.Row):
        response.list.extend(type_value_pairs(sub_node))
    elif isinstance(sub_node, api.Branch):
        response.children.nodes.extend([build_tree(k, v) for k, v in sub_node.items()])
    return response

# v.WhichOneof('kind')
# {f: v.ListFields()[0][1] for f, v in row_struct.fields.items()}


class TreeServicer(tree_service.TreeServicer):
    tree = api.create_tree()

    def create(self, path):
        resp = tree_messages.SuccessResponse()
        resp.success = self.tree.create(path)
        return resp

    def CreateBase(self, request, context):
        return self.create(request.path)

    def CreateTable(self, request, context):
        return self.create(request.path)

    def CreateRows(self, request, context):
        resp = tree_messages.SuccessResponse()
        rows_typeless = [{f: v.ListFields()[0][1] for f, v in row_struct.fields.items()} for row_struct in request.rows]
        rows = [{k: locate(v[0])(v[1]) for k, v in row.items()} for row in rows_typeless]
        resp.success = self.tree.create_rows(list(request.table_path), rows)
        return resp

    def CreateColumns(self, request, context):
        resp = tree_messages.SuccessResponse()
        columns = MessageToDict(request)['columns']
        for column in columns.values():
            if 'values' in column:
                column['values'] = [locate(v[0])(v[1]) for v in column['values']]
        resp.success = self.tree.create_columns(list(request.table_path), columns)
        return resp

    def read(self, path):
        path = list(path)
        # with server_rest.app.app_context():
        return build_tree(path[-1] if path else 'root', self.tree.read(path))
        # todo remove 'root'

    def ReadTree(self, request, context):
        return self.read(request.path)

    def ReadBase(self, request, context):
        return self.read(request.path)

    def ReadRows(self, request, context):
        return self.read(request.path)

    def ReadRow(self, request, context):
        path = list(request.path)
        path[-1] = int(path[-1])
        return self.read(path)

    def ReadColumns(self, request, context):
        table_path = list(request.path)
        resp = tree_messages.ColumnsResponse()
        resp.value = self.tree.read(request.path)
        return build_tree(table_path[-1], self.tree.read_columns(table_path))

    def ReadColumn(self, request, context):
        path = list(request.path)
        table_path, column_id = path[:-1], path[-1]
        resp = tree_messages.ColumnResponse()
        resp.columns.update(self.tree.read_column(table_path, column_id))
        return resp

    def ReadValue(self, request, context):
        resp = tree_messages.ValueResponse()
        resp.value = self.tree.read(request.path)
        return resp

    def UpdateRow(self, request, context):
        return self.tree.update_row(list(request.table_path), request.row_id, request.sub_row)

    def UpdateColumn(self, request, context):
        return self.tree.update_column(list(request.table_path), request.column_id, request.column)

    def delete(self, path):
        resp = tree_messages.SuccessResponse()
        resp.success = self.tree.delete(path)
        return resp

    def DeleteBase(self, request, context):
        return self.delete(request.path)

    def DeleteTable(self, request, context):
        return self.delete(request.path)

    def DeleteRow(self, request, context):
        path = list(request.path)
        path[-1] = int(path[-1])
        return self.delete(path)

    def DeleteColumn(self, request, context):
        return self.delete(request.path)

    def IntersectTables(self, request, context):
        resp = tree_messages.SuccessResponse()
        # with server_rest.app.app_context():
        resp.success = self.tree.intersect_tables(request.base_name, request.table1_name,
                                                  request.table2_name, request.new_name)
        return resp
