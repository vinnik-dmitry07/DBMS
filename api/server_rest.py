from flask import Flask, jsonify, request
# from pymongo import MongoClient

import api

# api.mongo_client = MongoClient()
tree = api.create_tree()


def create(**kwargs):
    return jsonify(success=tree.create(path=list(kwargs.values())))


def create_rows(**kwargs):
    return jsonify(success=tree.create_rows(table_path=list(kwargs.values()), rows=request.get_json()))


def create_columns(**kwargs):
    return jsonify(success=tree.create_columns(table_path=list(kwargs.values()), columns=request.get_json()))


def read(**kwargs):
    return jsonify(tree.read(list(kwargs.values())))


def read_columns(**kwargs):
    return jsonify(tree.read_columns(table_path=list(kwargs.values())))


def read_column(**kwargs):
    column_id = kwargs.pop('column_id')
    return jsonify(tree.read_column(table_path=list(kwargs.values()), column_id=column_id))


def read_value(**kwargs):
    row_id = kwargs.pop('row_id')
    column_id = kwargs.pop('column_id')
    path = list(kwargs.values()) + [row_id, column_id]
    return jsonify(tree.read(path))


def update_row(**kwargs):
    row_id = kwargs.pop('row_id')
    return jsonify(success=tree.update_row(table_path=list(kwargs.values()), row_id=row_id, sub_row=request.get_json()))


def update_column(**kwargs):
    column_id = kwargs.pop('column_id')
    return jsonify(success=tree.update_column(table_path=list(kwargs.values()), column_id=column_id,
                                              column=request.get_json()))


def delete(**kwargs):
    return jsonify(success=tree.delete(path=list(kwargs.values())))


def intersect_tables():
    return jsonify(success=tree.intersect_tables(**request.get_json()))


def test():
    return 'Hello'


app = Flask(__name__)

app.add_url_rule(rule='/tree/', view_func=read, methods=['GET'])
app.add_url_rule(rule='/tree/<base_id>/', view_func=create, methods=['POST'])
app.add_url_rule(rule='/tree/<base_id>/', view_func=read, methods=['GET'])
app.add_url_rule(rule='/tree/<base_id>/', view_func=delete, methods=['DELETE'])
app.add_url_rule(rule='/tree/<base_id>/<table_id>/', view_func=create, methods=['POST'])
app.add_url_rule(rule='/tree/<base_id>/<table_id>/', view_func=delete, methods=['DELETE'])
app.add_url_rule(rule='/tree/<base_id>/<table_id>/rows/', view_func=create_rows, methods=['POST'])
app.add_url_rule(rule='/tree/<base_id>/<table_id>/rows/', view_func=read, methods=['GET'])
app.add_url_rule(rule='/tree/<base_id>/<table_id>/rows/<int:row_id>/', view_func=read, methods=['GET'])
app.add_url_rule(rule='/tree/<base_id>/<table_id>/rows/<int:row_id>/', view_func=update_row, methods=['PUT'])
app.add_url_rule(rule='/tree/<base_id>/<table_id>/rows/<int:row_id>/', view_func=delete, methods=['DELETE'])
app.add_url_rule(rule='/tree/<base_id>/<table_id>/rows/<int:row_id>/<column_id>/', view_func=read_value, methods=['GET'])
app.add_url_rule(rule='/tree/<base_id>/<table_id>/columns/', view_func=create_columns, methods=['POST'])
app.add_url_rule(rule='/tree/<base_id>/<table_id>/columns/', view_func=read_columns, methods=['GET'])
app.add_url_rule(rule='/tree/<base_id>/<table_id>/columns/<column_id>/', view_func=read_column, methods=['GET'])
app.add_url_rule(rule='/tree/<base_id>/<table_id>/columns/<column_id>/', view_func=update_column, methods=['PUT'])
app.add_url_rule(rule='/tree/<base_id>/<table_id>/columns/<column_id>/', view_func=delete, methods=['DELETE'])
app.add_url_rule(rule='/tree/<base_id>/<table_id>/columns/<column_id>/<int:row_id>/', view_func=read_value, methods=['GET'])
app.add_url_rule(rule='/tree/intersect_tables/', view_func=intersect_tables, methods=['POST'])

# await (await fetch('/tree/db1/tb1/', {mode: 'no-cors', method: 'GET'})).json()
# await (await fetch('/tree/db1/tb1/rows/', {mode: 'no-cors', method: 'POST',
# body: JSON.stringify([{'co1': 123, 'co2': 456}]), headers: {'Content-Type': 'application/json'}})).json()


@app.route('/')
def hello_world():
    return 'Hello World!'


if __name__ == '__main__':
    app.run()
