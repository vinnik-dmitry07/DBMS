import graphene
from flask import Flask
# noinspection PyPep8Naming
from flask_graphql import GraphQLView

import api


class Children(graphene.ObjectType):
    nodes = graphene.List(lambda: NodeResponse)


class IntValue(graphene.ObjectType):
    item = graphene.Int()


class FloatValue(graphene.ObjectType):
    item = graphene.Float()


class StrValue(graphene.ObjectType):
    item = graphene.String()


class SubNode(graphene.Union):
    class Meta:
        types = (Children, IntValue, FloatValue, StrValue)


class NodeResponse(graphene.ObjectType):
    id_ = graphene.String()
    sub_node = SubNode()


class SuccessResponse(graphene.ObjectType):
    success = graphene.Boolean()


def build_tree(id_, sub_node):
    response = NodeResponse()
    response.id_ = id_
    if isinstance(sub_node, api.Value):
        if isinstance(sub_node, int):
            response.sub_node = IntValue(sub_node)
        elif isinstance(sub_node, float):
            response.sub_node = FloatValue(sub_node)
        elif isinstance(sub_node, str):
            response.sub_node = StrValue(sub_node)
        else:
            raise
    elif isinstance(sub_node, api.Branch):
        response.sub_node = Children([build_tree(k, v) for k, v in sub_node.items()])
    return response


class Query(graphene.ObjectType):
    get_node = graphene.Field(NodeResponse, path=graphene.List(graphene.String, required=True))
    post_node = graphene.Field(SuccessResponse, path=graphene.List(graphene.String, required=True))
    delete_node = graphene.Field(SuccessResponse, path=graphene.List(graphene.String, required=True))
    intersect_tables = graphene.Field(
        SuccessResponse,
        base_name=graphene.String(required=True),
        table1_name=graphene.String(required=True),
        table2_name=graphene.String(required=True),
        new_name=graphene.String(required=True),
    )

    @staticmethod
    def resolve_get_node(root, info, path):
        return build_tree(path[-1] if path else 'root', api.tree.get(path))

    @staticmethod
    def resolve_post_node(root, info, path):
        return SuccessResponse(api.tree.post(path))

    @staticmethod
    def resolve_delete_node(root, info, path):
        return SuccessResponse(api.tree.delete(path))

    @staticmethod
    def resolve_intersect_tables(root, info, base_name, table1_name, table2_name, new_name):
        return SuccessResponse(api.tree.intersect(base_name, table1_name, table2_name, new_name))


# noinspection PyTypeChecker
schema = graphene.Schema(query=Query)
app = Flask(__name__)
app.add_url_rule(
    '/graphql',
    view_func=GraphQLView.as_view(
        'graphql',
        schema=schema,
        graphiql=True
    )
)

# fetch('/graphql', {
#     method: 'POST',
#     headers: {
#         'Content-Type': 'application/json',
#         'Accept': 'application/json',
#     },
#     body: JSON.stringify({query: `
#         {
#           getNode(path: ["db1", "tb1"]) {
#             id_
#             subNode {
#               ... on Children {
#                 nodes {
#                   id_
#                   subNode {
#                     ... on Children {
#                       nodes {
#                         id_
#                         subNode {
#                           ... on IntValue {
#                             intItem: item
#                           }
#                         }
#                       }
#                     }
#                     ... on IntValue {
#                       intItem: item
#                     }
#                     ... on FloatValue {
#                       floatItem: item
#                     }
#                     ... on StrValue {
#                       strItem: item
#                     }
#                   }
#                 }
#               }
#               ... on IntValue {
#                 intItem: item
#               }
#               ... on FloatValue {
#                 floatItem: item
#               }
#               ... on StrValue {
#                 strItem: item
#               }
#             }
#           }
#         }
#   `})
# })
#   .then(r => r.json())
#   .then(data => console.log('data returned:', data));
