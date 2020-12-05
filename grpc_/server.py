from concurrent import futures

import grpc

import api
import grpc_.messages.tree_pb2_grpc as tree_service


def serve():
    from pymongo import MongoClient
    api.mongo_client = MongoClient()
    from grpc_.services.tree import TreeServicer

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # noinspection PyTypeChecker
    tree_service.add_TreeServicer_to_server(TreeServicer(api), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
