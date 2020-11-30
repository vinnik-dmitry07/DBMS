from concurrent import futures

import grpc

import messages.tree_pb2_grpc as tree_service
from services.tree import TreeServicer


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    tree_service.add_TreeServicer_to_server(TreeServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
