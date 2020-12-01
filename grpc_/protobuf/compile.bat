set SRC_DIR=./grpc_/protobuf
set DST_DIR=./grpc_/messages
python -m grpc_tools.protoc -I%SRC_DIR% --python_out=%DST_DIR% --grpc_python_out=%DST_DIR% %SRC_DIR%/tree.proto
