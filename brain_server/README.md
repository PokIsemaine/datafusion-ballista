# Brain Server

## grpc
python -m grpc_tools.protoc   --proto_path=/home/zsl/datafusion-ballista/ballista/core/   --python_out=/home/zsl/datafusion-ballista/brain_server/src   --grpc_python_out=/home/zsl/datafusion-ballista/brain_server/src   proto/brain_server.proto