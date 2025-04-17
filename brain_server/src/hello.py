import grpc
from concurrent import futures
import time

# 导入生成的模块
from proto.brain_server_pb2 import HelloReply
import proto.brain_server_pb2_grpc as brain_server_pb2_grpc

# 修改为正确的服务实现类
class BrainServerServicer(brain_server_pb2_grpc.BrainServerServicer):
    def SayHello(self, request, context):
        print(f"Received request: {request.name}")
        # 构造回应
        return HelloReply(message=f"Hello, {request.name}!")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    brain_server_pb2_grpc.add_BrainServerServicer_to_server(BrainServerServicer(), server)
    server.add_insecure_port('[::]:60061')
    server.start()
    print("gRPC server running on port 60061...")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
