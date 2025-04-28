from pprint import pprint
import grpc
from concurrent import futures
import time

# 导入生成的模块
from scheduler.moo_scheduler.moo_scheduler import MOOScheduler
from proto.data_type import JobInfo, parse_schedule_job
from proto.brain_server_pb2 import HelloReply, ScheduleResult, ScheduleJob
import proto.brain_server_pb2_grpc as brain_server_pb2_grpc

# 修改为正确的服务实现类
class BrainServerServicer(brain_server_pb2_grpc.BrainServerServicer):
    def __init__(self):
        self.moo_scheduler = MOOScheduler(config={})
    def SayHello(self, request, context):
        print(f"Received request: {request.name}")
        # 构造回应
        return HelloReply(message=f"Hello, {request.name}!")
    
    def RecommendSchedule(self, request : ScheduleJob, context):
        print(f"Received job_id: {request.job_id}")

        job_info : JobInfo = parse_schedule_job(request)
        pprint(f"Parsed schedule job: {job_info}")

        schedule_result : ScheduleResult = self.moo_scheduler.schedule(job_info)
        
        return schedule_result

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
