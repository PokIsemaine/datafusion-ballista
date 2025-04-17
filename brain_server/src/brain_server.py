#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import asyncio
import logging
import os
import sys
from concurrent import futures
from typing import Dict, List, Optional

import grpc
from grpc.aio import server as aio_server

# 导入生成的protobuf文件
# 注意：你可能需要根据实际情况调整导入路径
sys.path.append(os.path.join(os.path.dirname(__file__), "../../ballista/core/proto"))
try:
    import ballista_pb2
    import ballista_pb2_grpc
except ImportError:
    print("无法导入protobuf生成的文件，请确保已经运行protoc生成相应文件")
    sys.exit(1)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("brain_server")

class BrainServicer(ballista_pb2_grpc.BrainServerServicer):
    """
    实现Brain服务，处理来自scheduler的请求
    """
    
    def __init__(self):
        self.executors_info = {}  # 存储executor信息
        self.tasks_info = {}      # 存储任务信息
        logger.info("Brain服务初始化完成")
    
    async def ReportExecutorHeartbeat(self, request, context):
        """
        处理executor心跳消息，由scheduler转发
        """
        executor_id = request.executor_id
        logger.info(f"收到executor {executor_id} 的心跳信息")
        
        # 存储或更新executor信息
        self.executors_info[executor_id] = {
            "timestamp": request.timestamp,
            "metrics": request.metrics,
            "status": request.status.status,
        }
        
        # 返回响应
        return ballista_pb2.BrainReportExecutorHeartbeatResult(success=True)
    
    async def ReportTaskStatus(self, request, context):
        """
        处理任务状态更新，由scheduler转发
        """
        executor_id = request.executor_id
        task_statuses = request.task_status
        
        logger.info(f"收到executor {executor_id} 的任务状态更新，共 {len(task_statuses)} 个任务")
        
        # 更新任务状态
        for task in task_statuses:
            task_id = task.task_id
            self.tasks_info[task_id] = {
                "executor_id": executor_id,
                "job_id": task.job_id,
                "stage_id": task.stage_id,
                "partition_id": task.partition_id,
                "status": task.status,
                "timestamp": task.timestamp,
            }
            logger.info(f"任务 {task_id} 状态: {task.status}")
        
        return ballista_pb2.BrainReportTaskStatusResult(success=True)
    
    async def GetExecutorResources(self, request, context):
        """
        返回当前所有executor的资源信息
        """
        executor_id = request.executor_id
        logger.info(f"收到获取executor {executor_id} 资源请求")
        
        # 检查是否有此executor的信息
        if executor_id in self.executors_info:
            return ballista_pb2.BrainGetExecutorResourcesResult(
                success=True,
                executor_info=self.executors_info[executor_id]
            )
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"未找到executor {executor_id} 的信息")
            return ballista_pb2.BrainGetExecutorResourcesResult(success=False)
    
    async def GetTaskSchedulingHint(self, request, context):
        """
        为scheduler提供任务调度建议
        """
        job_id = request.job_id
        stage_id = request.stage_id
        logger.info(f"收到获取作业 {job_id}，阶段 {stage_id} 的任务调度建议请求")
        
        # 这里只是一个简单的实现，返回空的调度建议
        # 实际应用中可以根据收集的信息提供更智能的调度策略
        return ballista_pb2.BrainGetTaskSchedulingHintResult(
            success=True,
            scheduling_hint="ROUND_ROBIN"  # 可以是其他调度策略，如RESOURCE_AWARE等
        )

async def serve(port: int = 50051):
    """启动gRPC服务器"""
    server = aio_server()
    ballista_pb2_grpc.add_BrainServerServicer_to_server(BrainServicer(), server)
    server_address = f"[::]:{port}"
    server.add_insecure_port(server_address)
    
    logger.info(f"Brain服务器启动，监听端口: {port}")
    await server.start()
    
    try:
        await server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("收到终止信号，正在关闭服务器...")
        await server.stop(grace=5)

def main():
    """主函数"""
    port = int(os.environ.get("BRAIN_SERVER_PORT", "50051"))
    
    logger.info(f"启动Brain服务器，端口: {port}")
    asyncio.run(serve(port))

if __name__ == "__main__":
    main()