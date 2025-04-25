// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;
use std::vec;

use ballista_core::error::{BallistaError, Result};
use ballista_core::serde::brain_server_pb::{ScheduleJob, ScheduleStage, ScheduleTask};
use ballista_core::utils::create_grpc_client_connection;
use dashmap::DashMap;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::bytes::physical_plan_to_json;
use log::{debug, info};
use tonic::transport::Channel;

use ballista_core::serde::brain_server_pb::brain_server_client::BrainServerClient;

/// Client manager for brain server
type BrainServerClients = Arc<DashMap<String, BrainServerClient<Channel>>>;

/// BrainServer manager handles the communication with Python brain server
/// which provides advanced scheduling policies and resource management capabilities
#[derive(Clone)]
pub struct BrainServerManager {
    /// Client connection to the brain server
    clients: BrainServerClients,
    /// Brain server address
    brain_server_addr: String,
}

impl BrainServerManager {
    /// Create a new BrainServerManager instance
    pub fn new(brain_server_addr: String) -> Self {
        Self {
            clients: Default::default(),
            brain_server_addr,
        }
    }

    async fn get_client(&self, executor_id: &str) -> Result<BrainServerClient<Channel>> {
        let client = self.clients.get(executor_id).map(|value| value.clone());

        if let Some(client) = client {
            Ok(client)
        } else {
            let brain_server_url = format!("http://{}", self.brain_server_addr);
            debug!("Connecting to brain server at {}", brain_server_url);
            let connection = create_grpc_client_connection(brain_server_url).await?;
            let client = BrainServerClient::new(connection);

            {
                self.clients.insert(executor_id.to_owned(), client.clone());
            }
            Ok(client)
        }
    }

    pub async fn say_hello(&self) -> Result<()> {
        // Here we would send a hello message to the brain server
        // This is a placeholder for the actual implementation
        info!("Sending hello message to brain server");

        let mut client = self.get_client("default_executor_id").await.map_err(|e| {
            BallistaError::General(format!("Failed to get brain server client: {}", e))
        })?;
        let hello_request = ballista_core::serde::brain_server_pb::HelloRequest {
            name: "Hello from Scheduler".to_string(),
        };
        let response = client.say_hello(hello_request).await.map_err(|e| {
            BallistaError::General(format!("Failed to send hello: {}", e))
        })?;
        info!("Received response from brain server: {:?}", response);

        Ok(())
    }

    pub async fn recommend_schedule(&self, plan: Arc<dyn ExecutionPlan>) -> Result<()> {
        // Here we would send a scheduling job request to the brain server
        // This is a placeholder for the actual implementation
        info!("Sending scheduling job request to brain server");
        let mut client = self.get_client("default_executor_id").await.map_err(|e| {
            BallistaError::General(format!("Failed to get brain server client: {}", e))
        })?;

        let plan_json = physical_plan_to_json(plan).map_err(|e| {
            BallistaError::General(format!("Failed to convert plan to JSON: {}", e))
        })?;

        let tasks = ScheduleTask {};
        let stage = ScheduleStage {
            stage_id: "123".to_string(),
            physical_plan: plan_json,
            tasks: vec![tasks],
        };

        let schedule_job = ScheduleJob {
            job_id: "123".to_string(),
            job_name: "job_name".to_string(),
            stages: vec![stage],
        };
        let response = client.recommend_schedule(schedule_job).await.map_err(|e| {
            BallistaError::General(format!("Failed to send schedule job: {}", e))
        })?;
        info!("Received response from brain server: {:?}", response);

        Ok(())
    }
}

/// Placeholder for task execution metrics
#[derive(Debug, Clone)]
pub struct TaskMetrics {
    pub task_id: String,
    pub executor_id: String,
    pub start_time: u64,
    pub end_time: u64,
    pub cpu_usage: f64,
    pub memory_usage: f64,
    pub input_rows: u64,
    pub input_bytes: u64,
    pub output_rows: u64,
    pub output_bytes: u64,
}

/// Placeholder for executor information
#[derive(Debug, Clone)]
pub struct ExecutorInfo {
    pub executor_id: String,
    pub host: String,
    pub port: u16,
    pub task_slots: u32,
    pub cpu_limit: f64,
    pub memory_limit: f64,
    pub available_task_slots: u32,
}

/// Placeholder for resource allocation plan
#[derive(Debug, Clone)]
pub struct ResourceAllocationPlan {
    pub scale_up: Vec<ExecutorSpec>,
    pub scale_down: Vec<String>, // executor_ids to scale down
}

impl Default for ResourceAllocationPlan {
    fn default() -> Self {
        Self {
            scale_up: vec![],
            scale_down: vec![],
        }
    }
}

/// Placeholder for executor specification used in scale-up requests
#[derive(Debug, Clone)]
pub struct ExecutorSpec {
    pub cpu: f64,
    pub memory: f64,
    pub task_slots: u32,
}
