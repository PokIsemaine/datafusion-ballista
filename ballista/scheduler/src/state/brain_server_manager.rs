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

use ballista_core::error::{BallistaError, Result};
use ballista_core::serde::brain_server_pb::{ScheduleJob, ScheduleStage, StageOperator};
use ballista_core::utils::create_grpc_client_connection;
use dashmap::DashMap;
use datafusion::common::not_impl_err;
use datafusion::physical_plan::ExplainCsvRow;
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

    pub async fn recommend_schedule(
        &self,
        job_id: &str,
        job_name: &str,
        graph_stages: &Vec<Vec<ExplainCsvRow>>,
    ) -> Result<()> {
        // Here we would send a scheduling job request to the brain server
        // This is a placeholder for the actual implementation
        info!("Sending scheduling job request to brain server");
        let mut client = self.get_client("default_executor_id").await.map_err(|e| {
            BallistaError::General(format!("Failed to get brain server client: {}", e))
        })?;

        let schedule_stages = graph_stages
            .iter()
            .map(|stage| {
                let stage_operators = stage
                    .iter()
                    .map(|op| StageOperator {
                        job_id: op.job_id.clone(),
                        job_name: op.job_name.clone(),
                        stage_id: op.stage_id.clone() as u64,
                        current_op_id: op.current_op_id.clone() as u64,
                        parent_op_id: op.parent_op_id.clone() as u64,
                        operator_type: op.operator_type.clone(),
                        stat_num_rows: op.stat_num_rows.clone(),
                        stat_total_byte_size: op.stat_total_byte_size.clone(),
                        column_stats: op.column_stats.clone(),
                        topk_fetch: op.topk_fetch.map(|v| v as u64),
                        sort_expr: op.sort_expr.clone(),
                        preserve_partitioning: op.preserve_partitioning,
                        agg_mode: op.agg_mode.clone(),
                        agg_gby: op.agg_gby.clone(),
                        agg_aggr: op.agg_aggr.clone(),
                        agg_limit: op.agg_limit.map(|v| v as u64),
                        agg_input_order_mode: op.agg_input_order_mode.clone(),
                        coalesce_batch_target_batch_size: op
                            .coalesce_batch_target_batch_size
                            as u64,
                        coalesce_batch_fetch: op.coalesce_batch_fetch.map(|v| v as u64),
                        filter_predicate: op.filter_predicate.clone(),
                        filter_projection: op.filter_projection.clone(),
                        hj_mode: op.hj_mode.clone(),
                        hj_join_type: op.hj_join_type.clone(),
                        hj_on: op.hj_on.clone(),
                        hj_filter: op.hj_filter.clone(),
                        hj_projections: op.hj_projections.clone(),
                        nlj_join_type: op.nlj_join_type.clone(),
                        nlj_filter: op.nlj_filter.clone(),
                        nlj_projections: op.nlj_projections.clone(),
                        smj_join_type: op.smj_join_type.clone(),
                        smj_on: op.smj_on.clone(),
                        smj_filter: op.smj_filter.clone(),
                        sort_required_expr: op.sort_required_expr.clone(),
                        shj_mode: op.shj_mode.clone(),
                        shj_join_type: op.shj_join_type.clone(),
                        shj_on: op.shj_on.clone(),
                        shj_filter: op.shj_filter.clone(),
                        global_limit_skip: op.global_limit_skip as u64,
                        global_limit_fetch: op.global_limit_fetch.map(|v| v as u64),
                        local_limit_fetch: op.local_limit_fetch as u64,
                        memory_partitions: op.memory_partitions.clone() as u64,
                        memory_partition_sizes: op.memory_partition_sizes.clone(),
                        memory_output_ordering: op.memory_output_ordering.clone(),
                        memory_constraints: op.memory_constraints.clone(),
                        lazy_memory_partitions: op.lazy_memory_partitions.clone() as u64,
                        lazy_memory_batch_generators: op
                            .lazy_memory_batch_generators
                            .clone(),
                        projection_expr: op.projection_expr.clone(),
                        recursive_query_name: op.recursive_query_name.clone(),
                        recursive_query_is_distinct: op.recursive_query_is_distinct,
                        repartition_name: op.repartition_name.clone(),
                        repartition_partitioning: op.repartition_partitioning.clone(),
                        repartition_input_partitions: op
                            .repartition_input_partitions
                            .clone()
                            as u64,
                        repartition_preserve_order: op.repartition_preserve_order,
                        repartition_sort_exprs: op.repartition_sort_exprs.clone(),
                        partial_sort_tok_fetch: op
                            .partial_sort_tok_fetch
                            .map(|v| v as u64),
                        partial_sort_expr: op.partial_sort_expr.clone(),
                        partial_sort_common_prefix_length: op
                            .partial_sort_common_prefix_length
                            as u64,
                        sort_preserving_merge_fetch: op
                            .sort_preserving_merge_fetch
                            .map(|v| v as u64),
                        work_table_name: op.work_table_name.clone(),
                        streaming_table_partition_sizes: op
                            .streaming_table_partition_sizes
                            .clone()
                            as u64,
                        streaming_table_projection_schema: op
                            .streaming_table_projection_schema
                            .clone(),
                        streaming_table_infinite_source: op
                            .streaming_table_infinite_source,
                        streaming_table_fetch: op.streaming_table_fetch.map(|v| v as u64),
                        streaming_table_ordering: op.streaming_table_ordering.clone(),
                        statistics_col_count: op.statistics_col_count as u64,
                        statistics_row_count: op.statistics_row_count.clone(),
                        bounded_window_wdw: op.bounded_window_wdw.clone(),
                        bounded_window_input_order_mode: op
                            .bounded_window_input_order_mode
                            .clone(),
                        window_agg_wdw: op.window_agg_wdw.clone(),
                        memory_table_partitions: op.memory_table_partitions.clone()
                            as u64,
                        parquet_base_config: op.parquet_base_config.clone(),
                        parquet_predicate: op.parquet_predicate.clone(),
                        parquet_prunning_predicate: op.parquet_prunning_predicate.clone(),
                        csv_base_config: op.csv_base_config.clone(),
                        parquet_sink_file_group: op.parquet_sink_file_group.clone(),
                        shuffle_reader_partitions: op.shuffle_reader_partitions.clone()
                            as u64,
                        shuffle_writer_output_partitioning: op
                            .shuffle_writer_output_partitioning
                            .clone(),
                        unresolved_shuffle_output_partitioning: op
                            .unresolved_shuffle_output_partitioning
                            .clone(),
                    })
                    .collect::<Vec<_>>();
                let stage_id = stage.first().unwrap().stage_id.clone();
                let stage_output_links = stage.first().unwrap().output_links.clone();
                ScheduleStage {
                    stage_id: stage_id as u64,
                    output_links: stage_output_links
                        .into_iter()
                        .map(|x| x as u64)
                        .collect(),
                    operators: stage_operators,
                }
            })
            .collect::<Vec<_>>();

        let response = client
            .recommend_schedule(ScheduleJob {
                job_id: job_id.to_string(),
                job_name: job_name.to_string(),
                stages: schedule_stages,
            })
            .await
            .map_err(|e| {
                BallistaError::General(format!("Failed to send schedule job: {}", e))
            })?;

        info!("Received response from brain server: {:?}", response);

        Ok(())
    }

    pub async fn update_job_stage(
        &self,
        job_id: &str,
        stage_id: u64,
        update_stage: &Vec<ExplainCsvRow>,
    ) -> Result<()> {
        // Here we would send a job stage update request to the brain server
        // This is a placeholder for the actual implementation
        info!("Sending job stage update request to brain server");
        not_impl_err!("BrainServerManager::update_job_stage is not implemented yet")?;
        Ok(())
    }
}