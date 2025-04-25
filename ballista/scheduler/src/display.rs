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

//! Implementation of ballista physical plan display with metrics. See
//! [`crate::physical_plan::displayable`] for examples of how to
//! format

use ballista_core::utils::collect_plan_metrics;
use datafusion::common::stats::Precision;
use datafusion::logical_expr::{StringifiedPlan, ToStringifiedPlan};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    accept, csv_accept, CsvExecutionPlanVisitor, CsvMetricsRow, CsvVisitorResult, DisplayFormatType, ExecutionPlan, ExecutionPlanVisitor, ExplainCsvRow
};
use log::{error, info};
use std::fmt;
use std::fs::File;

pub fn print_stage_metrics(
    job_id: &str,
    stage_id: usize,
    plan: &dyn ExecutionPlan,
    stage_metrics: &[MetricsSet],
) {
    // The plan_metrics collected here is a snapshot clone from the plan metrics.
    // They are all empty now and need to combine with the stage metrics in the ExecutionStages
    //  每一个 MetricsSet 是一个 plan 节点对应的 metrics
    let mut plan_metrics = collect_plan_metrics(plan);
    // 阶段执行指标(stage_metrics)合并到计划指标(plan_metrics)中
    if plan_metrics.len() == stage_metrics.len() {
        plan_metrics.iter_mut().zip(stage_metrics).for_each(
            |(plan_metric, stage_metric)| {
                stage_metric
                    .iter()
                    .for_each(|s| plan_metric.push(s.clone()));
            },
        );

        info!(
            "=== [{}/{}] Stage finished, physical plan with metrics ===\n{}\n",
            job_id,
            stage_id,
            DisplayableBallistaExecutionPlan::new(plan, &plan_metrics).indent()
        );
    } else {
        error!("Fail to combine stage metrics to plan for stage [{}/{}],  plan metrics array size {} does not equal
                to the stage metrics array size {}", job_id, stage_id, plan_metrics.len(), stage_metrics.len());
    }
}

pub fn print_stage_metrics_csv(
    job_id: &str,
    stage_id: usize,
    plan: &dyn ExecutionPlan,
    stage_metrics: &[MetricsSet],
) {
    // The plan_metrics collected here is a snapshot clone from the plan metrics.
    // They are all empty now and need to combine with the stage metrics in the ExecutionStages
    let mut plan_metrics = collect_plan_metrics(plan);

    if plan_metrics.len() == stage_metrics.len() {
        plan_metrics.iter_mut().zip(stage_metrics).for_each(
            |(plan_metric, stage_metric)| {
                stage_metric
                    .iter()
                    .for_each(|s| plan_metric.push(s.clone()));
            },
        );
        let mut result = Vec::new();
        DisplayableBallistaExecutionPlan::new(plan, &plan_metrics)
            .csv(job_id.to_string(), "default_jobname".to_string(), stage_id, &mut result);
    } else {
        error!("Fail to combine stage metrics to plan for stage [{}/{}],  plan metrics array size {} does not equal
                to the stage metrics array size {}", job_id, stage_id, plan_metrics.len(), stage_metrics.len());
    }
}

/// Wraps an `ExecutionPlan` to display this plan with metrics collected/aggregated.
/// The metrics must be collected in the same order as how we visit and display the plan.
pub struct DisplayableBallistaExecutionPlan<'a> {
    inner: &'a dyn ExecutionPlan,
    metrics: &'a Vec<MetricsSet>,
}

impl<'a> DisplayableBallistaExecutionPlan<'a> {
    /// Create a wrapper around an [`'ExecutionPlan'] which can be
    /// pretty printed with aggregated metrics.
    pub fn new(inner: &'a dyn ExecutionPlan, metrics: &'a Vec<MetricsSet>) -> Self {
        Self { inner, metrics }
    }

    /// Return a `format`able structure that produces a single line
    /// per node.
    ///
    /// ```text
    /// ProjectionExec: expr=[a]
    ///   CoalesceBatchesExec: target_batch_size=4096
    ///     FilterExec: a < 5
    ///       RepartitionExec: partitioning=RoundRobinBatch(16)
    ///         CsvExec: source=...",
    /// ```
    pub fn indent(&self) -> impl fmt::Display + 'a {
        struct Wrapper<'a> {
            plan: &'a dyn ExecutionPlan,
            metrics: &'a Vec<MetricsSet>,
        }
        impl fmt::Display for Wrapper<'_> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let t = DisplayFormatType::Default;
                let mut visitor = IndentVisitor {
                    t,
                    f,
                    indent: 0,
                    metrics: self.metrics,
                    metric_index: 0,
                };
                accept(self.plan, &mut visitor)
            }
        }
        Wrapper {
            plan: self.inner,
            metrics: self.metrics,
        }
    }

    pub fn csv(
        &self,
        job_id: String,
        job_name: String,
        stage_id: usize,
        result: &mut Vec<ExplainCsvRow>,
    ) -> CsvVisitorResult {
        let plan_csv_file_path =
            format!("/home/zsl/datafusion-ballista/stage{}.csv", stage_id);
        let metrics_csv_file_path = format!(
            "/home/zsl/datafusion-ballista/stage{}_metrics.csv",
            stage_id
        );
        let plan_csv_writer = csv::WriterBuilder::new()
            .has_headers(true)
            .from_writer(File::create(plan_csv_file_path).unwrap());
        let metrics_csv_writer = csv::WriterBuilder::new()
            .has_headers(true)
            .from_writer(File::create(metrics_csv_file_path).unwrap());
        let mut visitor = CsvVisitor {
            job_id,
            job_name,
            stage_id,
            level: 0,
            plan_csv_writer,
            metrics_csv_writer,
            metrics: self.metrics,
            metric_index: 0,
        };
        csv_accept(self.inner, &mut visitor, result)
    }
}

/// Formats plans with a single line per node.
struct IndentVisitor<'a, 'b> {
    /// How to format each node
    t: DisplayFormatType,
    /// Write to this formatter
    f: &'a mut fmt::Formatter<'b>,
    /// Indent size
    indent: usize,
    /// The metrics along with the plan
    metrics: &'a Vec<MetricsSet>,
    /// The metric index
    metric_index: usize,
}

impl ExecutionPlanVisitor for IndentVisitor<'_, '_> {
    type Error = fmt::Error;
    fn pre_visit(
        &mut self,
        plan: &dyn ExecutionPlan,
    ) -> std::result::Result<bool, Self::Error> {
        write!(self.f, "{:indent$}", "", indent = self.indent * 2)?;
        plan.fmt_as(self.t, self.f)?;
        if let Some(metrics) = self.metrics.get(self.metric_index) {
            let metrics = metrics
                .aggregate_by_name()
                .sorted_for_display()
                .timestamps_removed();
            write!(self.f, ", metrics=[{metrics}]")?;
        } else {
            write!(self.f, ", metrics=[]")?;
        }
        writeln!(self.f)?;
        self.indent += 1;
        self.metric_index += 1;
        Ok(true)
    }

    fn post_visit(&mut self, _plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        self.indent -= 1;
        Ok(true)
    }
}

impl ToStringifiedPlan for DisplayableBallistaExecutionPlan<'_> {
    fn to_stringified(
        &self,
        plan_type: datafusion::logical_expr::PlanType,
    ) -> StringifiedPlan {
        StringifiedPlan::new(plan_type, self.indent().to_string())
    }
}

struct CsvVisitor<'a> {
    job_id: String,
    job_name: String,
    stage_id: usize,
    level: usize, // TODO: remove this
    plan_csv_writer: csv::Writer<File>,
    metrics_csv_writer: csv::Writer<File>,
    metrics: &'a Vec<MetricsSet>,
    metric_index: usize,
}

impl CsvExecutionPlanVisitor for CsvVisitor<'_> {
    type Error = String;
    fn pre_visit(
        &mut self,
        plan: &dyn ExecutionPlan,
        curr_id: usize,
        parrent_id: usize,
        _result: &mut Vec<ExplainCsvRow>,
    ) -> Result<bool, Self::Error> {
        let mut explain_csv_row = ExplainCsvRow::new()
            .with_job_id(self.job_id.clone())
            .with_job_name(self.job_name.clone())
            .with_stage_id(self.stage_id)
            .with_current_op_id(curr_id)
            .with_parent_op_id(parrent_id);
        plan.csv_as(&mut explain_csv_row)?;

        let stats = plan.statistics().map_err(|e| e.to_string())?;
        explain_csv_row.stat_num_rows = stats.num_rows.to_string();

        explain_csv_row.stat_total_byte_size = stats.total_byte_size.to_string();
        explain_csv_row.column_stats = stats
            .column_statistics
            .iter()
            .enumerate()
            .map(|(i, cs)| {
                let s = format!("(Col[{}]:", i);
                let s = if cs.min_value != Precision::Absent {
                    format!("{} Min={}", s, cs.min_value)
                } else {
                    s
                };
                let s = if cs.max_value != Precision::Absent {
                    format!("{} Max={}", s, cs.max_value)
                } else {
                    s
                };
                let s = if cs.sum_value != Precision::Absent {
                    format!("{} Sum={}", s, cs.sum_value)
                } else {
                    s
                };
                let s = if cs.null_count != Precision::Absent {
                    format!("{} Null={}", s, cs.null_count)
                } else {
                    s
                };
                let s = if cs.distinct_count != Precision::Absent {
                    format!("{} Distinct={}", s, cs.distinct_count)
                } else {
                    s
                };
                s + ")"
            })
            .collect::<Vec<_>>()
            .join(",");

        let mut metrics_csv_row: CsvMetricsRow =
            if let Some(metrics) = self.metrics.get(self.metric_index) {
                metrics
                    .aggregate_by_name()
                    .sorted_for_display()
                    .timestamps_removed()
                    .csv(&explain_csv_row.operator_type)
            } else {
                CsvMetricsRow::default() // Provide a default value for CsvMetrics
            };
        metrics_csv_row.operator_type = explain_csv_row.operator_type.clone();

        self.metric_index += 1;

        self.plan_csv_writer.serialize(explain_csv_row).unwrap();
        self.metrics_csv_writer.serialize(metrics_csv_row).unwrap();

        self.level += 1;
        Ok(true)
    }

    fn post_visit(
        &mut self,
        _plan: &dyn ExecutionPlan,
        _curr_id: usize,
        _parrent_id: usize,
    ) -> Result<bool, Self::Error> {
        self.level -= 1;
        if self.level == 0 {
            self.plan_csv_writer.flush().unwrap();
            self.metrics_csv_writer.flush().unwrap();
        }
        Ok(true)
    }
}
