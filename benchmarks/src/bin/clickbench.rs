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

use std::path::Path;
use std::path::PathBuf;
use std::time::Instant;
use std::time::SystemTime;

use ballista::prelude::SessionContextExt;
use ballista_benchmarks::util::CommonOpt;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::exec_datafusion_err;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::ParquetReadOptions;
use datafusion::DATAFUSION_VERSION;
use datafusion::{
    error::{DataFusionError, Result},
    prelude::SessionContext,
};
use serde::Serialize;
use structopt::StructOpt;

#[derive(Debug, Serialize)]
struct QueryResult {
    elapsed: f64,
    row_count: usize,
}

#[derive(Debug, Serialize)]
struct BenchmarkRun {
    /// Benchmark crate version
    benchmark_version: String,
    /// DataFusion crate version
    datafusion_version: String,
    /// Number of CPU cores
    num_cpus: usize,
    /// Start time
    start_time: u64,
    /// CLI arguments
    arguments: Vec<String>,
    /// query number
    query: usize,
    /// list of individual run times and row counts
    iterations: Vec<QueryResult>,
    benchmark_name: String,
}

impl BenchmarkRun {
    fn new(query: usize, benchmark_name: String) -> Self {
        Self {
            benchmark_version: env!("CARGO_PKG_VERSION").to_owned(),
            datafusion_version: DATAFUSION_VERSION.to_owned(),
            num_cpus: std::thread::available_parallelism().unwrap().get(),
            start_time: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("current time is later than UNIX_EPOCH")
                .as_secs(),
            arguments: std::env::args().skip(1).collect::<Vec<String>>(),
            query,
            iterations: vec![],
            benchmark_name: benchmark_name,
        }
    }

    fn add_result(&mut self, elapsed: f64, row_count: usize) {
        self.iterations.push(QueryResult { elapsed, row_count })
    }
}

/// Run the clickbench benchmark
///
/// The ClickBench[1] benchmarks are widely cited in the industry and
/// focus on grouping / aggregation / filtering. This runner uses the
/// scripts and queries from [2].
///
/// [1]: https://github.com/ClickHouse/ClickBench
/// [2]: https://github.com/ClickHouse/ClickBench/tree/main/datafusion
#[derive(Debug, StructOpt, Clone)]
#[structopt(verbatim_doc_comment)]
pub struct RunOpt {
    /// Query number (between 0 and 42). If not specified, runs all queries
    #[structopt(short, long)]
    query: Option<usize>,

    /// Common options
    #[structopt(flatten)]
    common: CommonOpt,

    /// Path to hits.parquet (single file) or `hits_partitioned`
    /// (partitioned, 100 files)
    #[structopt(
        parse(from_os_str),
        short = "p",
        long = "path",
        default_value = "benchmarks/clickbench_data/hits.parquet"
    )]
    path: PathBuf,

    /// Path to queries.sql (single file)
    #[structopt(
        parse(from_os_str),
        short = "r",
        long = "queries-path",
        default_value = "benchmarks/queries/clickbench/queries.sql"
    )]
    queries_path: PathBuf,

    /// If present, write results json here
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: Option<PathBuf>,

    /// Ballista executor host
    #[structopt(long = "host")]
    host: Option<String>,

    /// Ballista executor port
    #[structopt(long = "port")]
    port: Option<u16>,
}

struct AllQueries {
    queries: Vec<String>,
}

impl AllQueries {
    fn try_new(path: &Path) -> Result<Self> {
        // ClickBench has all queries in a single file identified by line number
        let all_queries = std::fs::read_to_string(path)
            .map_err(|e| exec_datafusion_err!("Could not open {path:?}: {e}"))?;
        Ok(Self {
            queries: all_queries.lines().map(|s| s.to_string()).collect(),
        })
    }

    /// Returns the text of query `query_id`
    fn get_query(&self, query_id: usize) -> Result<&str> {
        self.queries
            .get(query_id)
            .ok_or_else(|| {
                let min_id = self.min_query_id();
                let max_id = self.max_query_id();
                exec_datafusion_err!(
                    "Invalid query id {query_id}. Must be between {min_id} and {max_id}"
                )
            })
            .map(|s| s.as_str())
    }

    fn min_query_id(&self) -> usize {
        0
    }

    fn max_query_id(&self) -> usize {
        self.queries.len() - 1
    }
}
impl RunOpt {
    pub async fn run(self) -> Result<()> {
        println!("Running benchmarks with the following options: {self:?}");
        let queries = AllQueries::try_new(self.queries_path.as_path())?;
        let query_range = match self.query {
            Some(query_id) => query_id..=query_id,
            None => queries.min_query_id()..=queries.max_query_id(),
        };

        // configure parquet options
        let mut config = self.common.config();
        {
            let parquet_options = &mut config.options_mut().execution.parquet;
            // The hits_partitioned dataset specifies string columns
            // as binary due to how it was written. Force it to strings
            parquet_options.binary_as_string = true;
        }

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .build();

        let scheduler_address = format!(
            "df://{}:{}",
            self.host.clone().unwrap().as_str(),
            self.port.unwrap()
        );
        let mut ctx =
            SessionContext::remote_with_state(&scheduler_address, state).await?;

        ctx.register_parquet(
            "hits",
            self.path
                .to_str()
                .ok_or_else(|| DataFusionError::Execution("Invalid path".to_string()))?,
            ParquetReadOptions::new(),
        )
        .await?;

        let iterations = self.common.iterations;
        for query_id in query_range {
            let mut millis = Vec::with_capacity(iterations);
            let mut benchmark_run = BenchmarkRun::new(query_id, "clickbench".to_string());
            let sql = queries.get_query(query_id)?;
            println!("Q{query_id}: {sql}");

            let mut result: Vec<RecordBatch> = Vec::with_capacity(1);
            for i in 0..iterations {
                let start = Instant::now();
                let result = ctx.sql(sql).await?.collect().await?;
                let elapsed = start.elapsed();
                let ms = elapsed.as_secs_f64() * 1000.0;
                millis.push(ms);
                let row_count: usize = result.len();
                println!(
                    "Query {query_id} iteration {i} took {ms:.1} ms and returned {row_count} rows"
                );
            }
            if self.common.debug {
                ctx.sql(sql).await?.explain(false, false)?.show().await?;
            }
            let avg = millis.iter().sum::<f64>() / millis.len() as f64;
            println!("Query {query_id} avg time: {avg:.2} ms");
        }
        Ok(())
    }

    /// Registers the `hits.parquet` as a table named `hits`
    async fn register_hits(&self, ctx: &SessionContext) -> Result<()> {
        let options = Default::default();
        let path = self.path.as_os_str().to_str().unwrap();
        ctx.register_parquet("hits", path, options)
            .await
            .map_err(|e| {
                DataFusionError::Context(
                    format!("Registering 'hits' as {path}"),
                    Box::new(e),
                )
            })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let opt = RunOpt::from_args();
    opt.run().await
}
