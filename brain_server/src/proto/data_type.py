from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class HelloRequest:
    name: str = ""

@dataclass
class HelloReply:
    message: str = ""

@dataclass
class StageOperator:
    job_id: str = ""
    job_name: str = ""
    stage_id: int = 0
    current_op_id: int = 0
    parent_op_id: int = 0

    operator_type: str = ""

    stat_num_rows: str = ""
    stat_total_byte_size: str = ""
    column_stats: str = ""

    topk_fetch: Optional[int] = None
    sort_expr: str = ""
    preserve_partitioning: bool = False

    agg_mode: str = ""
    agg_gby: str = ""
    agg_aggr: str = ""
    agg_limit: Optional[int] = None
    agg_input_order_mode: str = ""

    coalesce_batch_target_batch_size: int = 0
    coalesce_batch_fetch: Optional[int] = None

    filter_predicate: str = ""
    filter_projection: str = ""

    hj_mode: str = ""
    hj_join_type: str = ""
    hj_on: str = ""
    hj_filter: str = ""
    hj_projections: str = ""

    nlj_join_type: str = ""
    nlj_filter: str = ""
    nlj_projections: str = ""

    smj_join_type: str = ""
    smj_on: str = ""
    smj_filter: str = ""

    sort_required_expr: str = ""

    shj_mode: str = ""
    shj_join_type: str = ""
    shj_on: str = ""
    shj_filter: str = ""

    global_limit_skip: int = 0
    global_limit_fetch: Optional[int] = None

    local_limit_fetch: int = 0

    memory_partitions: int = 0
    memory_partition_sizes: str = ""
    memory_output_ordering: str = ""
    memory_constraints: str = ""

    lazy_memory_partitions: int = 0
    lazy_memory_batch_generators: str = ""

    projection_expr: str = ""

    recursive_query_name: str = ""
    recursive_query_is_distinct: bool = False

    repartition_name: str = ""
    repartition_partitioning: str = ""
    repartition_input_partitions: int = 0
    repartition_preserve_order: bool = False
    repartition_sort_exprs: Optional[str] = None

    partial_sort_tok_fetch: Optional[int] = None
    partial_sort_expr: str = ""
    partial_sort_common_prefix_length: int = 0

    sort_preserving_merge_fetch: Optional[int] = None

    work_table_name: str = ""

    streaming_table_partition_sizes: int = 0
    streaming_table_projection_schema: str = ""
    streaming_table_infinite_source: bool = False
    streaming_table_fetch: Optional[int] = None
    streaming_table_ordering: str = ""

    statistics_col_count: int = 0
    statistics_row_count: str = ""

    bounded_window_wdw: str = ""
    bounded_window_input_order_mode: str = ""

    window_agg_wdw: str = ""

    memory_table_partitions: int = 0

    parquet_base_config: str = ""
    parquet_predicate: str = ""
    parquet_prunning_predicate: str = ""

    csv_base_config: str = ""

    parquet_sink_file_group: str = ""

    shuffle_reader_partitions: int = 0

    shuffle_writer_output_partitioning: str = ""

    unresolved_shuffle_output_partitioning: str = ""

@dataclass
class StageInfo:
    stage_id: int = 0
    output_links: List[int] = field(default_factory=list)
    operators: List[StageOperator] = field(default_factory=list)

@dataclass
class JobInfo:
    job_id: str = ""
    stages: List[StageInfo] = field(default_factory=list)
    
    def get_num_stages(self) -> int:
        return len(self.stages)
    def get_stage(self, stage_id: int) -> Optional[StageInfo]:
        for stage in self.stages:
            if stage.stage_id == stage_id:
                return stage
        return None

def parse_schedule_job(request) -> JobInfo:
    stages = []
    for stage in request.stages:
        operators = []
        for operator in stage.operators:
            op = StageOperator(
                job_id=operator.job_id,
                job_name=operator.job_name,
                stage_id=operator.stage_id,
                current_op_id=operator.current_op_id,
                parent_op_id=operator.parent_op_id,
                operator_type=operator.operator_type,
                stat_num_rows=operator.stat_num_rows,
                stat_total_byte_size=operator.stat_total_byte_size,
                column_stats=operator.column_stats,
                topk_fetch=operator.topk_fetch if operator.HasField('topk_fetch') else None,
                sort_expr=operator.sort_expr,
                preserve_partitioning=operator.preserve_partitioning,
                agg_mode=operator.agg_mode,
                agg_gby=operator.agg_gby,
                agg_aggr=operator.agg_aggr,
                agg_limit=operator.agg_limit if operator.HasField('agg_limit') else None,
                agg_input_order_mode=operator.agg_input_order_mode,
                coalesce_batch_target_batch_size=operator.coalesce_batch_target_batch_size,
                coalesce_batch_fetch=operator.coalesce_batch_fetch if operator.HasField('coalesce_batch_fetch') else None,
                filter_predicate=operator.filter_predicate,
                filter_projection=operator.filter_projection,
                hj_mode=operator.hj_mode,
                hj_join_type=operator.hj_join_type,
                hj_on=operator.hj_on,
                hj_filter=operator.hj_filter,
                hj_projections=operator.hj_projections,
                nlj_join_type=operator.nlj_join_type,
                nlj_filter=operator.nlj_filter,
                nlj_projections=operator.nlj_projections,
                smj_join_type=operator.smj_join_type,
                smj_on=operator.smj_on,
                smj_filter=operator.smj_filter,
                sort_required_expr=operator.sort_required_expr,
                shj_mode=operator.shj_mode,
                shj_join_type=operator.shj_join_type,
                shj_on=operator.shj_on,
                shj_filter=operator.shj_filter,
                global_limit_skip=operator.global_limit_skip,
                global_limit_fetch=operator.global_limit_fetch if operator.HasField('global_limit_fetch') else None,
                local_limit_fetch=operator.local_limit_fetch,
                memory_partitions=operator.memory_partitions,
                memory_partition_sizes=operator.memory_partition_sizes,
                memory_output_ordering=operator.memory_output_ordering,
                memory_constraints=operator.memory_constraints,
                lazy_memory_partitions=operator.lazy_memory_partitions,
                lazy_memory_batch_generators=operator.lazy_memory_batch_generators,
                projection_expr=operator.projection_expr,
                recursive_query_name=operator.recursive_query_name,
                recursive_query_is_distinct=operator.recursive_query_is_distinct,
                repartition_name=operator.repartition_name,
                repartition_partitioning=operator.repartition_partitioning,
                repartition_input_partitions=operator.repartition_input_partitions,
                repartition_preserve_order=operator.repartition_preserve_order,
                repartition_sort_exprs=operator.repartition_sort_exprs if operator.HasField('repartition_sort_exprs') else None,
                partial_sort_tok_fetch=operator.partial_sort_tok_fetch if operator.HasField('partial_sort_tok_fetch') else None,
                partial_sort_expr=operator.partial_sort_expr,
                partial_sort_common_prefix_length=operator.partial_sort_common_prefix_length,
                sort_preserving_merge_fetch=operator.sort_preserving_merge_fetch if operator.HasField('sort_preserving_merge_fetch') else None,
                work_table_name=operator.work_table_name,
                streaming_table_partition_sizes=operator.streaming_table_partition_sizes,
                streaming_table_projection_schema=operator.streaming_table_projection_schema,
                streaming_table_infinite_source=operator.streaming_table_infinite_source,
                streaming_table_fetch=operator.streaming_table_fetch if operator.HasField('streaming_table_fetch') else None,
                streaming_table_ordering=operator.streaming_table_ordering,
                statistics_col_count=operator.statistics_col_count,
                statistics_row_count=operator.statistics_row_count,
                bounded_window_wdw=operator.bounded_window_wdw,
                bounded_window_input_order_mode=operator.bounded_window_input_order_mode,
                window_agg_wdw=operator.window_agg_wdw,
                memory_table_partitions=operator.memory_table_partitions,
                parquet_base_config=operator.parquet_base_config,
                parquet_predicate=operator.parquet_predicate,
                parquet_prunning_predicate=operator.parquet_prunning_predicate,
                csv_base_config=operator.csv_base_config,
                parquet_sink_file_group=operator.parquet_sink_file_group,
                shuffle_reader_partitions=operator.shuffle_reader_partitions,
                shuffle_writer_output_partitioning=operator.shuffle_writer_output_partitioning,
                unresolved_shuffle_output_partitioning=operator.unresolved_shuffle_output_partitioning,
            )
            operators.append(op)

        stage_obj = StageInfo(
            stage_id=stage.stage_id,
            output_links=stage.output_links,
            operators=operators
        )
        stages.append(stage_obj)

    return JobInfo(
        job_id=request.job_id,
        stages=stages
    )
