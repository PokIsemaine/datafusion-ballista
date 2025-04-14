import duckdb
import os
import multiprocessing
import pyarrow.parquet as pq
import pyarrow as pa
import psutil


# 写入分片的函数：将一定数量的行写入一个新的 Parquet 文件
def write_partition_with_limit(input_file, output_file, rows_limit):
    """
    将输入的 Parquet 文件切分为小块，每块包含 rows_limit 行数据，写入到 output_file 中。
    """
    # 创建 DuckDB 连接
    con = duckdb.connect()

    # 使用 DuckDB 读取原始 Parquet 文件，并创建视图 v_hit
    con.execute(f"CREATE VIEW v_hit AS SELECT * FROM read_parquet('{input_file}')")

    # 将前 rows_limit 行数据写入新的 Parquet 文件
    con.execute(f"""
        COPY (
            SELECT * FROM v_hit
            LIMIT {rows_limit}  -- 限制每次写入的行数
        ) TO '{output_file}' (FORMAT PARQUET)  -- 写入到输出文件
    """)

    # 关闭连接
    con.close()
    print(f"[+] 写入完成: {output_file}")


# 估算每行的大小，并计算文件的总行数
def estimate_avg_row_size(input_file):
    """
    估算每行的大小（字节），并计算 Parquet 文件的总行数。
    """
    con = duckdb.connect()

    # 创建一个 DuckDB 视图，直接读取输入的 Parquet 文件
    con.execute(f"CREATE VIEW v_hit AS SELECT * FROM read_parquet('{input_file}')")

    # 使用临时文件来保存数据样本，用于计算平均行大小
    tmp_file = "tmp_sample.parquet"
    if os.path.exists(tmp_file):
        os.remove(tmp_file)

    # 选择前 10,000 行作为样本并将它们写入临时文件
    con.execute("COPY (SELECT * FROM v_hit LIMIT 10000) TO 'tmp_sample.parquet' (FORMAT PARQUET)")

    # 获取临时文件的大小，并计算每行的平均大小
    file_size = os.path.getsize(tmp_file)
    avg_row_size = file_size / 10000

    # 删除临时文件
    os.remove(tmp_file)

    # 获取文件中的总行数
    num_rows = con.execute("SELECT COUNT(*) FROM v_hit").fetchone()[0]

    con.close()
    return avg_row_size, num_rows


# 合并分片并删除已处理的文件
def merge_parquet_files_streaming(input_dir, output_file):
    """
    将多个分片文件合并为一个大的 Parquet 文件，合并过程中实时删除已经处理的文件。
    """
    print(f"🔁 边合并边删除，目标输出文件：{output_file}")

    # 获取所有分片文件，并按名称排序
    files = sorted([
        os.path.join(input_dir, f)
        for f in os.listdir(input_dir)
        if f.startswith("hit_part_") and f.endswith(".parquet")
    ])

    # 定义一个 writer 变量，用于在写入过程中保持文件句柄
    writer = None

    # 处理每一个分片文件
    for f in files:
        print(f"📥 读取并写入: {f}")
        pf = pq.ParquetFile(f)
        # 逐批读取数据并写入目标文件
        for batch in pf.iter_batches(batch_size=1_000_000):  # 使用更小的批次
            table = pa.Table.from_batches([batch])
            # 如果 writer 为 None，说明是第一次写入，初始化 writer
            if writer is None:
                writer = pq.ParquetWriter(output_file, table.schema)
            writer.write_table(table)

        # 删除已处理的分片文件，释放空间
        os.remove(f)
        print(f"🗑️ 已删除: {f}")

    # 完成合并后关闭 writer
    if writer:
        writer.close()
        print(f"✅ 合并完成: {output_file}")
    else:
        print("⚠️ 未发现任何 parquet 分片文件可合并")


# 扩展 Parquet 文件并将数据写入多个分片
def extend_parquet_parallel(input_file, output_dir, target_size_gb, num_workers=4):
    """
    扩展原始 Parquet 文件，生成多个分片并合并成目标大小。
    使用并发处理来加速过程。
    """
    # 估算每行大小并获取文件总行数
    avg_row_size, num_rows = estimate_avg_row_size(input_file)

    # 计算目标文件的字节大小和每个分片的大小
    total_bytes = target_size_gb * 1024 ** 3
    per_file_bytes = 256 * 1024 ** 2  # 每个分片大小为 256MB（可以根据需要调整）
    rows_per_part = int(per_file_bytes / avg_row_size)  # 每个分片应包含的行数
    total_parts = int(total_bytes / per_file_bytes) + 1  # 总共需要多少个分片

    # 输出估算信息
    print(f"估算平均行大小: {avg_row_size:.2f} 字节")
    print(f"每分片约 {per_file_bytes / 1024**2:.2f} MB，约 {rows_per_part:,} 行")
    print(f"目标大小: {target_size_gb}GB，需生成 {total_parts} 分片")

    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)

    # 准备多个任务，每个任务写入一个分片
    file_args = []
    for i in range(total_parts):
        output_file = os.path.join(output_dir, f"hit_part_{i:03d}.parquet")
        file_args.append((input_file, output_file, rows_per_part))

    # 使用并发池来加速分片写入过程
    print(f"🚀 并发写入中（{num_workers} 并发）...")
    with multiprocessing.Pool(processes=num_workers) as pool:
        pool.starmap(write_partition_with_limit, file_args)

    print("✅ 所有分文件写入完成")

    # 合并所有分片并输出最终文件
    final_file = os.path.join(output_dir, f"hits_{target_size_gb}gb.parquet")
    merge_parquet_files_streaming(output_dir, final_file)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="扩展 ClickBench hit.parquet 数据集")
    parser.add_argument("--input", help="输入原始 hit.parquet 文件路径", default="/home/zsl/datafusion-ballista/benchmarks/clickbench_data/hits.parquet")
    parser.add_argument("--output_dir", help="输出目录", default="/home/zsl/datafusion-ballista/benchmarks/clickbench_data")
    parser.add_argument("--size_gb", type=int, help="目标大小（单位 GB）", default=50)
    parser.add_argument("--workers", type=int, default=6, help="并发写入数量")

    args = parser.parse_args()

    extend_parquet_parallel(
        input_file=args.input,
        output_dir=args.output_dir,
        target_size_gb=args.size_gb,
        num_workers=args.workers
    )
