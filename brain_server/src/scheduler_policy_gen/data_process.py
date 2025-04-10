import os
import re
import pandas as pd

# 获取指定目录下所有 benchmark_tpchq19_{n}.csv 的文件
# 每个 csv 文件有以下列
# job_id,job_name,job_status,job_queued_at,job_start_time,job_end_time,
# stage_id,stage_attempt_num,stage_partitions,plan,
# task_id,task_scheduled_time,task_launch_time,task_start_exec_time,task_end_exec_time,task_finish_time,task_status
# 提取出 job_end_time 和 job_start_time 列
# 将这些 csv 文件合并成一个 csv 文件 benchmark_tpchq19.csv
# 新文件包括 source_file_name 列, 表示原文件名; job_time 列表示 job 的运行时间, 取值为第一行（非标题行）数据的 job_end_time列 - job_start_time列

def merge_csv_files(input_dir, output_file):
    # 获取所有 benchmark_tpchq19_{n}.csv 文件
    csv_files = [f for f in os.listdir(input_dir) if re.match(r'benchmark_tpchq19_\d+\.csv', f)]

    # 初始化一个空的 DataFrame
    merged_df = pd.DataFrame()

    # 遍历每个 CSV 文件
    for csv_file in csv_files:
        file_path = os.path.join(input_dir, csv_file)
        # 读取 CSV 文件
        df = pd.read_csv(file_path)

        # 提取 job_end_time 和 job_start_time 列
        df['job_time'] = df['job_end_time'] - df['job_start_time']
        df['source_file_name'] = csv_file

        # 合并到总的 DataFrame 中
        merged_df = pd.concat([merged_df, df[['source_file_name', 'job_time']]], ignore_index=True)

    # 保存合并后的数据到输出文件
    merged_df.to_csv(output_file, index=False)
    
# 对 benchmark_tpchq19.csv 文件进行处理
# 按照 job_time 列进行升序后输出到 benchmark_tpchq19_sorted.csv
def sort_csv_file(input_file, output_file):
    # 读取 CSV 文件
    df = pd.read_csv(input_file)

    # 按照 job_time 列进行升序排序
    df['job_time'] = pd.to_timedelta(df['job_time'])
    sorted_df = df.sort_values(by='job_time')

    # 保存排序后的数据到输出文件
    sorted_df.to_csv(output_file, index=False)    
    
# seaborn 生成 job_time 的分布图, 保存为 benchmark_tpchq19_job_time_distribution.png
def plot_job_time_distribution(input_file, output_image):
    import seaborn as sns
    import matplotlib.pyplot as plt

    # 读取 CSV 文件
    df = pd.read_csv(input_file)

    # 将 job_time 列转换为 timedelta 类型
    df['job_time'] = pd.to_timedelta(df['job_time'])

    # 绘制分布图
    sns.histplot(df['job_time'].dt.total_seconds() , bins=50, kde=True)
    plt.xlabel('Job Time (s)')
    plt.ylabel('Frequency')
    plt.title('Job Time Distribution')
    plt.savefig(output_image)
    plt.close()
    
if __name__ == "__main__":
    # 输入目录和输出文件名
    input_dir = "/home/zsl/datafusion-ballista/benchmarks/data/csv_data"
    output_file = "benchmark_tpchq19.csv"

    # 调用函数进行合并
    merge_csv_files(input_dir, output_file)
    
    sort_csv_file("benchmark_tpchq19.csv", "benchmark_tpchq19_sorted.csv")
    
    plot_job_time_distribution("benchmark_tpchq19_sorted.csv", "benchmark_tpchq19_job_time_distribution.png")