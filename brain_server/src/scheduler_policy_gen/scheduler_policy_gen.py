import itertools

# query_id : [tasks]
tpch_tasks = {
    6: [1000, 1001, 2000],
    19: [1000, 1001, 2000, 2001, 3000, 3001, 4000],
}

click_bench_2p_tasks = {
    20: [1000],
    21: [1000, 1001, 2000, 2001, 3000],
    22: [1000, 1001, 2000, 2001, 3000],
    30: [1000, 1001, 2000, 2001, 3000],
    31: [1000, 1001, 2000, 2001, 3000],
    32: [1000, 1001, 2000, 2001, 3000],
    33: [1000, 1001, 2000, 2001, 3000],
    40: [1000, 1001, 2000, 2001, 3000],
}
def generate_task_assignments(tasks, executors, output_file):
    """
    枚举所有任务分配到 executors 的可能性，并存储到指定的 shell 脚本文件中。

    :param tasks: 任务列表
    :param executors: 执行器列表
    :param output_file: 输出的 shell 文件路径
    """
    # 获取任务数量
    n = len(tasks)

    # 生成所有任务分配的可能性
    assignments = list(itertools.product(executors, repeat=n))

    # 将结果写入 shell 文件
    with open(output_file, mode='w') as file:
        file.write("#!/bin/bash\n\n")  # 添加 shell 脚本头部
        file.write("task_assignments=(\n")  # 定义数组
        for i, assignment in enumerate(assignments):
            # 构造策略字符串
            strategy = ",".join([f"{task}:{executor}" for task, executor in zip(tasks, assignment)])
            file.write(f"    \"{strategy}\"\n")
        file.write(")\n")  # 结束数组定义

    print(f"生成了 {len(assignments)} 种任务分配可能，并保存到 {output_file}")

if __name__ == "__main__":
    # 定义两个 executor
    executors = ['0', '1']

    # 输出文件名
    output_file = "./q6_task_assignments.sh"

    # 生成任务分配可能性
    generate_task_assignments(tpch_tasks[6], executors, output_file)