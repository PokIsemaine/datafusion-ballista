import pandas as pd
import os

def process_csv_by_family(input_file: str, output_dir: str) -> None:
    """
    读取 CSV 文件，按规格族分组，将每个规格族的数据写入不同的文件
    """
    # 读取 CSV 文件
    df = pd.read_csv(input_file)
    
    # 去除重复的 InstanceType
    df = df.drop_duplicates(subset=["InstanceType"])

    # 提取规格族和规格系列
    def extract_family_and_series(instance_type):
        if "." in instance_type:
            family = instance_type.split(".")[1]
            # 系列为数字之前的部分
            series = ''.join(filter(str.isalpha, family))
        else:
            family = instance_type
            series = ''.join(filter(str.isalpha, family))
        return family, series

    df[["Family", "Series"]] = df["InstanceType"].apply(
        lambda x: pd.Series(extract_family_and_series(x))
    )

    # 提取规格大小
    def extract_size(instance_type):
        return instance_type.split(".")[-1]

    df["Size"] = df["InstanceType"].apply(extract_size)

    # 动态计算规格大小的排序权重
    def calculate_size_weight(size):
        if size == "small":
            return 1  # small 表示 1 vCPU
        elif size == "large":
            return 2  # large 表示 2 vCPU
        elif "xlarge" in size:
            try:
                multiplier = int(size.replace("xlarge", "")) if size.replace("xlarge", "").isdigit() else 1
                return multiplier * 4  # n xlarge 表示 n * 4 vCPU
            except ValueError:
                return -1
        return -1  # 未知规格大小

    df["SizeWeight"] = df["Size"].apply(calculate_size_weight)

    # 按规格族分组
    grouped = df.groupby("Family")

    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)

    # 将每个规格族的数据写入单独的文件
    for family, group in grouped:
        # 按规格大小权重排序
        group = group.sort_values(by=["SizeWeight"])
        # 重新排列列顺序
        group = group[["Series", "Family", "InstanceType", "CPU", "Memory", "Price"]]
        output_file = os.path.join(output_dir, f"{family}_family.csv")
        group.to_csv(output_file, index=False)
        print(f"规格族 {family} 的数据已写入到 {output_file}")

# 调用函数
process_csv_by_family("instance_data.csv", "output_families")