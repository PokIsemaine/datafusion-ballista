import random
import time
import numpy as np
from pymoo.algorithms.moo.nsga3 import NSGA3
from pymoo.optimize import minimize
from pymoo.problems import get_problem
from pymoo.util.ref_dirs import get_reference_directions
from pymoo.core.problem import Problem
from pymoo.operators.sampling.rnd import IntegerRandomSampling

import sys
sys.path.append("/home/zsl/datafusion-ballista/brain_server")
from src.resource.vm_type import get_vm_types

class VMResourceAllocationProblem(Problem):
    def __init__(self, num_stages, vm_types):
        self.num_stages = num_stages
        self.vm_types = vm_types
        n_var = 2 * num_stages
        n_obj = 2  # 现在有两个目标：cost 和 执行时间
        n_constr = 0  # 无约束

        # 上下界 [虚拟机数量，虚拟机配置索引]
        xl = np.array([1, 0] * num_stages)
        xu = np.array([10, len(vm_types)-1] * num_stages)

        super().__init__(n_var=n_var, n_obj=n_obj, n_constr=n_constr, xl=xl, xu=xu)

    # TODO Cpython 加速
    # 多进程并行化
    def _evaluate(self, x, out, *args, **kwargs):
        # print(x.shape) (pop_size, num_stages * 2)
        vm_counts = x[:, ::2].astype(int)  # 将虚拟机数量
        vm_specs = x[:, 1::2].astype(int)  # 将虚拟机配置
        total_cost = np.zeros(x.shape[0])  # 用于计算所有个体的成本
        total_time = np.zeros(x.shape[0])  # 用于计算所有个体的执行时间

        # TODO 并行化
        for i in range(self.num_stages):
            # 计算每个个体的总成本和总执行时间
            total_cost += vm_counts[:, i] * np.array([self.vm_types[spec].price_per_hour for spec in vm_specs[:, i]])
            total_time += vm_counts[:, i] * np.array([20000 for spec in vm_specs[:, i]])
            time.sleep(0.01)  # 模拟计算时间

        out["F"] = np.column_stack([total_cost, total_time])  # 返回两个目标：cost 和 执行时间


vm_types = get_vm_types()

num_stages = 10  # 假设有 3 个阶段
# 创建问题实例
problem = VMResourceAllocationProblem(num_stages=num_stages, vm_types=vm_types)

# 创建参考方向，维度为2
ref_dirs = get_reference_directions("das-dennis", 2, n_partitions=12)

# 创建NSGA-III算法对象
algorithm = NSGA3(pop_size=num_stages,
                  ref_dirs=ref_dirs,
                  sampling=IntegerRandomSampling(),)

# 执行优化
res = minimize(problem,
               algorithm,
               seed=1,
               termination=('n_gen', num_stages * 2),
               verbose=True)

print("Best solution found:")
print(res.X)

for i in range(len(res.F)):
    print(f"Solution {i + 1}: {res.F[i]}")  

vm_counts = res.X[0, ::2].astype(int)
vm_specs = res.X[0, 1::2].astype(int)

print("VM counts:", vm_counts)
print("VM specs:", vm_specs)


print("Optimal VM allocation for each stage:")
for i in range(problem.num_stages):
    print(f"Stage {i + 1}: {vm_counts[i]} VMs of type {vm_types[vm_specs[i]].name}")
