import numpy as np
from pymoo.core.problem import Problem
from predict_model.predict import JobPredictResult, PredictModel
from proto.data_type import JobInfo
from resource.vm_type import VMType, get_vm_types

class VMResourceAllocationProblem(Problem):
    def __init__(self, job_info: JobInfo, vm_types):
        self.job_info = job_info
        self.num_stages = job_info.get_num_stages()  # 获取阶段数
        self.vm_types : list[VMType] = vm_types
        self.predict_model : PredictModel = None  # 这里可以放入预测模型的实例
        n_var = 2 * self.num_stages
        n_obj = 2  # 现在有两个目标：cost 和 执行时间
        n_constr = 0  # 无约束

        # 上下界 [虚拟机数量，虚拟机配置索引]
        xl = np.array([1, 0] * self.num_stages)
        xu = np.array([128, len(vm_types)-1] * self.num_stages)

        super().__init__(n_var=n_var, n_obj=n_obj, n_constr=n_constr, xl=xl, xu=xu)

    # TODO Cpython 加速
    # 多进程并行化
    def _evaluate(self, x, out, *args, **kwargs):
        # print(x.shape) (pop_size, num_stages * 2)
        vm_counts, vm_specs = x[:, ::2].astype(int), x[:, 1::2].astype(int)
        estimate_costs, estimate_times = np.zeros(x.shape[0]), np.zeros(x.shape[0])

        # TODO: 并行优化
        for i in range(x.shape[0]):
            predict_result : JobPredictResult = self.predict_model.predict(job_info=self.job_info, vm_counts=vm_counts[i], vm_specs=vm_specs[i])
            estimate_costs[i] = predict_result.job_predict_time_ms
            estimate_times[i] = self.cost_estimate(x[i], predict_result)

        out["F"] = np.column_stack([estimate_costs, estimate_times])  # 返回两个目标：cost 和 执行时间

    def cost_estimate(self, schedule_policy, predict_result: JobPredictResult) -> float:
        vm_counts = schedule_policy[::2].astype(int)
        vm_specs = schedule_policy[1::2].astype(int)
        return sum(
            vm_counts[i] * self.vm_types[vm_specs[i]].get_price_per_second() *
            predict_result.get_stage_predict_time(i + 1) / 1000
            for i in range(self.num_stages)
        )