from predict_model.predict import PredictModel
from proto.data_type import JobInfo
from proto.brain_server_pb2 import ScheduleResult, StageSchedulePolicy

class MOOScheduler:
    def __init__(self, config):
        self.config = config
        self.tasks = []
        self.current_task = None
        self.predict_model = PredictModel()
        self.vm_types = get_vm_types()
        
    def add_task(self, task):
        self.tasks.append(task)

    def schedule(self, job_info: JobInfo) -> ScheduleResult:
        # Mock
        if job_info is not None:
            stage_schedule_policy = StageSchedulePolicy(
                stage_id=1,
                vm_set={
                    "vm_spec_id": "VMTypeA",
                    "vm_count": 2
                }
            )
            return ScheduleResult(status="OK", schedule_policy=[stage_schedule_policy])
        """
        TODO: 
        1. 搜索算法找候选调度策略
        2. 计算每个候选调度策略的理想情况下性能和成本
        3. 选择最优的调度策略
        """    
        best_policy = None
        
        num_stages = 50  # 假设有 3 个阶段
        # 创建问题实例
        problem = VMResourceAllocationProblem(num_stages=num_stages, vm_types=self.vm_types)

        # 创建参考方向，维度为2
        ref_dirs = get_reference_directions("das-dennis", 2, n_partitions=12)

        # 创建NSGA-III算法对象
        algorithm = NSGA3(pop_size=num_stages,
                        ref_dirs=ref_dirs,
                        sampling=IntegerRandomSampling(),)

        # 执行优化
        moo_result = minimize(problem,
                    algorithm,
                    seed=1,
                    termination=('n_gen', num_stages * 2),
                    verbose=True)

        print("Best solution found:")
        print(moo_result.X)

        for i in range(len(res.F)):
            print(f"Solution {i + 1}: {moo_result.F[i]}")  

        vm_counts = res.X[0, ::2].astype(int)
        vm_specs = res.X[0, 1::2].astype(int)

        print("VM counts:", vm_counts)
        print("VM specs:", vm_specs)


        print("Optimal VM allocation for each stage:")
        for i in range(problem.num_stages):
            print(f"Stage {i + 1}: {vm_counts[i]} VMs of type {self.vm_types[vm_specs[i]].name}")
                
        # 4. 动态优化资源池使其满足理论状态
        self.request_resources()

    
    def cost_estimate(self, schedule_policy: ScheduleResult) -> float:
        """
        计算调度策略的成本估计
        :param schedule_policy: 调度策略
        :return: 成本估计值
        """
    
    def request_resources(self):
        """
        请求资源
        :param schedule_policy: 调度策略
        """