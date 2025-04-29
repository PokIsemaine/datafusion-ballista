from brain_server.src.resource.vm_type import get_vm_types
from brain_server.src.scheduler.moo_scheduler.moo_problem import VMResourceAllocationProblem
from predict_model.predict import PredictModel
from proto.data_type import JobInfo
from proto.brain_server_pb2 import ScheduleResult, StageSchedulePolicy
from pymoo.algorithms.moo.nsga3 import NSGA3
from pymoo.optimize import minimize
from pymoo.util.ref_dirs import get_reference_directions
from pymoo.operators.sampling.rnd import IntegerRandomSampling


class MOOScheduler:
    def __init__(self, config):
        self.config = config
        self.tasks = []
        self.current_task = None
        self.predict_model = PredictModel()
        self.vm_types = get_vm_types()
        self.user_preferences = 0.5
        
    def add_task(self, task):
        self.tasks.append(task)

    def schedule(self, job_info: JobInfo) -> ScheduleResult:
        # if job_info is not None:
        #     stage_schedule_policy = StageSchedulePolicy(
        #         stage_id=1,
        #         vm_set={
        #             "vm_spec_id": "VMTypeA",
        #             "vm_count": 2
        #         }
        #     )
        #     return ScheduleResult(status="OK", schedule_policy=[stage_schedule_policy])
        
        # NSGA3 多目标优化
        problem = VMResourceAllocationProblem(job_info=job_info, vm_types=self.vm_types)
        ref_dirs = get_reference_directions("das-dennis", 2, n_partitions=12)
        num_stages = job_info.get_num_stages()
        algorithm = NSGA3(pop_size = num_stages * len(self.vm_types) // 2,
                        ref_dirs=ref_dirs,
                        sampling=IntegerRandomSampling(),)
        moo_result = minimize(problem,
                    algorithm,
                    seed=1,
                    termination=('n_gen', num_stages * 2),
                    verbose=True)

        # 根据用户偏好从多目标优化结果中选择最优策略
        best_policy = None
        best_score = float('inf')
        for solution in moo_result.X:
            vm_counts = solution[::2].astype(int)
            vm_specs = solution[1::2].astype(int)
            cost, execution_time = moo_result.F[moo_result.X.tolist().index(solution)]
            score = compute_score(
                time=execution_time,
                cost=cost,
                time_min=0,
                time_max=1e6,
                cost_min=0,
                cost_max=1e6,
                alpha=self.user_preferences
            )
            if score < best_score:
                best_score = score
                best_policy = (vm_counts, vm_specs, cost, execution_time)
            
        stage_schedule_policies = [
            StageSchedulePolicy(
                stage_id=i + 1,
                vm_set={
                    "vm_spec_id": self.vm_types[best_policy[1][i]],
                    "vm_count": best_policy[0][i]
                }
            )
            for i in range(num_stages)
        ]
        schedule_result = ScheduleResult(
            status="OK",
            schedule_policy=stage_schedule_policies
        )
                
        # 4. 动态优化资源池使其满足理论状态
        self.request_resources()
        
        return schedule_result

    def compute_score(time, cost, time_min, time_max, cost_min, cost_max, alpha):
        if time > time_max or time < time_min:
            raise ValueError("Execution time is out of bounds.")
        if cost > cost_max or cost < cost_min:
            raise ValueError("Cost is out of bounds.")
        epsilon = 1e-8
        time_score = 1 - (time - time_min) / (time_max - time_min + epsilon)
        cost_score = 1 - (cost - cost_min) / (cost_max - cost_min + epsilon)
        return alpha * time_score + (1 - alpha) * cost_score
    
    def request_resources(self):
        """
        请求资源
        :param schedule_policy: 调度策略
        """
        pass