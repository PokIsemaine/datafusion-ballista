from dataclasses import dataclass, field
from typing import Dict

@dataclass
class VMSet:
    vm_type: str
    vm_count: int
    

@dataclass
class SchedulePolicy:
    policy: Dict[int, VMSet] = field(default_factory=dict)
    stage_graph: Dict[int, list] = field(default_factory=dict)

    def update_policy(self, stage_id: int, vm_type: str, vm_count: int):
        """
        更新调度策略
        :param stage_id: 阶段 ID
        :param vm_type: VM 类型
        :param vm_count: VM 数量
        """
        self.policy[stage_id] = VMSet(vm_type, vm_count)
    
    def get_policy(self) -> Dict[int, VMSet]:
        """
        获取当前调度策略
        :return: 调度策略字典
        """
        return self.policy
    
    def get_stage_vmset(self, stage_id: int) -> VMSet:
        """
        获取指定阶段的 VM 配置
        :param stage_id: 阶段 ID
        :return: VMSet 对象
        """
        return self.policy.get(stage_id, None)