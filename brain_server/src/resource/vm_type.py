from enum import Enum

class CloudStorage:
    def __init__(self, iops_base: float,  cloud_storage_bandwidth: int, iops_peak : float = None , cloud_storage_bandwidth_peak = None):
        self.iops_base = iops_base  # IOPS 基础单位万
        self.iops_peak = iops_peak if iops_peak else iops_base # 突发 IOPS
        self.cloud_storage_bandwidth = cloud_storage_bandwidth  # 云存储带宽基础，单位 Gbit/s
        self.cloud_storage_bandwidth_peak = cloud_storage_bandwidth_peak if cloud_storage_bandwidth_peak else cloud_storage_bandwidth # 突发云存储带宽

class LocalStorage:
    class LocalStorageType(Enum):
        SSD = "SSD"
        HDD = "HDD"
        
    def __init__(self, local_storage_type: LocalStorageType, local_storage_gb: int = 0):
        self.local_storage_type = local_storage_type
        self.local_storage_gb = local_storage_gb
        
class NetworkConfig:
    def __init__(self, bandwidth: float, pps: int, peak_bandwidth: float = None):
        self.bandwidth = bandwidth  # 网络带宽，单位 Gbit/s
        self.pps = pps  # 网络收发包 单位万
        self.peak_bandwidth = peak_bandwidth if peak_bandwidth else bandwidth  # 突发带宽

class VMType:
    def __init__(self, name, cpu, memory_gb, price_per_hour, network: NetworkConfig, cloud_storage : CloudStorage = None, local_storage: LocalStorage = None):
        """
        参数说明：
        - name: VM 名称
        - cpu: vCPU 数量
        - memory_gb: 内存（单位：GB）
        - price_per_hour: 每小时价格
        """
        self.name = name
        self.cpu = cpu
        self.memory_gb = memory_gb
        self.network = network  
        self.cloud_storage = cloud_storage
        self.local_storage = local_storage        

        # 自动换算每秒价格
        self.price_per_hour = price_per_hour / 3600

    def __repr__(self):
        vm_info = f"VMType(name={self.name}, cpu={self.cpu}, memory_gb={self.memory_gb}, price_per_hour={self.price_per_hour:.4f})"
        if self.network:
            vm_info += f", network={self.network.bandwidth} Gbit/s, {self.network.pps} pps"
            if self.network.peak_bandwidth:
                vm_info += f", peak_bandwidth={self.network.peak_bandwidth} Gbit/s"
        if self.cloud_storage:
            vm_info += f", cloud_storage={self.cloud_storage.iops_base} IOPS, {self.cloud_storage.cloud_storage_bandwidth} Gbit/s"
            if self.cloud_storage.iops_peak:
                vm_info += f", peak_iops={self.cloud_storage.iops_peak} IOPS"
            if self.cloud_storage.cloud_storage_bandwidth_peak:
                vm_info += f", peak_bandwidth={self.cloud_storage.cloud_storage_bandwidth_peak} Gbit/s"
        if self.local_storage:
            vm_info += f", local_storage={self.local_storage.local_storage_type}, {self.local_storage.local_storage_gb} GB"
        return vm_info

# 阿里云部分 VM 类型，有的配置太高实验不需要就省略了
vm_types = [
    # 通用型 
    VMType(name="ecs.g8i.large", cpu=2, memory_gb=8, price_per_hour=0.827, network=NetworkConfig(2.5, 100, 15), cloud_storage=CloudStorage(2.5, 2,  20, 10)),
    VMType(name="ecs.g8i.xlarge", cpu=4, memory_gb=16, price_per_hour=1.655, network=NetworkConfig(4, 120, 15), cloud_storage=CloudStorage(5, 2.5,  20, 10)),
    VMType(name="ecs.g8i.2xlarge", cpu=8, memory_gb=32, price_per_hour=3.310, network=NetworkConfig(6, 160, 15), cloud_storage=CloudStorage(6, 4,  20, 10)),
    VMType(name="ecs.g8i.3xlarge", cpu=12, memory_gb=48, price_per_hour=4.965, network=NetworkConfig(10, 240, 15), cloud_storage=CloudStorage(8, 5,  20, 10)),
    VMType(name="ecs.g8i.4xlarge", cpu=16, memory_gb=64, price_per_hour=6.620, network=NetworkConfig(12, 300, 15), cloud_storage=CloudStorage(10, 6,  20, 10)),
    VMType(name="ecs.g8i.6large", cpu=24, memory_gb=96, price_per_hour=9.930, network=NetworkConfig(15, 450, 25), cloud_storage=CloudStorage(12, 7.5,  20, 10)),
    VMType(name="ecs.g8i.8large", cpu=32, memory_gb=128, price_per_hour=13.240, network=NetworkConfig(20, 600, 25), cloud_storage=CloudStorage(20, 10)),
    VMType(name="ecs.g8i.12large", cpu=48, memory_gb=192, price_per_hour=19.980, network=NetworkConfig(25, 900), cloud_storage=CloudStorage(30, 2)),
    VMType(name="ecs.g8i.16large", cpu=64, memory_gb=256, price_per_hour=26.480, network=NetworkConfig(25, 1200), cloud_storage=CloudStorage(36, 2)),
    # 计算型
    VMType(name="ecs.c8i.large", cpu=2, memory_gb=4, price_per_hour=0.677, network=NetworkConfig(2.5, 100, 15), cloud_storage=CloudStorage(2.5, 2,  20, 10)),
    VMType(name="ecs.c8i.xlarge", cpu=4, memory_gb=8, price_per_hour=1.354, network=NetworkConfig(4, 120, 15), cloud_storage=CloudStorage(5, 2.5,  20, 10)),
    VMType(name="ecs.c8i.2xlarge", cpu=8, memory_gb=16, price_per_hour=2.708, network=NetworkConfig(6, 160, 15), cloud_storage=CloudStorage(6, 4,  20, 10)),
    VMType(name="ecs.c8i.3xlarge", cpu=12, memory_gb=24, price_per_hour=4.06, network=NetworkConfig(10, 240, 15), cloud_storage=CloudStorage(8, 5,  20, 10)),
    VMType(name="ecs.c8i.4xlarge", cpu=16, memory_gb=32, price_per_hour=5.416, network=NetworkConfig(12, 300, 15), cloud_storage=CloudStorage(10, 6,  20, 10)),
    VMType(name="ecs.c8i.6xlarge", cpu=24, memory_gb=48, price_per_hour=8.124, network=NetworkConfig(15, 450, 25), cloud_storage=CloudStorage(12, 7.5,  20, 10)),
    VMType(name="ecs.c8i.8xlarge", cpu=32, memory_gb=64, price_per_hour=9.641, network=NetworkConfig(20, 600, 25), cloud_storage=CloudStorage(20, 10)),
    VMType(name="ecs.c8i.12xlarge", cpu=48, memory_gb=96, price_per_hour=16.249, network=NetworkConfig(25, 900), cloud_storage=CloudStorage(30, 2)),
    VMType(name="ecs.c8i.16xlarge", cpu=64, memory_gb=128, price_per_hour=21.666, network=NetworkConfig(25, 1200), cloud_storage=CloudStorage(36, 2)),
    # 内存型
    VMType(name="ecs.r8i.large", cpu=2, memory_gb=16, price_per_hour=1.046, network=NetworkConfig(2.5, 100, 15), cloud_storage=CloudStorage(2.5, 2,  20, 10)),
    VMType(name="ecs.r8i.xlarge", cpu=4, memory_gb=32, price_per_hour=2.093, network=NetworkConfig(4, 120, 15), cloud_storage=CloudStorage(5, 2.5,  20, 10)),
    VMType(name="ecs.r8i.2xlarge", cpu=8, memory_gb=64, price_per_hour=4.187, network=NetworkConfig(6, 160, 15), cloud_storage=CloudStorage(6, 4,  20, 10)),
    VMType(name="ecs.r8i.3xlarge", cpu=12, memory_gb=96, price_per_hour=6.281, network=NetworkConfig(10, 240, 15), cloud_storage=CloudStorage(8, 5,  20, 10)),
    VMType(name="ecs.r8i.4xlarge", cpu=16, memory_gb=128, price_per_hour=8.375, network=NetworkConfig(12, 300, 15), cloud_storage=CloudStorage(10, 6,  20, 10)),
    VMType(name="ecs.r8i.6xlarge", cpu=24, memory_gb=192, price_per_hour=12.563, network=NetworkConfig(15, 450, 25), cloud_storage=CloudStorage(12, 7.5,  20, 10)),
    VMType(name="ecs.r8i.8xlarge", cpu=32, memory_gb=256, price_per_hour=16.751, network=NetworkConfig(20, 600, 25), cloud_storage=CloudStorage(20, 10)),
    VMType(name="ecs.r8i.12xlarge", cpu=48, memory_gb=384, price_per_hour=25.126, network=NetworkConfig(25, 900), cloud_storage=CloudStorage(30, 2)),
    # 本地SSD型
    VMType(name="ecs.i2ne.xlarge", cpu=4, memory_gb=32, price_per_hour=2.257, network=NetworkConfig(1.5, 50), local_storage=LocalStorage(LocalStorage.LocalStorageType.SSD, 1788)),
    VMType(name="ecs.i2ne.2xlarge", cpu=8, memory_gb=64, price_per_hour=4.515, network=NetworkConfig(2.5, 100), local_storage=LocalStorage(LocalStorage.LocalStorageType.SSD, 1788)),
    VMType(name="ecs.i2ne.4xlarge", cpu=16, memory_gb=128, price_per_hour=9.03, network=NetworkConfig(5, 150), local_storage=LocalStorage(LocalStorage.LocalStorageType.SSD, 2 * 1788)),
    VMType(name="ecs.i2ne.8xlarge", cpu=32, memory_gb=256, price_per_hour=18.06, network=NetworkConfig(10, 200), local_storage=LocalStorage(LocalStorage.LocalStorageType.SSD, 4 * 1788)),
    VMType(name="ecs.i2ne.16xlarge", cpu=64, memory_gb=512, price_per_hour=36.12, network=NetworkConfig(20, 400),  local_storage=LocalStorage(LocalStorage.LocalStorageType.SSD, 8 * 1788)),
    # 大数据网络增强型
    VMType(name="ecs.d1ne.2xlarge", cpu=8, memory_gb=32, price_per_hour=5.472, network=NetworkConfig(6, 100), local_storage=LocalStorage(LocalStorage.LocalStorageType.HDD, 4 * 5500)),
    VMType(name="ecs.d1ne.4xlarge", cpu=16, memory_gb=64, price_per_hour=10.951, network=NetworkConfig(12, 160), local_storage=LocalStorage(LocalStorage.LocalStorageType.HDD, 8 * 5500)),
    VMType(name="ecs.d1ne.6xlarge", cpu=24, memory_gb=96, price_per_hour=16.423, network=NetworkConfig(16, 200), local_storage=LocalStorage(LocalStorage.LocalStorageType.HDD, 12 * 5500)),
    VMType(name="ecs.d1ne-c8d3.8xlarge", cpu=32, memory_gb=128, price_per_hour=21.04, network=NetworkConfig(20, 200), local_storage=LocalStorage(LocalStorage.LocalStorageType.HDD, 12 * 5500)),
    VMType(name="ecs.d1ne.8xlarge", cpu=32, memory_gb=128, price_per_hour=21.902, network=NetworkConfig(20, 250), local_storage=LocalStorage(LocalStorage.LocalStorageType.HDD, 16 * 5500)),
    VMType(name="ecs.d1-c14d3.14xlarge", cpu=56, memory_gb=160, price_per_hour=38.317, network=NetworkConfig(35, 450), local_storage=LocalStorage(LocalStorage.LocalStorageType.HDD, 12 * 5500)),
    VMType(name="ecs.d1ne.14xlarge", cpu=56, memory_gb=225, price_per_hour=38.317, network=NetworkConfig(35, 450), local_storage=LocalStorage(LocalStorage.LocalStorageType.HDD, 28 * 5500)),
]

for vm in vm_types:
    print(vm)
    
def get_vm_types():
    return vm_types