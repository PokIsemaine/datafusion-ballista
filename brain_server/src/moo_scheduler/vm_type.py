class VMType:
    def __init__(self, name, cpu, memory_gb, price_per_hour):
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

        # 自动换算每秒价格
        self.price_per_hour = price_per_hour / 3600

    def __repr__(self):
        return (f"VMType(name={self.name}, cpu={self.cpu}, mem={self.memory_gb}GB, "
                f"price={self.price_per_hour:.4f}/s)")


vm_types = [
    # 通用型 
    VMType(name="ecs.g8i.large", cpu=2, memory_gb=8, price_per_hour=0.827),
    VMType(name="ecs.g8i.2xlarge", cpu=4, memory_gb=16, price_per_hour=1.655),
    VMType(name="ecs.g8i.3xlarge", cpu=12, memory_gb=32, price_per_hour=3.310),
    VMType(name="ecs.g8i.4xlarge", cpu=16, memory_gb=48, price_per_hour=4.965),
    VMType(name="ecs.g8i.6large", cpu=24, memory_gb=64, price_per_hour=6.620),
    VMType(name="ecs.g8i.8large", cpu=32, memory_gb=96, price_per_hour=9.930),
    VMType(name="ecs.g8i.12large", cpu=48, memory_gb=128, price_per_hour=13.240),
    VMType(name="ecs.g8i.16large", cpu=64, memory_gb=192, price_per_hour=19.980),
    # 计算型
    VMType(name="ecs.c8i.large", cpu=2, memory_gb=4, price_per_hour=0.677),
    VMType(name="ecs.c8i.xlarge", cpu=4, memory_gb=8, price_per_hour=1.354),
    VMType(name="ecs.c8i.2xlarge", cpu=8, memory_gb=16, price_per_hour=2.708),
    VMType(name="ecs.c8i.3xlarge", cpu=12, memory_gb=24, price_per_hour=4.06),
    VMType(name="ecs.c8i.4xlarge", cpu=16, memory_gb=32, price_per_hour=5.416),
    VMType(name="ecs.c8i.6xlarge", cpu=24, memory_gb=48, price_per_hour=8.124),
    VMType(name="ecs.c8i.8xlarge", cpu=32, memory_gb=64, price_per_hour=9.641),
    VMType(name="ecs.c8i.12xlarge", cpu=48, memory_gb=96, price_per_hour=16.249),
    VMType(name="ecs.c8i.16xlarge", cpu=64, memory_gb=128, price_per_hour=21.666),
    # 内存型
    VMType(name="ecs.r8i.large", cpu=2, memory_gb=16, price_per_hour=1.046),
    VMType(name="ecs.r8i.xlarge", cpu=4, memory_gb=32, price_per_hour=2.093),
    VMType(name="ecs.r8i.2xlarge", cpu=8, memory_gb=64, price_per_hour=4.187),
    VMType(name="ecs.r8i.3xlarge", cpu=12, memory_gb=96, price_per_hour=6.281),
    VMType(name="ecs.r8i.4xlarge", cpu=16, memory_gb=128, price_per_hour=8.375),
    VMType(name="ecs.r8i.6xlarge", cpu=24, memory_gb=192, price_per_hour=12.563),
    VMType(name="ecs.r8i.8xlarge", cpu=32, memory_gb=256, price_per_hour=16.751),
    VMType(name="ecs.r8i.12xlarge", cpu=48, memory_gb=384, price_per_hour=25.126),   
]

for vm in vm_types:
    print(vm)