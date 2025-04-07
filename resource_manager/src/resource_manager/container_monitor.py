import docker

client = docker.from_env()

for container in client.containers.list():
    stats = container.stats(stream=False)
    name = container.name
    cpu_delta = stats["cpu_stats"]["cpu_usage"]["total_usage"] - stats["precpu_stats"]["cpu_usage"]["total_usage"]
    system_cpu_delta = stats["cpu_stats"]["system_cpu_usage"] - stats["precpu_stats"]["system_cpu_usage"]
    num_cpus = stats["cpu_stats"].get("online_cpus", 1)
    cpu_percent = (cpu_delta / system_cpu_delta) * num_cpus * 100 if system_cpu_delta > 0 else 0

    memory_usage = stats["memory_stats"]["usage"]
    memory_limit = stats["memory_stats"]["limit"]
    memory_percent = (memory_usage / memory_limit) * 100 if memory_limit > 0 else 0

    print(f"容器: {name}")
    print(f"  CPU使用率: {cpu_percent:.2f}%")
    print(f"  内存使用: {memory_usage / (1024**2):.2f} MiB / {memory_limit / (1024**2):.2f} MiB ({memory_percent:.2f}%)")
    print()
