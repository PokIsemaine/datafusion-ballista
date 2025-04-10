import socket
import subprocess
from uuid import uuid4

def is_port_available(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)  # 设置超时时间，避免长时间等待
        result = s.connect_ex(("localhost", int(port)))
        return result != 0  # 如果返回值为 0，表示端口被占用；否则表示可用

def get_available_ports():
    for port in range(50061, 60000, 2):
        if is_port_available(port) and is_port_available(port + 1):
            yield port
            yield port + 1

def get_user_input(port_gen):
    while True:
        memory_limit = input("请输入内存限制 (整数, GB为单位): ")
        cpu_limit = input("请输入 CPU 限制 (例如 1): ")
        bind_port = next(port_gen)
        bind_grpc_port = next(port_gen)
        print(f"自动绑定的端口: {bind_port}, {bind_grpc_port}")
        return memory_limit, cpu_limit, bind_port, bind_grpc_port
             
def generate_container_name(cpu_limit, memory_limit):
    return f"executor_cpu{cpu_limit}_mem{memory_limit}_{uuid4()}"

def run_docker_command(memory_limit, cpu_limit, bind_port, bind_grpc_port):
    container_name = generate_container_name(cpu_limit, memory_limit)
    command = [
        "docker", "run",
        f"--memory={memory_limit}g",
        f"--cpus={cpu_limit}",
        f"--name={container_name}",
        f"-e BIND_PORT={bind_port}",
        f"-e BIND_GRPC_PORT={bind_grpc_port}",
        "--network=host",
        "-d",
        "-v", "/home/zsl/datafusion-ballista/benchmarks/data:/data",
        "apache/arrow-ballista-executor:latest",
        "--external-host", "localhost",
        f"--bind-port", str(bind_port),
        f"--bind-grpc-port", str(bind_grpc_port),
        "--task-scheduling-policy", "push-staged",
        "--cpu-limit", cpu_limit,
        "--memory-limit", memory_limit,
        "--executor-name", container_name
    ]
    
    command_str = " ".join(command)
    print(f"执行的 Docker 命令: {command_str}")
    
    try:
        result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(result.stdout.decode())
        if result.stderr:
            print(result.stderr.decode())
        return True
    except subprocess.CalledProcessError as e:
        print(f"启动 Docker 容器时出错: {e}\n错误详情: {e.stderr.decode() if e.stderr else '无详细错误信息'}")
        return False

def main():
    print("Docker 容器创建脚本已启动。输入 'q' 退出。")
    port_gen = get_available_ports()
    while True:
        user_input = input("按回车键继续创建新容器，或输入 'q' 退出: ").strip().lower()
        print("\n创建新容器：")
        
        if user_input == 'q':
            print("程序退出。")
            break
        memory_limit, cpu_limit, bind_port, bind_grpc_port = get_user_input(port_gen)
        run_docker_command(memory_limit, cpu_limit, bind_port, bind_grpc_port)
        
if __name__ == "__main__":
    main()