import socket
import subprocess

def is_port_available(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("localhost", int(port)))
            return True
        except socket.error:
            return False

def get_user_input():
    while True:
        memory_limit = input("请输入内存限制 (例如 300M): ")
        cpu_limit = input("请输入 CPU 限制 (例如 1): ")
        bind_port = input("请输入绑定端口 (例如 50051): ")
        bind_grpc_port = input("请输入绑定 gRPC 端口 (例如 50052): ")
        if not bind_port.isdigit() or not (0 < int(bind_port) < 65536):
            print("绑定端口格式无效，请输入 0 到 65535 之间的整数。")
            continue
        if not bind_grpc_port.isdigit() or not (0 < int(bind_grpc_port) < 65536):
            print("绑定 gRPC 端口格式无效，请输入 0 到 65535 之间的整数。")
            continue
        if not is_port_available(bind_port):
            print(f"bind_port 端口 {bind_port} 已被占用，请选择其他端口。")
            continue
        if not is_port_available(bind_grpc_port):
            print(f"bind_grpc_port 端口 {bind_grpc_port} 已被占用，请选择其他端口。")
            continue
        return memory_limit, cpu_limit, bind_port, bind_grpc_port
             
def generate_container_name(cpu_limit, memory_limit, index):
    return f"executor_cpu{cpu_limit}_mem{memory_limit}_{index}"

def run_docker_command(memory_limit, cpu_limit, bind_port, bind_grpc_port, container_index):
    container_name = generate_container_name(cpu_limit, memory_limit, container_index)
    command = [
        "docker", "run",
        f"--name={container_name}",
        f"-e BIND_PORT={bind_port}",
        f"-e BIND_GRPC_PORT={bind_grpc_port}",
        "--network=host",
        "-m", memory_limit,
        "-d",
        "-v", "/home/zsl/datafusion-ballista/benchmarks/data:/data",
        "apache/arrow-ballista-executor:45.0.0",
        "--external-host", "localhost",
        "--bind-port", bind_port,
        "--bind-grpc-port", bind_grpc_port,
        "--task-scheduling-policy", "push-staged"
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
    # TODO: 如果是多次运行创建，container_index 可能会重复
    container_index = 1
    while True:
        user_input = input("按回车键继续创建新容器，或输入 'q' 退出: ").strip().lower()
        print("\n创建新容器：")
        
        if user_input == 'q':
            print("程序退出。")
            break
        
        memory_limit, cpu_limit, bind_port, bind_grpc_port = get_user_input()
        if run_docker_command(memory_limit, cpu_limit, bind_port, bind_grpc_port, container_index):
            container_index += 1
        
if __name__ == "__main__":
    main()