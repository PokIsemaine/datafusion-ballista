#!/bin/bash
set -e
set -x

Q6_SCHEDULER_POLICY=(
    "1000:0,1001:0,2000:0"
    "1000:0,1001:0,2000:1"
    "1000:0,1001:1,2000:0"
    "1000:0,1001:1,2000:1"
    "1000:1,1001:0,2000:0"
    "1000:1,1001:0,2000:1"
    "1000:1,1001:1,2000:0"
    "1000:1,1001:1,2000:1"
)

policy_id=0
for gen_policy in ${Q6_SCHEDULER_POLICY[@]}
do
    policy_id=$((policy_id + 1))
    while true; do
        echo "Stopping and removing all containers..."
        # 如果 docker ps -q 不为空，则停止所有容器
        if [ "$(docker ps -q)" ]; then
            docker stop $(docker ps -q)
        fi
        if [ "$(docker ps -aq)" ]; then
            docker rm $(docker ps -aq)
        fi

        echo "Starting scheduler with policy$policy_id: $gen_policy"
        scheduler_name="scheduler$policy_id"
        docker run --name $scheduler_name --network=host -v /home/zsl/datafusion-ballista/benchmarks/data:/data -d apache/arrow-ballista-scheduler:latest --bind-port 50050 --scheduler-policy push-staged --task-distribution generated-policy --gen-policy $gen_policy
        sleep 1s
        echo "Starting executor..."
        docker run --memory=8g --cpus=4 --name=executor_cpu4_mem8_1 -e BIND_PORT=50061 -e BIND_GRPC_PORT=50062 --network=host -d -v /home/zsl/datafusion-ballista/benchmarks/data:/data apache/arrow-ballista-executor:latest --external-host localhost --bind-port 50061 --bind-grpc-port 50062 --task-scheduling-policy push-staged --cpu-limit 4 --memory-limit 8 --executor-name executor_cpu4_mem8_1
        docker run --memory=8g --cpus=4 --name=executor_cpu4_mem8_2 -e BIND_PORT=50063 -e BIND_GRPC_PORT=50064 --network=host -d -v /home/zsl/datafusion-ballista/benchmarks/data:/data apache/arrow-ballista-executor:latest --external-host localhost --bind-port 50063 --bind-grpc-port 50064 --task-scheduling-policy push-staged --cpu-limit 4 --memory-limit 8 --executor-name executor_cpu4_mem8_2
        sleep 1s
        # docker ps -q 应该有 3 行输出，如果没有就要重新启动
        if [ "$(docker ps -q | wc -l)" -eq 3 ]; then
            break
        else
            echo -e "\e[31mDocker containers are not running as expected. Retrying...\e[0m"
        fi
    done

    # docker ps -q 应该有 3 行输出，如果没有就要重新启动
    if [ "$(docker ps -q | wc -l)" -ne 3 ]; then
        echo "$policy_id skipped, because docker ps -q is not 3"
    else
        benchmark_name="benchmark_tpchq6_$policy_id"
        docker run --name $benchmark_name --network=host -v /home/zsl/datafusion-ballista/benchmarks/data:/data apache/arrow-ballista-benchmarks-query:latest --benchmark-name $benchmark_name --query 23  --partitions 16
    fi
done