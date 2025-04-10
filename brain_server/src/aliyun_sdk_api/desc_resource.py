# -*- coding: utf-8 -*-
# This file is auto-generated, don't edit it. Thanks.
import os
import sys
import csv

from typing import List

from alibabacloud_ecs20140526.client import Client as Ecs20140526Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_ecs20140526 import models as ecs_20140526_models
from alibabacloud_tea_util import models as util_models
from alibabacloud_tea_util.client import Client as UtilClient


class Sample:
    def __init__(self):
        pass

    @staticmethod
    def create_client() -> Ecs20140526Client:
        """
        使用AK&SK初始化账号Client
        @return: Client
        @throws Exception
        """
        config = open_api_models.Config(
            access_key_id=os.environ['ALIBABA_CLOUD_ACCESS_KEY_ID'],
            access_key_secret=os.environ['ALIBABA_CLOUD_ACCESS_KEY_SECRET']
        )
        config.endpoint = f'ecs.cn-hongkong.aliyuncs.com'
        return Ecs20140526Client(config)

    @staticmethod
    def get_available_instance_types(client: Ecs20140526Client) -> List[dict]:
        """
        获取当前区域中可用的并且有库存的实例规格及其配置信息
        """
        describe_instance_types_request = ecs_20140526_models.DescribeInstanceTypesRequest()
        runtime = util_models.RuntimeOptions()
        available_instance_types = []
        try:
            # 获取所有实例规格
            res = client.describe_instance_types_with_options(describe_instance_types_request, runtime)
            all_instance_types = {instance.instance_type_id: instance for instance in res.body.instance_types.instance_type}

            # 获取有库存的实例规格
            describe_available_resource_request = ecs_20140526_models.DescribeAvailableResourceRequest(
                region_id='cn-hongkong',
                destination_resource='InstanceType'
            )
            res = client.describe_available_resource_with_options(describe_available_resource_request, runtime)
            for zone in res.body.available_zones.available_zone:
                for resource in zone.available_resources.available_resource:
                    for supported_resource in resource.supported_resources.supported_resource:
                        if supported_resource.status == 'Available' and supported_resource.status_category == "WithStock":
                            instance_type_id = supported_resource.value
                            if instance_type_id in all_instance_types:
                                instance = all_instance_types[instance_type_id]
                                available_instance_types.append({
                                    "InstanceType": instance.instance_type_id,
                                    "CPU": instance.cpu_core_count,
                                    "Memory": instance.memory_size,
                                })
        except Exception as error:
            print(error.message)
            UtilClient.assert_as_string(error.message)
        return available_instance_types

    @staticmethod
    def get_instance_type_prices(client: Ecs20140526Client, instance_types: List[str]) -> List[dict]:
        """
        获取所有实例规格的价格
        """
        runtime = util_models.RuntimeOptions()
        prices = []
        for instance_type in instance_types:
            try:
                print(f"Fetching price for instance type: {instance_type}")
                describe_price_request = ecs_20140526_models.DescribePriceRequest(
                    region_id='cn-hongkong',
                    resource_type='instance',
                    instance_type=instance_type
                )
                res = client.describe_price_with_options(describe_price_request, runtime)
                prices.append({
                    "InstanceType": instance_type,
                    "Price": res.body.price_info.price.trade_price
                })
                
            except Exception as error:
                # 打印警告信息并跳过当前实例规格
                print(f"Warning: Failed to fetch price for instance type {instance_type}. Error: {error.message}")
        return prices

    @staticmethod
    def write_to_csv(data: List[dict], filename: str) -> None:
        """
        将数据写入到 CSV 文件
        """
        keys = data[0].keys() if data else []
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=keys)
            writer.writeheader()
            writer.writerows(data)

    @staticmethod
    def main(args: List[str]) -> None:
        client = Sample.create_client()

        # 1. 获取可用的并且有库存的实例规格及其配置信息
        available_instance_types = Sample.get_available_instance_types(client)
        print("Available Instance Types:", available_instance_types)

        # 2. 获取这些实例规格的价格
        instance_type_ids = [item["InstanceType"] for item in available_instance_types]
        prices = Sample.get_instance_type_prices(client, instance_type_ids)
        print("Instance Prices:", prices)

        # 3. 合并数据并输出到 CSV 文件
        merged_data = []
        for instance in available_instance_types:
            price = next((p["Price"] for p in prices if p["InstanceType"] == instance["InstanceType"]), None)
            merged_data.append({**instance, "Price": price})
        Sample.write_to_csv(merged_data, "instance_data.csv")
        print("Data written to instance_data.csv")


if __name__ == '__main__':
    Sample.main(sys.argv[1:])