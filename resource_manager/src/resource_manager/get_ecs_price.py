import sys
import json
from alibabacloud_ecs20140526.client import Client as EcsClient
from alibabacloud_tea_openapi.models import Config
from alibabacloud_tea_util.models import RuntimeOptions
from alibabacloud_ecs20140526.models import DescribeInstanceTypesRequest

def create_client(access_key_id: str, access_key_secret: str):
    config = Config(
        access_key_id=access_key_id,
        access_key_secret=access_key_secret,
    )
    config.endpoint = "ecs.aliyuncs.com"
    return EcsClient(config)

def get_pay_as_you_go_prices(client, region_id="cn-hongkong"):
    request = DescribeInstanceTypesRequest(
        RegionId=region_id  
    )
    response = client.describe_instance_types_with_options(request, RuntimeOptions())
    return response.to_map()
def get_spot_instance_prices(client, region_id="cn-hongkong"):
    request = {"RegionId": region_id}
    response = client.describe_spot_price_history_with_options(request, RuntimeOptions())
    return response.to_map()

def main():
    access_key_id = "your-access-key-id"
    access_key_secret = "your-access-key-secret"

    client = create_client(access_key_id, access_key_secret)
    
    print("Fetching Pay-As-You-Go prices...")
    payg_prices = get_pay_as_you_go_prices(client)
    print(json.dumps(payg_prices, indent=4))
    
    print("Fetching Spot Instance prices...")
    spot_prices = get_spot_instance_prices(client)
    print(json.dumps(spot_prices, indent=4))

if __name__ == "__main__":
    main()
