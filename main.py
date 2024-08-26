try:
    import re
    import os
    import json
    import boto3
    import datetime
    import uuid
    from datetime import datetime
    import json
    from faker import Faker
    import random
    import faker
except Exception as e:
    print("Error : {} ".format(e))


def main():
 
    AWS_ACCESS_KEY = ""
    AWS_SECRET_KEY = ""
    AWS_REGION_NAME = ""
 
    for i in range(1, 25):
        faker = Faker()
        json_data = {
            "transaction_amount": round(random.uniform(1, 1000), 2),  # Generate transaction amounts between 1 and 1000
            "merchant_name": faker.company(),  # Generate fake merchant names
            "merchant_location": faker.address(),  # Generate fake merchant locations
            "transaction_timestamp": str(faker.date_time_this_year()),  # Generate fake transaction timestamps
            "transaction_type": random.choice(["purchase", "refund", "transfer"]),  # Randomly select transaction type
            "customer_name": faker.name(),  # Generate fake customer names
            "customer_address": faker.address(),  # Generate fake customer addresses
            "customer_phone": faker.phone_number(),  # Generate fake customer phone numbers
            "device_type": random.choice(["mobile", "desktop", "tablet"]),  # Randomly select device type
            "ip_address": faker.ipv4(),  # Generate fake IP addresses
            "transaction_status": random.choice(["approved", "declined", "pending"]),  # Randomly select transaction status
            "customer_id": str(random.randint(1, 1000))  # Generate fake customer IDs
        }
        print(json_data)
 
        client = boto3.client(
            "firehose",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION_NAME,
        )
 
        response = client.put_record(
            DeliveryStreamName='kinesis-rtfd',
            Record={
                'Data': json.dumps(json_data)
            }
        )
        print(response)
 
 
 
main()
 
"""
customer_id=!{partitionKeyFromQuery:customer_id}/
"""
