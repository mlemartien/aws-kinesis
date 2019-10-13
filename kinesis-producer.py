# Create the Kinesis Stream using the AWS CLI:
# aws kinesis create-stream --stream-name discover_kinesis --shard-count 1

import json
import uuid

import boto3

# https://faker.readthedocs.io/en/stable/index.html
# pip install fake-factory==0.6.0
from faker import Factory
from faker.providers import internet, date_time


def single_put_to_stream(kinesis_client, stream_name, record):
    put_response = kinesis_client.put_record(
        StreamName=stream_name,
        Data=json.dumps(record),
        PartitionKey=str(uuid.uuid4())
    )
    print(put_response)


def multi_put_to_stream(kinesis_client, stream_name, records):
    put_response = kinesis_client.put_records(
        StreamName=stream_name,
        Records=records,
        PartitionKey=str(uuid.uuid4())
    )
    print(put_response)

def main():
    fake = Factory.create("fr_FR")
    fake.add_provider(internet)
    fake.add_provider(date_time)

    my_stream_name = 'discover_kinesis'
    kinesis_client = boto3.client('kinesis', region_name='eu-central-1')

    # Add 10 records using multiple putrecord() calls
    for _ in range(0, 10):
        payload = {
            "name": fake.name(),
            "ssn": fake.ssn(),
            "position": fake.job(),
            "address": fake.address(),
            "ip_address": fake.ipv4(),
            "mac_address": fake.mac_address(),
            "timestamp": fake.iso8601()
        }

        single_put_to_stream(
            kinesis_client,
            my_stream_name,
            payload
        )
    
    # Add another 10 records by batching/collecting using single putrecords() call
    kinesis_records = []

    for _ in range(0, 10):
        payload = {
            "name": fake.name(),
            "ssn": fake.ssn(),
            "position": fake.job(),
            "address": fake.address(),
            "ip_address": fake.ipv4(),
            "mac_address": fake.mac_address(),
            "timestamp": fake.iso8601()
        }
        kinesis_records.append(
            payload)

    multi_put_to_stream(
        kinesis_client,
        my_stream_name,
        kinesis_records
    )

if __name__ == "__main__":
    # execute only if run as a script
    main()
