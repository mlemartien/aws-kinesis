# Create the Kinesis Stream using the AWS CLI:
# aws kinesis create-stream --stream-name discover_kinesis --shard-count 1

import json
import uuid

import boto3

# https://faker.readthedocs.io/en/stable/index.html
# pip install fake-factory==0.6.0
from faker import Factory
from faker.providers import internet, date_time

def create_sample_payload(faker):
    sample_payload = {
        "name": faker.name(),
        "ssn": faker.ssn(),
        "position": faker.job(),
        "address": faker.address(),
        "ip_address": faker.ipv4(),
        "mac_address": faker.mac_address(),
        "timestamp": faker.iso8601()
    }
    return sample_payload


def single_put_to_stream(kinesis_client, stream_name, payload):
    put_response = kinesis_client.put_record(
        StreamName=stream_name,
        Data=json.dumps(payload),
        PartitionKey=str(uuid.uuid4())
    )
    # print(put_response)


def multi_put_to_stream(kinesis_client, stream_name, payloads, batch_size=5):
    i = 0
    kinesis_records = []

    # Create a list of kinesis records
    for payload in payloads:
        i = i + 1
        kinesis_records.append({
            "Data": json.dumps(payload),
            "PartitionKey": str(uuid.uuid4())
        })

        # Every batch_size sends a put_records command
        if i % batch_size == 0:
            put_response = kinesis_client.put_records(
                Records=kinesis_records,
                StreamName=stream_name
            )
            print("Batch #{} - Failed records: {}".format(int(i / batch_size), put_response.get("FailedRecordCount")))
            kinesis_records = []


def main():
    fake = Factory.create("fr_FR")
    fake.add_provider(internet)
    fake.add_provider(date_time)

    my_stream_name = 'discover_kinesis'
    kinesis_client = boto3.client('kinesis', region_name='eu-central-1')

    # Add 10 records using multiple putrecord() calls
    for _ in range(0, 10):
        single_put_to_stream(
            kinesis_client=kinesis_client,
            stream_name=my_stream_name,
            payload=create_sample_payload(fake)
        )
    
    # Add another 10 records by batching/collecting using single putrecords() call
    payloads = []
    for _ in range(0, 1000):
        payloads.append(create_sample_payload(fake))

    multi_put_to_stream(
        kinesis_client=kinesis_client,
        stream_name=my_stream_name,
        payloads=payloads,
        batch_size=250
    )
if __name__ == "__main__":
    # execute only if run as a script
    main()
