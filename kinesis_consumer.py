# Create the Kinesis Stream using the AWS CLI:
# aws kinesis create-stream --stream-name discover_kinesis --shard-count 1
# Delete using
# aws kinesis delete-stream --stream-name discover_kinesis

# https://aws.amazon.com/fr/blogs/big-data/snakes-in-the-stream-feeding-and-eating-amazon-kinesis-streams-with-python/

import json
import time

import boto3

def main():
    my_stream_name = 'discover_kinesis'
    kinesis_client = boto3.client('kinesis', region_name='eu-central-1')

    # Obtain the shard ID of the stream (should only be one shard in this stream)
    stream_descriptor = kinesis_client.describe_stream(StreamName=my_stream_name)
    my_shard_id = stream_descriptor['StreamDescription']['Shards'][0]['ShardId']
    print("Reading from shard {}".format(my_shard_id))

    # Get a shard iterator (https://docs.aws.amazon.com/fr_fr/kinesis/latest/APIReference/API_GetShardIterator.html)
    shard_iterator = kinesis_client.get_shard_iterator(
        StreamName=my_stream_name,
        ShardId=my_shard_id,
        ShardIteratorType="LATEST")["ShardIterator"]

    # Keep reading from the shard
    while True:
        record_response = kinesis_client.get_records(
            ShardIterator=shard_iterator,
            Limit=5
        )
        print(record_response)
        time.sleep(3)

        if 'NextShardIterator' in record_response:
            shard_iterator = record_response["NextShardIterator"]
        else:
            break

if __name__ == "__main__":
    # execute only if run as a script
    main()        
