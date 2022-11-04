import boto3
import click
import pytz
from botocore.config import Config
from boto3.session import Session
from datetime import datetime, timedelta


def set_config(timeout: int = 10):
    return Config(connect_timeout=timeout, read_timeout=15, retries={'max_attempts': 2})


# todo: address nested exceptions in socket timeout when service is inaccessible
def get_client(region: str, endpoint: str, timeout: int):
    try:
        config = Config(connect_timeout=timeout, read_timeout=timeout, retries={'max_attempts': 1})
        client = boto3.client('s3', region_name=region, endpoint_url=endpoint, config=config)
        return client
    except:
        print("Connection timeout error to S3.")
        exit(1)


# get all the common prefixes ("folders") in the bucket
def get_prefixes(s3: Session.client, bucket: str) -> dict:
    prefixes = {}
    try:
        paginator = s3.get_paginator('list_objects_v2')
        for result in paginator.paginate(Bucket=bucket, Delimiter="/"):
            for prefix in result.get('CommonPrefixes'):
                prefixes[prefix.get('Prefix')] = ""
        return prefixes
    except:
        print('Error getting folder list from s3.')
        exit(1)


# make an assumption that the first key in the deployment "folder" is a close enough representation
# of the deployment timestamp and use that as the "folder timestamp"
def get_ordered_prefix_timestamps(s3: Session.client, bucket: str, prefixes: dict) -> dict:
    current_prefix = None
    try:
        for prefix in prefixes:
            current_prefix = prefix
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
            prefixes[prefix] = response.get('Contents')[0].get('LastModified')

        # sort by date desc
        return dict(sorted(prefixes.items(), key=lambda item: item[1], reverse=True))
    except:
        print(f'Error getting folder list sorted by timestamp. Error occurred on prefix {current_prefix}')
        exit(1)


# get all the prefixes based on count/deployment date
def get_prefixes_to_delete(prefixes: dict, count: int, days: int = 0) -> list:
    prefixes_to_delete = []
    actual_count = 0
    days_timestamp = None
    utc = pytz.UTC

    if days > 0:
        days_timestamp = utc.localize(datetime.combine(datetime.today(), datetime.min.time()) + timedelta(days=-days))

    for prefix in prefixes:
        if actual_count < count:
            actual_count += 1
            continue
        if days > 0:
            timestamp = prefixes[prefix]
            if timestamp < days_timestamp:
                prefixes_to_delete.append(prefix)
        else:
            prefixes_to_delete.append(prefix)

    return prefixes_to_delete


# delete the actual objects from the keys obtained in `delete_objects_by_prefix`
# since that function is using a pagination process, we do not need to consider 1000 object limits
def delete_objects_from_bucket(s3: Session.client, bucket: str, prefix: str, keys: list) -> dict:
    payload = {'Objects': [], 'Quiet': True}
    for key in keys:
        payload['Objects'].append({'Key': key})

    try:
        return s3.delete_objects(Bucket=bucket, Delete=payload)
    except:
        print(f'There was an error deleting objects for prefix {prefix}')
        exit(1)


# paginate over all objects in the prefix, call the delete process, merge the errors and bubble up
def delete_objects_by_prefix(s3: Session.client, bucket: str, prefix: str) -> dict:
    response = {'Errors': []}

    try:
        paginator = s3.get_paginator('list_objects_v2')
        for result in paginator.paginate(Bucket=bucket, Prefix=prefix):
            keys = []
            for obj in result.get('Contents'):
                keys.append(obj['Key'])
            resp = delete_objects_from_bucket(s3=s3, bucket=bucket, prefix=prefix, keys=keys)
            if 'Errors' in resp:
                response['Errors'] = response['Errors'] + resp['Errors']
        return response
    except:
        print(f'Unable to fetch keys for prefix {prefix}')
        exit(1)


# iterate over all the prefixes and delete all the objects within
def delete_prefixes_from_bucket(s3: Session.client, bucket: str, prefixes: list):
    response = {'Errors': []}
    for prefix in prefixes:
        print(f'Deleting prefix {prefix}...')
        del_response = delete_objects_by_prefix(s3=s3, bucket=bucket, prefix=prefix)
        if 'Errors' in del_response:
            response['Errors'] = response['Errors'] + del_response['Errors']

    if len(response['Errors']) > 0:
        for error in response['Errors']:
            print(f'Deleting key {error["Key"]} produced error: {error["Message"] }')
        exit(1)


@click.command()
@click.argument("bucket_name", type=str, nargs=1)
@click.argument("count", type=int, nargs=1)
@click.option("--days", "-d", type=int,
              help="Maximum days to retain deployments. (Must also meet minimum COUNT requirements.)")
@click.option("--endpoint", "-e", help="Endpoint URL")
@click.option("--profile", "-p", type=str, help="AWS credentials profile to use for this session.")
@click.option("--region", "-r", default="us-east-1", help="Region for S3 bucket connection.")
@click.option("--timeout", "-t", type=int, default=10, help="Default connect/read timeouts for S3 connection.")
def main(bucket_name: str, count: int, days: int, endpoint: str, profile: str, region: str, timeout: int):
    try:
        if profile is not None:
            boto3.setup_default_session(profile_name=profile)
    except:
        print(f'AWS profile {profile} not found. Please provide a valid profile.')
        exit(1)

    if days is None:
        days = 0

    s3 = get_client(region=region, endpoint=endpoint, timeout=timeout)

    prefixes = get_prefixes(s3=s3, bucket=bucket_name)
    ordered_prefixes = get_ordered_prefix_timestamps(s3=s3, bucket=bucket_name, prefixes=prefixes)
    prefixes_to_delete = get_prefixes_to_delete(prefixes=ordered_prefixes, count=count, days=days)
    delete_prefixes_from_bucket(s3=s3, bucket=bucket_name, prefixes=prefixes_to_delete)
    print('S3 Deploy Cleanup Succeeded.')


if __name__ == "__main__":
    main()
