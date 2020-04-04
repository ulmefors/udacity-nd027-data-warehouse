import argparse
import configparser
import json
import logging
import os

import boto3


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY = os.environ['AWS_ACCESS_KEY_ID']
SECRET = os.environ['AWS_SECRET_ACCESS_KEY']
DWH_IAM_ROLE_NAME = config['CLUSTER']['DWH_IAM_ROLE_NAME']
REGION = config['CLUSTER']['REGION']
S3_READ_ARN = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"


def create_resources():
    options = dict(region_name=REGION, aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    ec2 = boto3.resource('ec2', **options)
    s3 = boto3.resource('s3', **options)
    iam = boto3.client('iam', **options)
    redshift = boto3.client('redshift', **options)
    return ec2, s3, iam, redshift


def create_iam_role(iam):
    dwh_role = iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        AssumeRolePolicyDocument=json.dumps({
            'Statement': [{
                'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}
            }],
            'Version': '2012-10-17'
        })
    )
    iam.attach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn=S3_READ_ARN
    )
    role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    logging.info('Created role {} with arn {}'.format(DWH_IAM_ROLE_NAME, role_arn))


def delete_iam_role(iam):
    role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn=S3_READ_ARN)
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    logging.info('Deleted role {} with arn {}'.format(DWH_IAM_ROLE_NAME, role_arn))


def main(args):
    ec2, s3, iam, redshift = create_resources()
    if args.delete:
        delete_iam_role(iam)
    else:
        create_iam_role(iam)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument('--delete', dest='delete', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
