'''
# Example Usage (assumes that your AWS `credentials` are correct and updated)

# Basic usage from CLI (filling in your S3 bucket name):
python launch_spark_cluster.py \
    --s3_bucket YOUR_BUCKET_NAME \
    --primary_count 1 \
    --core_count 2 \
    --instance_type "m5.xlarge"

# More advanced usage, with additional app + configs
python launch_spark_cluster.py \
    --s3_bucket YOUR_BUCKET_NAME \
    --primary_count 1 \
    --core_count 2 \
    --instance_type "m5.xlarge" \
    --additional_apps "Zeppelin" \
    --additional_configs '{"Classification": "livy-conf", "Properties": {"livy.server.session.timeout-check": "false"}}'
'''

import time
import boto3
import json
import argparse

emr = boto3.client('emr')
ec2 = boto3.client('ec2')

def launch_cluster(name='Spark Cluster', emr_release='emr-6.2.0', 
                   instance_type='m5.xlarge', primary_count=1, core_count=2,
                   persistence_bucket=None, ec2_key="vockey", 
                   additional_apps=[], additional_configs=[]):
    '''
    Launches Spark-enabled EMR cluster & JupyterHub server, given a name,
        EMR release number, EC2 instance type, primary node count,
        core node count, as well as EC2 key name, and any additional apps or
        configs the user would like to install on the cluster (Hadoop, Hive,
        JupyterEnterpriseGateway, JupyterHub, Livy, Pig, Spark, and Tez are
        installed by default).

    User must provide a valid S3 bucket to persist their JupyterHub data.
    '''
    if not persistence_bucket or persistence_bucket == 'YOUR_BUCKET_NAME':
        m = 'No S3 persistence bucket specified; ' \
            + 'you must provide a valid bucket to save your work'
        raise m

    # Populate apps and configs + any additional user specs
    apps = [
            {'Name': 'Hadoop'}, {'Name': 'Hive'}, 
            {'Name': 'JupyterEnterpriseGateway'}, {'Name': 'JupyterHub'},
            {'Name': 'Livy'}, {'Name': 'Pig'}, {'Name': 'Spark'},
            {'Name': 'Tez'}
        ]
    apps.extend([{'Name': a} for a in additional_apps])
    configs = [{
            'Classification': 'jupyter-s3-conf',
            'Properties': {
                's3.persistence.enabled': 'true',
                's3.persistence.bucket': persistence_bucket
            }
        }]
    configs.extend(additional_configs)

    response = emr.run_job_flow(
        Name=name,
        ReleaseLabel=emr_release,
        Applications=apps,
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Primary node',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': instance_type,
                    'InstanceCount': primary_count,
                },
                {
                    'Name': 'Core nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': instance_type,
                    'InstanceCount': core_count,
                }
            ],
            'Ec2KeyName': ec2_key,
            'KeepJobFlowAliveWhenNoSteps': True,
        },
        Configurations=configs,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole'
    )
    return response['JobFlowId']


def enable_ssh(cluster_id):
    '''
    Given an EMR Cluster ID, adds SSH inbound rule for all IPs on primary node
    security group.
    '''
    sg_id = None
    while True:
        cluster_info = emr.describe_cluster(ClusterId=cluster_id)['Cluster']
        
        # Security group info populated once state is 'STARTING' or later
        sg_id = cluster_info.get('Ec2InstanceAttributes', {}) \
                            .get('EmrManagedMasterSecurityGroup')
        
        if sg_id:
            break

        # wait to poll again if not populated yet
        time.sleep(5)

    # Add inbound rule for SSH (Port 22) from all IPs for primary node SG  
    try:
        ec2.authorize_security_group_ingress(
            GroupId=sg_id,
            IpProtocol='tcp',
            FromPort=22,
            ToPort=22,
            CidrIp='0.0.0.0/0'
        )
        print(f"SSH access enabled for primary node")
    except ec2.exceptions.ClientError as e:
        if 'InvalidPermission.Duplicate' in str(e):
            print("SSH access already enabled for primary node.")
        else:
            raise e


def cluster_ready(cluster_id, ec2_key='vockey', port_forwarding='9443'):
    '''
    Waits until cluster is running, then prints SSH & port forwarding commands
    to access cluster CLI + JupyterHub server
    '''
    print(f"Waiting for cluster {cluster_id} to be ready...")
    waiter = emr.get_waiter('cluster_running')
    
    try:
        waiter.wait(ClusterId=cluster_id)
        
        # Once ready, get the DNS name of the primary node
        cluster_info = emr.describe_cluster(ClusterId=cluster_id)['Cluster']
        public_dns = cluster_info['MasterPublicDnsName']
        
        print('Cluster is ready')
        print('Use the following command to SSH into the Primary Node ',
            '(inserting the path to your own PEM file after the -i flag):')
        print(f'ssh -i {ec2_key}.pem hadoop@{public_dns}')
        print(f'Use the following command to forward port {port_forwarding} ',
              f'for Jupyter access at https://localhost:{port_forwarding}/ ',
              '(inserting the path to your own PEM file after the -i flag):')
        print(f'ssh -i {ec2_key}.pem -NL ',
              f'{port_forwarding}:localhost:{port_forwarding} ',
              f'hadoop@{public_dns}')
        
    except Exception as e:
        print(f"An error occurred while waiting for cluster to launch: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=
                        "Launch EC2 instances for parallel scraping.")
    parser.add_argument('--s3_bucket', type=str, required=True,
                        help=
                        'S3 bucket that you have access to in your account. '
                        + 'Setting this parameter will automatically save any '
                        + 'Jupyter Notebooks you create to this S3 bucket, '
                        + 'meaning they will persist even after you terminate '
                        + 'your EMR cluster.')
    parser.add_argument('--primary_count', type=int, default=1,
                        help=
                        'Number of primary nodes you are requesting for your '
                        + 'Spark Cluster (default: 1).')
    parser.add_argument('--core_count', type=int, default=2,
                        help=
                        'Number of core nodes you are requesting for your '
                        + 'Spark Cluster (default: 2).')
    parser.add_argument('--instance_type', type=str, default='m5.xlarge',
                        help='EC2 instance type (default: m5.xlarge).')
    parser.add_argument('--ec2_key', type=str, default='vockey',
                        help='Name of your EC2 key pair (default: vockey).')
    parser.add_argument('--cluster_name', type=str, default='Spark Cluster',
                        help='Name for your Spark Cluster '
                        + '(default: Spark Cluster).')
    parser.add_argument('--emr_release', type=str, default='emr-6.2.0',
                        help='EMR Release Number (default: emr-6.2.0).')
    parser.add_argument('--additional_apps', nargs='+', type=str, default=[],
                        help=
                        'List of additional EMR applications, separated by '
                        + 'spaces, to install to EMR Cluster (the following '
                        + 'will already be installed: Hadoop, Hive, '
                        + 'JupyterEnterpriseGateway, JupyterHub, Livy, Pig, '
                        + 'Spark, Tez)')
    parser.add_argument('--additional_configs', nargs='+', type=str, 
                        default=[], help=
                        'Any additional EMR configurations, designated as '
                        + 'JSON strings separated by spaces (S3 persistence '
                        + 'bucket is already defined by default)')
    args = parser.parse_args()

    # launch cluster with user's params
    cluster_id = launch_cluster(
                        name=args.cluster_name,
                        emr_release=args.emr_release, 
                        instance_type=args.instance_type,
                        primary_count=args.primary_count,
                        core_count=args.core_count,
                        persistence_bucket=args.s3_bucket,
                        ec2_key=args.ec2_key,
                        additional_apps=args.additional_apps,
                        additional_configs=[json.loads(c) 
                                            for c in args.additional_configs]
                    )
    print(f'Cluster {cluster_id} is launching with {args.primary_count} ',
          f'primary and {args.core_count} core nodes ({args.instance_type}).')
    
    # enable SSH access if not already enabled
    enable_ssh(cluster_id)

    # wait until cluster is ready and print out sample command for ssh access
    cluster_ready(cluster_id, ec2_key=args.ec2_key)