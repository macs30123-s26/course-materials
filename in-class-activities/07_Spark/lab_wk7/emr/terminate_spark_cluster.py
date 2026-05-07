'''
# Sample usage from CLI (assumes that your AWS `credentials` are updated):
python terminate_spark_cluster.py --cluster_id j-XXXXXXXXXXXXX
'''

import argparse
import boto3

emr = boto3.client('emr')

def terminate_cluster(cluster_id):
    '''
    Sends termination request for EMR cluster given `cluster_id`
    '''
    response = emr.terminate_job_flows(JobFlowIds=[cluster_id])
    print(f'Sent termination request for {cluster_id}.')
    return response

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=
                                     'Terminate Spark Cluster on AWS EMR.')
    parser.add_argument('--cluster_id', type=str, required=True,
                        help='Cluster ID # for cluster you wish to terminate '
                        + '(in the form: "j-XXXXXXXXXXXXX")')
    args = parser.parse_args()

    r = terminate_cluster(args.cluster_id)