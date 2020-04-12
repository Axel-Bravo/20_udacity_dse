import json
import time
import boto3
import psycopg2
import configparser
from dataclasses import dataclass
from collections import namedtuple


config = configparser.ConfigParser()

aws = namedtuple('aws', ['key', 'secret'])
dwh = namedtuple('dwh', ['cluster_type', 'num_nodes', 'node_type'])
redshift = namedtuple('redshift', ['cluster_id', 'db_name', 'db_user', 'db_password', 'db_host', 'db_port'])
s3 = namedtuple('s3', ['log_data', 'log_json_path', 'song_data'])


@dataclass(init=False)
class AwsParameters(object):
    """ Data class to store the parameters issued from a Data Warehouse configuration file"""

    def __init__(self, configuration_path_aws: str, configuration_path: str, region: str = 'us-west-2') -> None:
        """
        Class constructor
        :param configuration_path_aws: configuration file with all required AWS parameters
        :param configuration_path: configuration file with AWS user data (confidential)
        """
        config.read_file(open(configuration_path_aws))
        self.aws = aws(key=config.get('AWS', 'KEY'),
                       secret=config.get('AWS', 'SECRET'))

        config.read_file(open(configuration_path))
        self.dwh = dwh(cluster_type=config.get("DWH", "DWH_CLUSTER_TYPE"),
                       num_nodes=int(config.get("DWH", "DWH_NUM_NODES")),
                       node_type=config.get("DWH", "DWH_NODE_TYPE"))

        self.redshift = redshift(cluster_id=config.get('REDSHIFT', 'CLUSTER_IDENTIFIER'),
                                 db_name=config.get("REDSHIFT", "DB_NAME"),
                                 db_user=config.get("REDSHIFT", "DB_USER"),
                                 db_password=config.get("REDSHIFT", "DB_PASSWORD"),
                                 db_host='',
                                 db_port=int(config.get("REDSHIFT", "DB_PORT")))

        self.s3 = s3(log_data=config.get("S3", "LOG_DATA"),
                     log_json_path=config.get("S3", "LOG_JSON_PATH"),
                     song_data=config.get("S3", "SONG_DATA"))

        self.iam = config.get("IAM_ROLE", "IAM_ROLE_NAME")
        self.region = region


class AwsExecutor(object):
    def __init__(self, aws_config: AwsParameters) -> None:
        self.config = aws_config

        self.ec2 = boto3.resource('ec2',
                                  aws_access_key_id=self.config.aws.key,
                                  aws_secret_access_key=self.config.aws.secret,
                                  region_name=self.config.region)

        self.iam = boto3.client('iam',
                                aws_access_key_id=self.config.aws.key,
                                aws_secret_access_key=self.config.aws.secret,
                                region_name=self.config.region
                                )

        self.redshift = boto3.client('redshift',
                                     aws_access_key_id=self.config.aws.key,
                                     aws_secret_access_key=self.config.aws.secret,
                                     region_name=self.config.region
                                     )

        self.role_arn = self._create_iam_role()

    def _create_iam_role(self) -> object:
        """
        Creates a AWS IAM Role
        :return: the IAM role ARN
        """
        try:
            print("Creating a new IAM Role")
            _ = self.iam.create_role(
                Path='/',
                RoleName=self.config.iam,
                Description="Allows Redshift clusters to call AWS services on your behalf.",
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{'Action': 'sts:AssumeRole',
                                    'Effect': 'Allow',
                                    'Principal': {'Service': 'redshift.amazonaws.com'}}],
                     'Version': '2012-10-17'})
            )
        except Exception as e:
            print(e)

        print("Attaching Policy")
        _ = self.iam.attach_role_policy(RoleName=self.config.iam,
                                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                                        )['ResponseMetadata']['HTTPStatusCode']

        print("Get the IAM role ARN")
        return [self.iam.get_role(RoleName=self.config.iam)['Role']['Arn']]

    def create_redshift(self) -> None:
        """
        Launches a redshift instance in AWS
        """
        try:
            print('Creating Redshift Cluster')
            _ = self.redshift.create_cluster(
                ClusterType=self.config.dwh.cluster_type,
                NodeType=self.config.dwh.node_type,
                NumberOfNodes=self.config.dwh.num_nodes,
                ClusterIdentifier=self.config.redshift.cluster_id,
                DBName=self.config.redshift.db_name,
                MasterUsername=self.config.redshift.db_user,
                MasterUserPassword=self.config.redshift.db_password,

                # Roles (for s3 access)
                IamRoles=self.role_arn
            )
        except Exception as e:
            print(e)

        while self._redshift_status() != 'available':
            time.sleep(15)
            print("Redshift cluster creation in progress")

        cluster_info = self.redshift.describe_clusters(ClusterIdentifier=self.config.redshift.cluster_id)['Clusters'][0]
        self.config.redshift.db_host = cluster_info['Endpoint']['Address']
        print('Redshift Cluster created')

    def _redshift_status(self) -> str:
        """
        Get redshift cluster current status
        :return: redshift cluster status
        """
        cluster_info = self.redshift.describe_clusters(ClusterIdentifier=self.config.redshift.cluster_id)['Clusters'][0]
        return [(k, v) for k, v in cluster_info.items() if k in ['ClusterStatus']][0][1]

    def open_redshift_access(self) -> None:
        """
        Open an incoming TCP port to access the cluster endpoint
        """
        cluster_info = self.redshift.describe_clusters(ClusterIdentifier=self.config.redshift.cluster_id)['Clusters'][0]

        try:
            print('Opening Redshift access')
            vpc = self.ec2.Vpc(id=cluster_info['VpcId'])
            default_sg = list(vpc.security_groups.all())[0]
            print(default_sg)
            default_sg.authorize_ingress(
                GroupName=default_sg.group_name,
                CidrIp='0.0.0.0/0',
                IpProtocol='TCP',
                FromPort=self.config.redshift.db_port,
                ToPort=self.config.redshift.db_port
            )
        except Exception as e:
            print(e)

        print('Redshift access opened')

    def test_redshift_connection(self) -> None:
        """
        Test that we can connect to the the cluster by PostGre SQL
        """
        conn = psycopg2.connect(database=self.config.redshift.db_name,
                                user=self.config.redshift.db_user,
                                password=self.config.redshift.db_password,
                                host=self.config.redshift.db_host,
                                port=self.config.redshift.db_port)

        conn.close()

        print("Test successful")

    def delete_redshift(self) -> None:
        """
        Deletes the AWS redshift cluster
        """
        print('Delete - Redshift Cluster - Start')
        self.redshift.delete_cluster(ClusterIdentifier=self.config.redshift.cluster_id,
                                     SkipFinalClusterSnapshot=True)

        while self._redshift_status() != 'deleted':
            time.sleep(15)
            print('Delete - Redshift Cluster - In progress')

        print('Delete - Redshift Cluster - End')

        print('Delete - IAM policy & role - Start')
        self.iam.detach_role_policy(RoleName=self.config.iam,
                                    PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        self.iam.delete_role(RoleName=self.config.iam)
        print('Delete - IAM policy & role - End')
