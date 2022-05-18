from concurrent.futures import process
import json
import configparser
import argparse
import time
import sys
import boto3


def create_iam_role(iam_client):
    """
    Create an AWS iam role
    :param iam_client: an iam client
    :returns: True if success, otherwise False
    """

    try:
        print('1 Creating a new IAM Role')
        dwh_role = iam_client.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description='Allow Redshift clusters to call AWS services on your behalf.',
            AssumeRolePolicyDocument=json.dumps({
                'Statement':[{'Action':'sts:AssumeRole',
                              'Effect':'Allow',
                              'Principal':{'Service': 'redshift.amazonaws.com'}}],
                'Version':'2012-10-17'
            })
        )

    except Exception as exception:
        print(exception)
        return False

    return True if (dwh_role['ResponseMetadata']['HTTPStatusCode'] == 200) else False



def attach_iam_role_policy(iam_client):
    """
    Attach policy to iam role
    :param iam_client: an iam client
    :returns: True if success, otherwise False
    """

    try:
        print('2 Attaching a new policy to an IAM Role')
        attach_policy = iam_client.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")

    except Exception as exception:
        print(exception)
        return False
    return True if (attach_policy['ResponseMetadata']['HTTPStatusCode'] == 200) else False


def detach_iam_role_policy(iam_client):
    """
    Detach policy to iam role
    :param iam_client: an iam client
    :returns: True if success, otherwise False
    """

    try:
        print("Detaching policy")
        detach_response = iam_client.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    except Exception as exception:
        print(exception)
        return False
    return True if (detach_response['ResponseMetadata']['HTTPStatusCode'] == 200) else False


def delete_iam_role(iam_client):
    """
    Delete policy to iam role
    :param iam_client: an iam client
    :returns: True if success, otherwise False
    """

    try:
        print("Deleting role")
        delete_response = iam_client.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    except Exception as exception:
        print(exception)
        return False
    return True if (delete_response['ResponseMetadata']['HTTPStatusCode'] == 200) else False


def create_cluster(redshift_client, role_arn):
    """
    Create a Redshift cluster using the IAM role created.
    Reference: https://docs.aws.amazon.com/redshift/latest/APIReference/API_CreateCluster.html
    :param redshift_client: a redshift client instance
    :param vpc_security_group_id: vpc group for network setting for cluster
    :return: True if cluster created successfully.
    """

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    #DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")

    try:
        print("3 Creating Cluster")
        response = redshift_client.create_cluster(      
            # add parameters for hardware
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            #NumberOfNodes=int(DWH_NUM_NODES), #not required if clusterType is single-node
            DBName=DWH_DB,

            # add parameters for identifiers & credentials
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,

            # add parameter for role (to allow s3 access)
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            IamRoles=[role_arn],

            EnhancedVpcRouting=True
        )

        process_count = 1
        while redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER) \
            ['Clusters'][0]['ClusterStatus'] == 'creating':
            dot = "." * process_count
            sys.stdout.write(f'\rcluster is creating{dot}')
            #print("cluster is creating...")
            process_count += 1
            time.sleep(10)
        print("\ncluster is available")
        endpoint = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER) \
            ['Clusters'][0]['Endpoint']['Address']
        print(f"Cluster Endpoint: {endpoint}")

    except Exception as exception:
        print(exception)
        return False

    return (response['ResponseMetadata']['HTTPStatusCode'] == 200)

def delete_cluster(redshift_client):
    """
    Deleting the redshift cluster
    :param redshift_client: a redshift client instance
    :return: True if cluster deleted successfully.
    """

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")

    if(len(redshift_client.describe_clusters()['Clusters']) == 0):
        #logger.info(f"Cluster {DWH_CLUSTER_IDENTIFIER} does not exist.")
        return True

    try:
        print("Deleting cluster")
        response = redshift_client.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    except Exception as exception:
        print(exception)
        return False
    
    return response['ResponseMetadata']['HTTPStatusCode']

def security_group_setting(ec2_client):
    """
    Open an incoming TCP port to access the cluster ednpoint
    """
    DWH_PORT               = config.get("DWH","DWH_PORT")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    cluster_info = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    try:
        print("1.4 Setting security group")
        vpc_id = cluster_info['VpcId']
        vpc = ec2_client.Vpc(id=vpc_id)
        default_security_grp = list(vpc.security_groups.all())[0]
        print(default_security_grp)

        for permissons in default_security_grp.ip_permissions:
            if permissons['FromPort'] == int(DWH_PORT) \
            and permissons['ToPort'] == int(DWH_PORT) \
            and permissons['IpProtocol'] == 'tcp' \
            and permissons['IpRanges'][0]['CidrIp'] == '0.0.0.0/0':
                print('the specified rule already exists')
                return False

            else:
                response= default_security_grp.authorize_ingress(
                    GroupName= default_security_grp.group_name,  
                    CidrIp='0.0.0.0/0',
                    IpProtocol='TCP',
                    FromPort=int(DWH_PORT),
                    ToPort=int(DWH_PORT)
                )
                return response['ResponseMetadata']['HTTPStatusCode']
    except Exception as exception:
        print(exception)
        return False


if __name__ == "__main__":

    config = configparser.ConfigParser()
    config.read_file(open('credentials.cfg'))

    KEY                    = config.get('AWS','ACCESS_KEY')
    SECRET                 = config.get('AWS','SECRET_ACCESS_KEY')
    REGION                 = config.get("AWS", 'AWS_DEFAULT_REGION')
    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

    iam = boto3.client('iam', region_name=REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET)

    redshift = boto3.client('redshift', region_name=REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET)

    ec2 = boto3.resource('ec2', region_name=REGION,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)

    parser = argparse.ArgumentParser()
    parser.add_argument("-c"
                        , "--create"
                        , required=True
                        , help="True or False. Create IAM role, " \
                               "security group and redshift cluster if it doesn't exist")
    parser.add_argument("-d"
                        , "--delete"
                        , required=True
                        , help="True or False. Delete IAM role, security group and redshift cluster if it exists")
    args = parser.parse_args()
    
    if (args.create=='true'):
        if (create_iam_role(iam_client=iam)):
            if(attach_iam_role_policy(iam_client=iam)):               
                arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
                create_cluster(redshift_client=redshift, role_arn=arn)
                security_group_setting(ec2_client=ec2)


    if (args.delete=='true'):
        delete_cluster(redshift_client=redshift)
        if (detach_iam_role_policy(iam_client=iam)):
            delete_iam_role(iam_client=iam)
