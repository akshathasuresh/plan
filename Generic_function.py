import ibm_db
import ibm_db_dbi as db2
import boto3
import os
import logging
import warnings

warnings.filterwarnings('ignore')
#from src.awslambda.Credentials import DEV_ODS_PWD, DEV_ODS_UID, DEV_HOSTNAME, DEV_DATABASENAME, DEV_PORT_NUM

# -----------------------------------------------
# LOG_LEVEL to be set in environ variables. Default set to 'DEBUG'.
LOG_LEVEL = os.getenv('LOG_LEVEL', 'DEBUG')

# custom logger is created and handler is attached to filter the logs containing PII data. Additional loggers can be created,
# if required but mandatory to attach the stream_handler.
logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)
# logger.addHandler(stream_handler)

# START EDITING CODE FROM HERE.
logger.info('Loading function')
# ----------------------------------------------------------------------------------

# ------------------------------------------------
# Create SQS client
region = os.getenv('REGION')
if region is None:
    region = 'us-east-1'
# --------------------------------------------------

s3 = boto3.client('s3', region_name='us-east-1')

# ---------------------------------------------------





ssm_client = boto3.client('ssm', region_name='us-east-1')
environment = os.environ.get("Environment")
ods_prefix = "/dba/ifx/"
user = "dbuser"
password = "dbpassword"

def get_db2_parameters():

    dbuser = str(ods_prefix) + str(environment) + "/" + str(user)
    dbpassword = str(ods_prefix) + str(environment) + "/" + str(password)
    dbhostname = str("/db2/ifx/") + str(environment) + "/" + str("dbhostname")
    dbname = str("/db2/ifx/") + str(environment) + "/" + str("dbname")
    dbport = str("/db2/ifx/") + str(environment) + "/" + str("dbport")

    parameter_names = [dbuser, dbpassword, dbhostname, dbname, dbport]
    response = ssm_client.get_parameters(Names=parameter_names, WithDecryption=True)
    parameters = {param['Name']: param['Value'] for param in response['Parameters']}
    ODS_UID = parameters.get(dbuser)
    ODS_PWD = parameters.get(dbpassword)
    ODS_HOSTNAME = parameters.get(dbhostname)
    ODS_DATABASENAME = parameters.get(dbname)
    ODS_PORT = parameters.get(dbport)
    ODS_DRIVER = 'ibm_db'

    return ODS_DRIVER, ODS_UID, ODS_PWD, ODS_HOSTNAME, ODS_PORT, ODS_DATABASENAME


def db2_conn_test():

    #Getting db credentials to connect db2
    #DEV_ODS_UID,DEV_ODS_PWD,DEV_HOSTNAME,DEV_DATABASENAME,DEV_PORT_NUM=get_parameters()
    ODS_DRIVER, ODS_UID, ODS_PWD, ODS_HOSTNAME, ODS_PORT, ODS_DATABASENAME=get_db2_parameters()

    try:

        #conndb2 = ibm_db.connect(f'DATABASE={DEV_DATABASENAME};HOSTNAME={DEV_HOSTNAME};PORT={DEV_PORT_NUM};PROTOCOL=TCPIP;UID={DEV_ODS_UID};PWD={DEV_ODS_PWD};',"", "")
        conndb2 = ibm_db.connect(f'DATABASE={ODS_DATABASENAME};HOSTNAME={ODS_HOSTNAME};PORT={ODS_PORT};PROTOCOL=TCPIP;UID={ODS_UID};PWD={ODS_PWD};',"", "")

        '''Dont ever use this connection anything in devlopment because its an QA environment
        use only DEV for insert anything to table'''
        #conndb2 = ibm_db.connect(    f'DATABASE={QA_DATABASENAME};HOSTNAME={QA_HOSTNAME};PORT={QA_PORT_NUM};PROTOCOL=TCPIP;UID={QA_ODS_UID};PWD={QA_ODS_PWD};',"", "")

        Conn = db2.Connection(conndb2)
        print("db2 connected successfully")


    except Exception as e:
        # handle the connection exception
        print(f"An error occurred while connecting to the database: {e}" )

    return Conn

# db2_conn_test()

def s3_get_object():
    bucket_name = str(environment)+"-gwf-investments-filexfer-us-east-1"
    file_key = "tpifx/incoming/"
    return bucket_name, file_key, s3

def s3_put_incoming_object():
    bucket_name = str(environment)+"-gwf-investments-filexfer-us-east-1"
    file_key = "tpifx/incoming/"
    return bucket_name, file_key, s3


def s3_put_object():
    bucket_name = str(environment)+"-gwf-investments-filexfer-us-east-1"
    file_key = "tpifx/outgoing/"
    return bucket_name, file_key, s3

def s3_Dataset_get_object():
    bucket_name = str(environment)+"-gwf-investments-filexfer-us-east-1"
    file_key = "tpifx/stage/plan_dataset/"
    return bucket_name, file_key, s3

def s3_Dataset_put_object():
    bucket_name = str(environment)+"-gwf-investments-filexfer-us-east-1"
    file_key = "tpifx/stage/plan_dataset/"
    return bucket_name, file_key, s3

def s3_intermediate_files_put_object():
    bucket_name = str(environment)+"-gwf-investments-filexfer-us-east-1"
    file_key = "tpifx/stage/plan_intermediate/"
    return bucket_name, file_key, s3

def s3_intermediate_files_get_object():
    bucket_name = str(environment)+"-gwf-investments-filexfer-us-east-1"
    file_key = "tpifx/stage/plan_intermediate/"
    return bucket_name, file_key, s3

def s3_seq_done_object():
    bucket_name = str(environment)+"-gwf-investments-filexfer-us-east-1"
    file_key = "tpifx/stage/plan_module_done/"
    return bucket_name, file_key, s3

