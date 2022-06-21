import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import * 
from pyspark.sql.functions import *
import boto3
import base64


from awsglue.dynamicframe import DynamicFrame
import hashlib

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

finding_macie_database="PREFIX"
finding_macie_tables="PREFIX_glue_AWS_REGION_ACCOUNT_ID"
bucket_athena="s3://dcp-athena-AWS_REGION-ACCOUNT_ID".replace("_", "-")


#inputs to KMS
key_id = "alias/encryptionDataRow"
region_name = "us_east_1".replace("_", "-")


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

macie_findings = glueContext.create_dynamic_frame.from_catalog(database = finding_macie_database, table_name = finding_macie_tables)

macie_findings_df = macie_findings.toDF()

macie_findings.printSchema()

columns_to_be_masked_and_encrypted = []

tables_list = []


def get_tables_to_be_masked_and_encrypted(df):
    df = df.select('key').collect()
    try:
        for i in df:
            e = i['key'].split("/")[1] 
            if e not in tables_list:
                tables_list.append(e)

    except:
        print ("DEBUG:",sys.exc_info())
    

    return tables_list

def get_columns_to_be_masked_and_encrypted(df):
    df = df.select('jsonPath').collect()
    try:
        for i in df:
            columns_to_be_masked_and_encrypted.append(i['jsonPath'].split(".")[1])    
    except:
        print ("DEBUG:",sys.exc_info())
    

    return columns_to_be_masked_and_encrypted

if 'detail' in macie_findings_df.columns:
    #Working with json in Spark
    try:
        #sensitiveData
        #detail.classificationDetails.result.sensitiveData.detections
        macie_finding_sensitiveData = macie_findings_df.select("detail.resourcesAffected.s3Object.key", "detail.classificationDetails.result.sensitiveData.detections").select("key", explode("detections").alias("new_detections")).select("key","new_detections.occurrences.records").select("key", explode("records").alias("new_records")).select("key","new_records.jsonPath").select("key", explode("jsonPath").alias("jsonPath")).drop_duplicates()


    #     macie_finding_sensitiveData.printSchema();
    #     macie_finding_sensitiveData.head(20)

        get_tables_to_be_masked_and_encrypted(macie_finding_sensitiveData);
        get_columns_to_be_masked_and_encrypted(macie_finding_sensitiveData);
    except:
        print ("DEBUG:",sys.exc_info())

if 'detail' in macie_findings_df.columns:

    try:    
        #customDataIdentifiers
        macie_finding_custome_data_identifiers = macie_findings_df.select("detail.resourcesAffected.s3Object.key", "detail.classificationDetails.result.customDataIdentifiers.detections").select("key", explode("detections").alias("new_detections")).select("key","new_detections.occurrences.records").select("key", explode("records").alias("new_records")).select("key","new_records.jsonPath").select("key", "jsonPath").drop_duplicates()
        
        #macie_findings_custome_data_identifiers_df_7.printSchema();
        #macie_findings_custome_data_identifiers_df_7.head(20)
        
        get_tables_to_be_masked_and_encrypted(macie_finding_custome_data_identifiers);
        get_columns_to_be_masked_and_encrypted(macie_finding_custome_data_identifiers);
    except:
        print ("DEBUG:",sys.exc_info())
    

def masked_rows(r):
    try:
        for entity in columns_to_be_masked_and_encrypted:
            if entity in table_columns:
                r[entity + '_masked'] = "##################"
                del r[entity]          
    except:
        print ("DEBUG:",sys.exc_info())

    return r

def get_kms_encryption(row):
    # Create a KMS client
    session = boto3.session.Session()
    client = session.client(service_name='kms',region_name=region_name)
    
    try:
        encryption_result = client.encrypt(KeyId=key_id, Plaintext=row)
        blob = encryption_result['CiphertextBlob']
        encrypted_row = base64.b64encode(blob)        
        return encrypted_row
        
    except:
        return 'Error on get_kms_encryption function'


def encrypt_rows(r):
    encrypted_entities = columns_to_be_masked_and_encrypted
    try:
        for entity in encrypted_entities:
            if entity in table_columns:
                encrypted_entity = get_kms_encryption(r[entity])
                r[entity + '_encrypted'] = encrypted_entity.decode("utf-8")
                del r[entity]
    except:
        print ("DEBUG:",sys.exc_info())
    return r


for table in tables_list:
    dataset_table_to_mask_and_encrypt = glueContext.create_dynamic_frame.from_catalog(database = "dataset", table_name = table)

    # Get columns names
    table_columns = dataset_table_to_mask_and_encrypt.toDF().columns

    # Apply mask to the identified fields
    df_masked_completed = Map.apply(frame = dataset_table_to_mask_and_encrypt, f = masked_rows)
    masked_path = bucket_athena +  "/masked/" + table
    # output to s3 in parquet format
    data_masked = glueContext.write_dynamic_frame.from_options(frame = df_masked_completed, connection_type = "s3", connection_options = {"path": masked_path}, format = "parquet", transformation_ctx = "datasink5")
    
    
    # Apply encryption to the identified fields
    df_encrypted_completed = Map.apply(frame = dataset_table_to_mask_and_encrypt, f = encrypt_rows)
    encrypted_path = bucket_athena + "/encrypted/"  + table   
     # output to s3 in parquet format
    data_encrypted = glueContext.write_dynamic_frame.from_options(frame = df_encrypted_completed, connection_type = "s3", connection_options = {"path": encrypted_path}, format = "parquet", transformation_ctx = "datasink5")


job.commit()




