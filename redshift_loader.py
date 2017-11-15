from __future__ import print_function
import boto3
import json
import logging, inspect
import datetime, time
import psycopg2
import re
from logilab.common.optik_ext import level_options


'''
TODO: this should run on a schedule so that it can check to see how many files are in the Bucket
      if there is more than one file at the time of trigger, then load multiple files by creating 
      a manifest. SEe copy command docs for details.


TODO: we need a directory for each schema.  We will pull the schema from the key. We also need a file
      that we can parse that has the sql in it.
'''

# for local testing set profile
#boto3.setup_default_session(profile_name='nelsone')


current_session = boto3.session.Session()
current_region = current_session.region_name

s3 = boto3.client("s3")
redshift_DBNAME = "INSERT_NAME_HERE"
redshift_User = "INSERT_USER_HERE"
#encyprt this thing!
# not yet, it's only a demo/poc.
# do not use for production until encrypted
redshift_Pass="INSERT_PASSWD_HERE"
redshift_Port="INSERT_PORT_HERE"
redshift_EndPoint="INSERT_REDSHIFT_ENDPOINT_HERE"
redshift_Role="INSERT_REDSHIFT_ROLE_HERE"


'''
This function is used for logging
To use it, in your function/catch, pass in your Exception and the logging level_options

Logging levels are:
CRITICAL
ERROR
WARNING
DEBUG
INFO
NOTSET

'''

def log(e, logging_level):
    func_name=inspect.currentframe().f_back.f_code
    logging_level = logging_level.upper()
    print(logging_level+":"+func_name.co_name+":"+str(e))



''' Used to evaluate if any returned structures are empty '''
def is_empty(any_structure):
    if any_structure:
        return False
    else:
        return True

def returnSchemaName(s3_object_key):
    return s3_object_key.split('/')[-2]


def check_schema_exists(schema_name, cur):
    print("Checking to see if schema " + schema_name + " exists. If not, will create it.")
    sql = "create schema if not exists " + schema_name + ";"
    cur.execute(sql)
    #need to add some checks in here so it returns if success or Failure


def getCreateTableSQL(s3_source_bucket, s3_object_key, schema_name, csvFileNameNoExtension):
    #fetch the schema definitoion file from s3
    # eventually I want to do this from dynamo
    
    defFileKey = s3_object_key.split(schema_name)[0]
    defFileKey = defFileKey + schema_name + "/" + schema_name + ".def"
    print("Getting " + defFileKey + " from s3.")
    definitionsFile = s3.get_object(Bucket=s3_source_bucket, Key=defFileKey)
    table_sql = definitionsFile['Body'].read()
    sql=table_sql.split("\n")
    print("Created a list with " + str(len(sql)) + " items.")
    for item in sql:
        print("looking for " + csvFileNameNoExtension + " in " + item)
        #This needs to be setup so that it matches on the EXACT string
        if csvFileNameNoExtension in item:
            print("definition for table: " + item)
            table_sql = item
    print("This is the sql for the table: " + table_sql)
    return table_sql.split("::")[1]



''' this is the main handler for the lambda function'''
def lambda_handler(event, context):
    #Make Attempts to carch and log exceptions
    try:
        s3_source_bucket = event['Records'][0]['s3']['bucket']['name']
        s3_object_key = event['Records'][0]['s3']['object']['key']
        
        print("I've found " + s3_source_bucket + " and " + s3_object_key)
        
        conn = psycopg2.connect("dbname=" + redshift_DBNAME + " user=" + redshift_User + " password=" + redshift_Pass + " port=" + redshift_Port + " host=" + redshift_EndPoint, sslmode='require')
        conn.autocommit = True
        cur = conn.cursor()
        
        csvFileName=s3_object_key.split("/")[-1]
        csvFileNameNoExtension=csvFileName.split(".")[0]
        
        #Get and verify schema
        schema_name = returnSchemaName(s3_object_key)
        print("working with schema: " + schema_name)
        check_schema_exists(schema_name, cur)
        
        sql = getCreateTableSQL(s3_source_bucket, s3_object_key, schema_name, csvFileNameNoExtension)
        cur.execute(sql)
        
        print("I should be working with the file " + csvFileName + " and without an extension is " + csvFileNameNoExtension)
        
        sql = "copy " + schema_name + "." + csvFileNameNoExtension + " from 's3://" + s3_source_bucket + "/" + s3_object_key + "' iamrole '" + redshift_Role + "' IGNOREHEADER 1 REMOVEQUOTES;"
        print("this is the sql: " + sql)
        
        cur.execute(sql)
        cur.close()
        conn.close()
    
    except Exception as e:
        log(e, 'warning')
    
    