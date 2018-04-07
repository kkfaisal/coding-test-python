"""
Answer for Question 2.
This script will load a csv file from local to s3 and then load that file to Redhift table.
If table not present it will be created.
Unit tests are included.
"""

import s3_upload
import logging
import os
import psycopg2



def redshift_connect():
    """
    Connect to Redshift Cluster
    """
    try:
        v_host=os.environ['RD_HOST']
        v_db = os.environ['RD_DB']
        v_usr = os.environ['RD_USR']
        v_pwd = os.environ['RD_PWD']
    except:
        logging.error("error when trying to access env variable")
        raise ValueError

    return psycopg2.connect(dbname= v_db, host=v_host,port= '5439', user= v_usr, password= v_pwd)

def copy_file_to_redshift(s3_bucket,s3_key,rd_schema,rd_table_name):
    """
    Create table and copy data to it from s3 file.
    :return:
    """
    try:
        acc_id=os.environ['RD_ACC_ID']
        redshift_role=os.environ['RD_ROLE']
    except:
        logging.error("error when trying to access env variable")
        raise ValueError


    create_schema_sql = """create schema if not exists {schema}""".format(schema=rd_schema)
    create_table_sql = """create table if not exists {schema}.{tbl_name}(
                            id INT4,
                            name varchar(50),
                            dept_id INT2,
                            dob date)""".format(schema=rd_schema,tbl_name=rd_table_name)
    copy_data_sql  = """copy {schema}.{tbl_name} from 's3://{bkt}/{key}'
                        CSV
                        IGNOREHEADER 1
                        credentials 'aws_iam_role=arn:aws:iam::{rd_accnt_id}:role/{rd_role}'""".format(schema=rd_schema,tbl_name=rd_table_name,
                                                                                                       bkt=s3_bucket,key=s3_key,
                                                                                                       rd_accnt_id=acc_id,rd_role=redshift_role)

    # 1. Upload file to S3.
    s3_upload.upload_file_to_s3('redshift_table_data.csv',s3_bucket,s3_key)

    #Connect to Redshift
    rd_con = redshift_connect()
    cur = rd_con.cursor()

    # 2. Create schema and table if needed and load data.
    try:
        cur.execute(create_schema_sql)
        cur.execute(create_table_sql)
        cur.execute(copy_data_sql)
        print(copy_data_sql)

    except Exception:
        logging.exception(Exception)
        rd_con.rollback()
    else:
        rd_con.commit()
    finally:
        cur.close()
        rd_con.close()


if __name__ == '__main__':
    redshift_schema_name="test1"
    redshift_table_name="users1"
    s3_bkt="faisalkk-data-test"
    s3_key="test/data.csv"
    copy_file_to_redshift(s3_bkt,s3_key, redshift_schema_name, redshift_table_name)