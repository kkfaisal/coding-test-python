import unittest
import redshift_load
import logging

# Note : Schema for testing should be created manually in Redshift before running the test.
# TODO : Change these variables to read dynamically as args.
RD_SCHEMA='unit_test'
RD_TABLE='users'
S3_BUCKET="faisalkk-data-test"
S3_KEY="unittest/redshift/data"



def drop_table(schema,table):
    drop_sql = """ DROP TABLE IF EXISTS {s}.{t}""".format(s=schema,t=table)
    con = redshift_load.redshift_connect()
    cur=con.cursor()
    print(drop_sql)
    try:
        cur.execute(drop_sql)
    except Exception:
        raise Exception
    else:
        con.commit()
    finally:
        cur.close()
        con.close()

def get_count_from_table(schema,table):
    count_sql = """ SELECT COUNT(*) FROM {s}.{t}""".format(s=schema,t=table)
    con = redshift_load.redshift_connect()
    cur=con.cursor()
    try:
        cur.execute(count_sql)
        res = cur.fetchone()[0]
    except Exception:
        raise Exception
    finally:
        cur.close()
        con.close()
    return res

class TestS3Upload(unittest.TestCase):
    def test_data_load(self):
        drop_table(RD_SCHEMA,RD_TABLE)
        redshift_load.copy_file_to_redshift(S3_BUCKET,S3_KEY,RD_SCHEMA,RD_TABLE)
        self.assertEqual(get_count_from_table(RD_SCHEMA,RD_TABLE),9)



if __name__ == '__main__':
    unittest.main()
