import boto3
import traceback
import time
import sys
from datetime import datetime
import os
from pprint import pprint
'''
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:GetTable"
            ],
            "Resource": "arn:aws:glue:*:*:catalog"
        },
        {
            "Sid": "VisualEditor1",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucketMultipartUploads",
                "s3:ListBucket",
                "s3:GetBucketLocation",
                "s3:ListMultipartUploadParts",
                "s3:AbortMultipartUpload",
                "athena:StartQueryExecution",
                "athena:GetQueryResults",
                "athena:GetQueryExecution",
                "glue:GetDatabase",
                "glue:GetTable"
            ],
            "Resource": [
                "arn:aws:s3:::{dns-bucket}",
                "arn:aws:s3:::{dns-bucket}/*",
                "arn:aws:s3:::{query-result-bucket}",
                "arn:aws:s3:::{query-result-bucket}/*",
                "arn:aws:athena:*:*:workgroup/*",
                "arn:aws:glue:{region}:{account_num}:database/{dns-database}",
                "arn:aws:glue:{region}:{account_num}:table/{dns-database}/{dns-table pattern ex dns-*}",
                "arn:aws:glue:{region}:{account_num}:catalog/*"
            ]
        },
        {
            "Sid": "VisualEditor2",
            "Effect": "Allow",
            "Action": "s3:ListAllMyBuckets",
            "Resource": "*"
        }
    ]
}
'''

MAX_ERR_REPITITIONS = 5

client_args={}
client_args['service_name']='athena'
client_args['region_name']='ap-northeast-2'
client_args['aws_access_key_id']='{ACCESS KEY}'
client_args['aws_secret_access_key']='{SECRET_KEY}'
client_args['aws_session_token']='{TOKEN}'
DATABASE='{DATABASE}'
TABLE='{TABLE}'
ATHENA_S3='{QUERY_SAVE_BUCKET}'
ATHENA_S3_PREFIX= '{PREFIX}'

athena_client = boto3.client(**client_args)

def query(ips):
    ips = ','.join(ips)
    ips.replace(' ','')
    ip_list = []
    for ip in ips.split(','):
        if ip:
            ip_list.append("'{}'".format(ip))
    sql='''
    select
        concat(hostname,'.',zonename) as domain,
        record_type,
        record_data,
        'INTERNAL' as type
    from
        {}
    record_data in (
        {}
    )
    order by record_data
    '''

    sql = sql.format(TABLE, ','.join(ip_list))
    print(sql)
    sql_response=athena_client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': 's3://{}{}'.format(ATHENA_S3, ATHENA_S3_PREFIX)
        }
    )
    execution_id=sql_response['QueryExecutionId']
    fail_cnt = 1
    get_cnt = 0
    is_success_query = False
    while True:
        try:
            time.sleep(5)
            get_cnt+=1
            response = athena_client.get_query_execution(
                QueryExecutionId=execution_id
            )
            '''
            {
                'QueryExecution': {
                    'QueryExecutionId': 'string',
                    'Query': 'string',
                    'StatementType': 'DDL'|'DML'|'UTILITY',
                    'ResultConfiguration': {
                        'OutputLocation': 'string',
                        'EncryptionConfiguration': {
                            'EncryptionOption': 'SSE_S3'|'SSE_KMS'|'CSE_KMS',
                            'KmsKey': 'string'
                        }
                    },
                    'QueryExecutionContext': {
                        'Database': 'string',
                        'Catalog': 'string'
                    },
                    'Status': {
                        'State': 'QUEUED'|'RUNNING'|'SUCCEEDED'|'FAILED'|'CANCELLED',
                        'StateChangeReason': 'string',
                        'SubmissionDateTime': datetime(2015, 1, 1),
                        'CompletionDateTime': datetime(2015, 1, 1)
                    },
                    'Statistics': {
                        'EngineExecutionTimeInMillis': 123,
                        'DataScannedInBytes': 123,
                        'DataManifestLocation': 'string',
                        'TotalExecutionTimeInMillis': 123,
                        'QueryQueueTimeInMillis': 123,
                        'QueryPlanningTimeInMillis': 123,
                        'ServiceProcessingTimeInMillis': 123
                    },
                    'WorkGroup': 'string'
                }
            }
            '''
            #'QUEUED'|'RUNNING'|'SUCCEEDED'|'FAILED'|'CANCELLED'
            query_status = response['QueryExecution']['Status']['State']
            query_status = str(query_status)
            print('status {}: {}'.format(get_cnt, query_status))
            if query_status == 'SUCCEEDED':
                is_success_query=True
                break
            if query_status in ['FAILED','CANCELLED']:
                break
        except:
            traceback.print_exc()
            if not is_success_query:
                fail_cnt+=1
                if fail_cnt > MAX_ERR_REPITITIONS:
                    break
    if is_success_query:
        return execution_id

def get(id):
    response = athena_client.get_query_results(
        QueryExecutionId=id
    )
    rows = []
    for r in response['ResultSet']['Rows']:
        r_strs = []
        for r_i in r['Data']:
            r_strs.append(r_i[list(r_i.keys())[0]])
        rows.append(','.join(r_strs))
    #f = open('1.csv','w',encoding="UTF8")
    resultFolder = os.path.join(sys.path[0], datetime.now().strftime("%Y-%m-%d-%H%M%S"))
    if not os.path.exists(resultFolder):
        os.makedirs(resultFolder)
    output_path = os.path.join(resultFolder, 'result.csv')
    f = open(output_path,'w')
    f.write('\n'.join(rows))
    f.close()

    print("output : {}".format(output_path))

if __name__ == '__main__':
    q_id = query(sys.argv[1:])
    if q_id:
        get(q_id)
