# Windows Domain Server Domain Record

## Domain Record csv Files
powsershell 원격으로 Domain Server 의 레코드를 csv 파일 형식으로 생성

```
$results = Get-DnsServerZone -computername 서버명 | % {
    $zone = $_.zonename
    Get-DnsServerResourceRecord -computername 서버명 $zone | select @{n='ZoneName';e={$zone}}, HostName, RecordType, @{n='RecordData';e={if ($_.RecordData.IPv4Address.IPAddressToString) {$_.RecordData.IPv4Address.IPAddressToString} else {$_.RecordData.NameServer.ToUpper()}}}
}

$results | Export-Csv -NoTypeInformation c:\temp\DNSRecords.csv -Append
```

## AWS Athena
- AWS S3 Bucket
    - Upload DNSRecords.csv
- Athena DDL
```
CREATE EXTERNAL TABLE IF NOT EXISTS {TABLE_NAME} (
  zonename string,
  hostname string,
  record_type string,
  record_data string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar" = "\"",
  "escapeChar" = "\\"
)
LOCATION 's3://{BUCKET_NAME}/{FOLDER_PATH}/'
TBLPROPERTIES ("skip.header.line.count"="1");
```
- Select Example
```
select
  concat(hostname,'.',zonename) as full_domain,
  hostname,
  zonename,
  record_type,
  record_data
from
  {DATABASE}.{TABLE}
where
  record_data in (
	'{IP 1}',
	'{IP 2}',
	'{IP N}'
  )
```
- Using athena.py
    - pip install boto3
    - line 63 ~ 70
```
client_args['region_name']='ap-northeast-2'
client_args['aws_access_key_id']='{ACCESS KEY}'
client_args['aws_secret_access_key']='{SECRET_KEY}'
client_args['aws_session_token']='{TOKEN}'
DATABASE='{DATABASE}'
TABLE='{TABLE}'
ATHENA_S3='{QUERY_SAVE_BUCKET}'
ATHENA_S3_PREFIX= '{PREFIX}'
```
    - python athena.py {ip 1},{ip 2},...,{ip N}



## MariaDB(using maria_dns.py)
### Prerequisites
- install python
- pip install pymysql pandas
- Install mariaDB
- create Database
- create User


### Arguments
- csvpath : Folder where DNS record CSV files are located.(default= ./csv)
- dbhost : Database host
- dbport : Database port (default= 3306)
- dbuser : Database user
- dbpw : Database password
- db : mariaDB database name (default= dns)
- tablename : Table name of Domain Record Info. (default= domain_info)
