#-*- coding: utf-8 -*-
'''
$results = Get-DnsServerZone -computername 서버명 | % {
    $zone = $_.zonename
    Get-DnsServerResourceRecord -computername 서버명 $zone | select @{n='ZoneName';e={$zone}}, HostName, RecordType, @{n='RecordData';e={if ($_.RecordData.IPv4Address.IPAddressToString) {$_.RecordData.IPv4Address.IPAddressToString} else {$_.RecordData.NameServer.ToUpper()}}}
}

$results | Export-Csv -NoTypeInformation c:\temp\DNSRecords.csv -Append
'''
from __future__ import print_function
import argparse
import pymysql
import pandas as pd
import os
import sys
import traceback
from glob import glob

def create_table(connection, table_name):
    try:
        with connection.cursor() as cursor:
            sql='''
            create table IF NOT exists {}(
                ZoneName text,
                HostName tinytext,
                RecordType varchar(15),
                RecordData tinytext
            )
            '''.format(table_name)
            print(sql)
            cursor.execute(sql)
    except Exception as e:
        print(table_name)
        traceback.print_exc()
        raise e


def truncate_table(connection, table_name):
    print("[start truncate table {}]".format(table_name))
    try:
        with connection.cursor() as cursor:
            sql='truncate table {}'.format(table_name)
            print(sql)
            cursor.execute(sql)
    except Exception as e:
        traceback.print_exc()
        raise e
    print("[END]".format(table_name))

def insert_records(connection, table_name, records):
    print("[Start Update Domain Records]")
    try:
        record_cnt = 0
        value_list = []
        record_len = len(records)
        print('total record count : {}'.format(record_len))
        while records:
            record_cnt += 1
            temp = records.pop()
            value_list.append(temp)
            if record_cnt%100==0 or record_len==record_cnt:
                insert_values(connection, table_name, value_list)
                value_list=[]
    except Exception as e:
        traceback.print_exc()
        raise e
    print("[END]")

def insert_values(connection, table_name, rows):
    try:
        with connection.cursor() as cursor:
            sql='''
            INSERT
            INTO {}
            VALUES (%s, %s, %s, %s)
            '''.format(table_name)
            cursor.executemany(sql, rows)
    except Exception as e:
        traceback.print_exc()
        raise e


def copy_table(connection, destination_table_name, source_table_name):
    print("[Start Backup {} to {}]".format(source_table_name, destination_table_name))
    try:
        with connection.cursor() as cursor:
            sql='''
            INSERT
            INTO {}
            SELECT * FROM {}
            '''.format(destination_table_name, source_table_name)
            print(sql)
            cursor.execute(sql)
    except Exception as e:
        traceback.print_exc()
        raise e
    print("[END]")

def get_merged_csv(flist, **kwargs):
    return pd.concat([pd.read_csv(f, **kwargs) for f in flist], ignore_index=True)

def main(args):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--csvpath', required=False, help='Folder where DNS record CSV files are located.', default=os.path.join(sys.path[0],'csv'))
    parser.add_argument('--dbhost', required=False, help='Database host', default="localhost")
    parser.add_argument('--dbport', required=False, help='Database port', default="3306")
    parser.add_argument('--dbuser', required=False, help='Database user', default="root")
    parser.add_argument('--dbpw', required=False, help='Database password', default="123qwer@")
    parser.add_argument('--db', required=False, help='DB database name', default="dns")
    parser.add_argument('--tablename', required=False, help='Table name of Domain Record Info.', default="domain_info")
    args = parser.parse_args(args)

    print("*******************************")
    print("****       Arguments       ****")
    print("*******************************")
    print("Database Host : {}".format(args.dbhost))
    print("Database Port : {}".format(args.dbport))
    print("Database User : {}".format(args.dbuser))
    print("Database PW : {}".format(args.dbpw))
    print("Database DB : {}".format(args.db))
    print("Table name : {}".format(args.tablename))
    print("*******************************")
    print("*******************************")
    print()
    print("*******************************")
    print("****         Init          ****")
    print("*******************************")
    fmask=os.path.join(args.csvpath, '*.csv')
    csv_files = glob(fmask)
    table_name = args.tablename
    bak_table_name = '{}_bak'.format(table_name)
    if len(csv_files)==0:
        print("[Error] There is no csv file in {}".format(fmask))
        return
    try:
        connection = pymysql.connect(host=args.dbhost,
                                    port= int(args.dbport),
                                    user=args.dbuser,
                                    password=args.dbpw,
                                    db=args.db,
                                    cursorclass=pymysql.cursors.DictCursor)
    except:
        print("[Error] Database Connection")
        traceback.print_exc()
        return
    try:
        create_table(connection, table_name)
        create_table(connection, bak_table_name)
    except:
        traceback.print_exc()
        connection.close()
        return

    record_list = get_merged_csv(csv_files, keep_default_na=False)
    records = record_list.values.tolist()
    print("*******************************")
    print("*******************************")
    print()


    print("*******************************")
    print("****     Start Update      ****")
    print("*******************************")
    try:
        truncate_table(connection, bak_table_name)
        copy_table(connection, bak_table_name, table_name)
        truncate_table(connection, table_name)
        insert_records(connection, table_name, records)
        connection.commit()
    except:
        traceback.print_exc()
    finally:
        connection.close()
    print("*******************************")
    print("****          END          ****")
    print("*******************************")
if __name__ == '__main__':
    main(sys.argv[1:])
