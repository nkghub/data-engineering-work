#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    turl = params.trip_url
    zurl = params.zone_url

    print('************** Zone start ******************')
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    # for Zone table
    table_name = 'zones'
    csv_name   = 'zoneoutput.csv'
    os.system(f"wget {zurl} -O {csv_name}")
    df_iter = pd.read_csv(csv_name, iterator=True)
    print('************** Zone start Read ******************')
    df = next(df_iter)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')
    print('************** Zone done ******************')
    # for trips table
    table_name = 'yellow_taxi_trips'
    csv_name   = 'tripoutput.csv'

    os.system(f"wget {turl} -O {csv_name}")
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True: 
        t_start = time()
        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))
    print('************** trips done ******************')
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    #parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--trip_url', help='trip url of the csv file')
    parser.add_argument('--zone_url', help='zone url of the csv file')

    args = parser.parse_args()

    main(args)