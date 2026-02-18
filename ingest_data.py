#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

# Define dtypes to optimize memory
DTYPES = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

PARSE_DATES = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--year', default=2021, help='Year of the trip data (e.g., 2021)')
@click.option('--month', default=1, help='Month of the trip data (e.g., 1)')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, year, month):
    
    # ---------------------------------------------------------
    # 1. Construct URL (Logic from your screenshot)
    # ---------------------------------------------------------
    
    # The prefix for the GitHub URL
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    
    # Dynamic filename using f-string with padding (e.g., 1 -> 01)
    csv_name = f'yellow_tripdata_{year}-{month:02d}.csv.gz'
    
    # Combine them to get the full URL
    url = f'{prefix}/{csv_name}'

    print(f"[INFO] Processing: {csv_name}")
    print(f"[INFO] Source URL: {url}")

    # ---------------------------------------------------------
    # 2. Handle File Download
    # ---------------------------------------------------------
    
    if os.path.exists(csv_name):
        print(f"[INFO] File '{csv_name}' already exists locally. Skipping download.")
    else:
        print(f"[INFO] Downloading file...")
        # Use wget to download
        exit_code = os.system(f"wget {url} -O {csv_name}")
        if exit_code != 0:
            print(f"[ERROR] Download failed. Check internet or if file exists for {year}-{month:02d}.")
            return

    # ---------------------------------------------------------
    # 3. Database Connection
    # ---------------------------------------------------------
    
    engine_url = f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    engine = create_engine(engine_url)
    print(f"[INFO] Connecting to DB: {pg_db}")

    # ---------------------------------------------------------
    # 4. Ingest Data
    # ---------------------------------------------------------

    try:
        df_iter = pd.read_csv(
            csv_name,
            dtype=DTYPES,
            parse_dates=PARSE_DATES,
            iterator=True,
            chunksize=1000,
            compression='gzip'
        )
    except Exception as e:
        print(f"[ERROR] Error reading CSV: {e}")
        return

    # Insert first chunk
    try:
        first_chunk = next(df_iter)
        first_chunk.head(0).to_sql(name=target_table, con=engine, if_exists="replace", index=False)
        first_chunk.to_sql(name=target_table, con=engine, if_exists="append", index=False, method='multi')
        print(f"[INFO] First chunk inserted.")
    except StopIteration:
        print("[ERROR] File is empty.")
        return

    # Insert remaining chunks
    for df_chunk in tqdm(df_iter, desc="Ingesting data"):
        df_chunk.to_sql(name=target_table, con=engine, if_exists="append", index=False, method='multi')

    print(f"[SUCCESS] Finished ingesting {csv_name} into {target_table}.")

if __name__ == "__main__":
    run()