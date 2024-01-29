import os
from sqlalchemy import create_engine
import pandas as pd
import argparse
import time


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    url = params.url
    table_name = params.table_name
    
    # download csv
    csv_name = 'output.csv'
    os.system(f"wget {url} -O {csv_name}")
    
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df_iter = pd.read_csv(
        csv_name,
        iterator=True,
        chunksize=100_000,
        compression="gzip"
    )

    df = next(df_iter)
    df["tpep_pickup_datetime"] = pd.to_datetime(df.tpep_pickup_datetime)
    df["tpep_dropoff_datetime"] = pd.to_datetime(df.tpep_dropoff_datetime)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
    df.to_sql(name=table_name, con=engine, if_exists="append")

    i = 0
    while True:
        print(f"Ingesting chunk {i}...")
        s = time.time()
        df = next(df_iter)
        df["tpep_pickup_datetime"] = pd.to_datetime(df.tpep_pickup_datetime)
        df["tpep_dropoff_datetime"] = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table_name, con=engine, if_exists="append")
        e = time.time()
        print(f"Completed ingesting chunk {i} in {str(e - s)} seconds.")
        i += 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    parser.add_argument("--user", help="user for postgres")
    parser.add_argument("--password", help="password for postgres")
    parser.add_argument("--host", help="host for postgres")
    parser.add_argument("--port", help="port for postgres")
    parser.add_argument("--db", help="database name for postgres")
    parser.add_argument("--table_name", help="name of the table where we will write the results to.")
    parser.add_argument("--url", help="url of the csv file")

    args = parser.parse_args()
    main(args)