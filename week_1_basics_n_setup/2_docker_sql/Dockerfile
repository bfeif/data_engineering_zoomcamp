FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_yellow_taxi_data_to_postgres.py ingest_yellow_taxi_data_to_postgres.py

ENTRYPOINT [ "python", "ingest_yellow_taxi_data_to_postgres.py" ]
