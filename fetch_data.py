import argparse
import collections
import csv
import datetime
import configparser

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import sqlalchemy
import boto3


DB_ENGINE = create_engine("sqlite+pysqlite:///:memory:", echo=True)
GLOBAL_INI_PATH = 'defaults.ini'


def fetch_file(
        global_config, dataset_id, variable_id, date_str):
    """Fetch a file from remote data store to local target path.

    Args:
        global_config (configparser object):
        dataset_id (str):
        variable_id (str):
        date_str (str):
        target (str):

    """
    reader = csv.DictReader(
        open(global_config[f'{dataset_id}_access_key']))
    bucket_access_dict = next(reader)
    s3 = boto3.resource(
        's3',
        endpoint_url=global_config[f'{dataset_id}_base_uri'],
        aws_access_key_id=bucket_access_dict['Access Key Id'],
        aws_secret_access_key=bucket_access_dict['Secret Access Key'],
    )

    # Create connection to Wasabi / S3
    # Get bucket object
    my_bucket = s3.Bucket(global_config[f'{dataset_id}_bucket_id'])
    date_format = global_config[f'{dataset_id}_date_format']
    formatted_date = datetime.datetime.strptime(date_str, date_format).strftime(
        date_format)

    filename = global_config[f'{dataset_id}_file_format'].format(
        variable=variable_id, date=formatted_date)
    print(filename)

    with DB_ENGINE.connect() as conn:
        result = conn.execute(sqlalchemy.text("select 'hello world'"))
        print(result.all())


    with DB_ENGINE.connect() as conn:
        conn.execute(sqlalchemy.text("CREATE TABLE some_table (x int, y int)"))
        conn.execute(
            sqlalchemy.text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
            [{"x": 1, "y": 1}, {"x": 2, "y": 4}],
        )
        conn.commit()

    with DB_ENGINE.begin() as conn:
        conn.execute(
            sqlalchemy.text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
            [{"x": 6, "y": 8}, {"x": 9, "y": 10}],
        )

    with DB_ENGINE.connect() as conn:
        result = conn.execute(sqlalchemy.text("SELECT x, y FROM some_table"))
        for row in result:
            print(f"x: {row.x}  y: {row.y}")

    with DB_ENGINE.connect() as conn:
        conn.execute(
            sqlalchemy.text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
            [{"x": 11, "y": 12}, {"x": 13, "y": 14}],
        )
        conn.commit()

    stmt = sqlalchemy.text("SELECT x, y FROM some_table WHERE y > :y ORDER BY x, y")
    with Session(DB_ENGINE) as session:
        result = session.execute(stmt, {"y": 6})
        for row in result:
            print(f"x: {row.x}  y: {row.y}")

    with Session(DB_ENGINE) as session:
        result = session.execute(
            sqlalchemy.text("UPDATE some_table SET y=:y WHERE x=:x"),
            [{"x": 9, "y": 11}, {"x": 13, "y": 15}],
        )
        session.commit()

    # continue here: https://docs.sqlalchemy.org/en/20/orm/session_basics.html#id1
    return filename
    #my_bucket.download_file(filename, "test.tif")



def main():
    """Entry point."""
    global_config = configparser.ConfigParser(allow_no_value=True)
    global_config.read(GLOBAL_INI_PATH)
    global_config = global_config['defaults']
    available_commands = global_config['available_commands'].split(',')
    available_data = global_config['available_data'].split(',')
    parser = argparse.ArgumentParser(description='Data platform entry point')
    parser.add_argument('command', help=(
        'Command to execute, one of: ' + ', '.join(available_commands)))
    parser.add_argument('dataset_id', help=(
        'Dataset ID to operate on, one of: ' + ', '.join(available_data)))
    parser.add_argument('variable_id', help=(
        'Variable in that dataset, should be known to the caller.'))
    parser.add_argument('date', help='Date in the form of YYYY-MM-DD')
    args = parser.parse_args()

    target_path = fetch_file(
        global_config, args.dataset_id, args.variable_id, args.date)



if __name__ == '__main__':
    main()
