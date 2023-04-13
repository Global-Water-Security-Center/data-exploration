"""See `python scriptname.py --help"""
import argparse
import configparser
import csv
import datetime
import logging
import os
import sys

from sqlalchemy import create_engine
from sqlalchemy import Integer
from sqlalchemy import Text
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import and_
import boto3
import sqlalchemy

logging.basicConfig(
    level=logging.WARNING,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


DB_FILE = 'file_registry.sqlite'
DB_ENGINE = create_engine(f"sqlite:///{DB_FILE}", echo=False)
GLOBAL_INI_PATH = 'defaults.ini'


class Base(DeclarativeBase):
    pass


class File(Base):
    __tablename__ = "file_to_location"
    id_val = mapped_column(Integer, primary_key=True)
    dataset_id = mapped_column(Text, index=True)
    variable_id = mapped_column(Text, index=True)
    date_str = mapped_column(Text, index=True)
    file_path = mapped_column(Text, index=True)

    def __repr__(self) -> str:
        return (
            f'File(id_val={self.id_val!r}, '
            f'dataset_id={self.dataset_id!r}, '
            f'variable_id={self.variable_id!r}, '
            f'date_str={self.date_str!r}, '
            f'date_str={self.file_path!r}')


Base.metadata.create_all(DB_ENGINE)


def fetch_file(
        global_config, dataset_id, variable_id, date_str):
    """Fetch a file from remote data store to local target path.

    Args:
        global_config (configparser object): contains info about location
            of data buckets and local storage
        dataset_id (str): dataset defined by config
        variable_id (str): variable id that's consistent with dataset
        date_str (str): date to query that's consistent with the dataset

    Returns:
        (str) path to local downloaded file.
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

    date_format = global_config[f'{dataset_id}_date_format']
    formatted_date = datetime.datetime.strptime(date_str, date_format).strftime(
        date_format)

    with Session(DB_ENGINE) as session:
        stmt = sqlalchemy.select(File).where(and_(
            File.dataset_id == dataset_id,
            #File.variable_id == variable_id,
            File.date_str == formatted_date))
        result = session.execute(stmt).first()[0]

    if result is not None:
        return result.file_path

    filename = global_config[f'{dataset_id}_file_format'].format(
        variable=variable_id, date=formatted_date)
    target_path = os.path.join(global_config['cache_dir'], filename)
    if not os.path.exists(target_path):
        dataset_bucket = s3.Bucket(global_config[f'{dataset_id}_bucket_id'])
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        dataset_bucket.download_file(filename, target_path)
    else:
        LOGGER.warning(f'{target_path} exists but no entry in database')

    with Session(DB_ENGINE) as session:
        file_entry = File(
            dataset_id=dataset_id,
            variable_id=variable_id,
            date_str=formatted_date,
            file_path=target_path)
        session.add(file_entry)
        session.commit()
    return target_path

    # seems useful for testing database in parallel or working database
    # https://docs.sqlalchemy.org/en/20/orm/session_transaction.html#joining-a-session-into-an-external-transaction-such-as-for-test-suites

    # this thread local makes sense, global object but it creates a local variable automatically with a new thread
    # https://docs.sqlalchemy.org/en/20/orm/contextual.html#using-thread-local-scope-with-web-applications

    # with DB_ENGINE.connect() as conn:
    #     result = conn.execute(sqlalchemy.text("select 'hello world'"))
    #     print(result.all())


    # with DB_ENGINE.connect() as conn:
    #     conn.execute(sqlalchemy.text("CREATE TABLE some_table (x int, y int)"))
    #     conn.execute(
    #         sqlalchemy.text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
    #         [{"x": 1, "y": 1}, {"x": 2, "y": 4}],
    #     )
    #     conn.commit()

    # with DB_ENGINE.begin() as conn:
    #     conn.execute(
    #         sqlalchemy.text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
    #         [{"x": 6, "y": 8}, {"x": 9, "y": 10}],
    #     )

    # with DB_ENGINE.connect() as conn:
    #     result = conn.execute(sqlalchemy.text("SELECT x, y FROM some_table"))
    #     for row in result:
    #         print(f"x: {row.x}  y: {row.y}")

    # with DB_ENGINE.connect() as conn:
    #     conn.execute(
    #         sqlalchemy.text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
    #         [{"x": 11, "y": 12}, {"x": 13, "y": 14}],
    #     )
    #     conn.commit()

    # stmt = sqlalchemy.text("SELECT x, y FROM some_table WHERE y > :y ORDER BY x, y")
    # with Session(DB_ENGINE) as session:
    #     result = session.execute(stmt, {"y": 6})
    #     for row in result:
    #         print(f"x: {row.x}  y: {row.y}")

    # with Session(DB_ENGINE) as session:
    #     result = session.execute(
    #         sqlalchemy.text("UPDATE some_table SET y=:y WHERE x=:x"),
    #         [{"x": 9, "y": 11}, {"x": 13, "y": 15}],
    #     )
    #     session.commit()

    # continue here: https://docs.sqlalchemy.org/en/20/orm/session_state_management.html
    # use session objects... like
    # with Session(engine) as session:
    #     result = session.execute(select(User))
    # return filename
    # with Session(engine, autobegin=False) as session:
    #     session.begin()  # <-- required, else InvalidRequestError raised on next call

    #     session.add(User(name="u1"))
    #     session.commit()

    #     session.begin()  # <-- required, else InvalidRequestError raised on next call

    #     u1 = session.scalar(select(User).filter_by(name="u1"))


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
    print(f'downloaded to: {target_path}')


if __name__ == '__main__':
    main()
