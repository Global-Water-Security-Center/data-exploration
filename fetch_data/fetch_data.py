"""See `python scriptname.py --help"""
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

DB_FILE = os.path.join(os.path.dirname(__file__), 'file_registry.sqlite')
DB_ENGINE = create_engine(f"sqlite:///{DB_FILE}", echo=False)

GLOBAL_INI_PATH = os.path.join(os.path.dirname(__file__), 'defaults.ini')


# Need this because we can't subclass it directly
class Base(DeclarativeBase):
    pass


# Table to store local file locations
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


# create the table if it doesn't exist
Base.metadata.create_all(DB_ENGINE)


def fetch_file(dataset_id, variable_id, date_str):
    """Fetch a file from remote data store to local target path.

    Args:
        dataset_id (str): dataset defined by config
        variable_id (str): variable id that's consistent with dataset
        date_str (str): date to query that's consistent with the dataset

    Returns:
        (str) path to local downloaded file.
    """
    global_config = configparser.ConfigParser(allow_no_value=True)
    global_config.read(GLOBAL_INI_PATH)
    global_config = global_config['defaults']
    access_key_path = os.path.join(
        os.path.dirname(__file__),
        global_config[f'{dataset_id}_access_key'])
    if not os.path.exists(access_key_path):
        raise ValueError(
            f'expected a keyfile to access the S3 bucket at {access_key_path} '
            'but not found')
    reader = csv.DictReader(open(access_key_path))
    bucket_access_dict = next(reader)
    s3 = boto3.resource(
        's3',
        endpoint_url=global_config[f'{dataset_id}_base_uri'],
        aws_access_key_id=bucket_access_dict['Access Key Id'],
        aws_secret_access_key=bucket_access_dict['Secret Access Key'],
    )

    date_format = global_config[f'{dataset_id}_date_format']
    formatted_date = datetime.datetime.strptime(
        date_str, date_format).strftime(date_format)

    with Session(DB_ENGINE) as session:
        stmt = sqlalchemy.select(File).where(and_(
            File.dataset_id == dataset_id,
            File.variable_id == variable_id,
            File.date_str == formatted_date))
        result = session.execute(stmt).first()

    if result is not None:
        LOGGER.debug('locally cached!')
        return result[0].file_path

    filename = global_config[f'{dataset_id}_file_format'].format(
        variable=variable_id, date=formatted_date)
    target_path = os.path.join(
        os.path.dirname(__file__), global_config['cache_dir'], filename)
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
