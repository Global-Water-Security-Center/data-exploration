import argparse
import collections
import csv
import configparser

import boto3

GLOBAL_INI_PATH = 'defaults.ini'


def fetch_file(
        global_config, dataset_id, variable_id, date_str, target):
    """Fetch a file from remote data store to local target path.

    Args:
        global_config (configparser object):
        dataset_id (str):
        variable_id (str):
        date_str (str):
        target (str):

    """
    pass


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
    args = parser.parse_args()

    reader = csv.DictReader(
        open(global_config[f'{args.dataset_id}_access_key']))
    for row in reader:
        s3 = boto3.resource(
            's3',
            endpoint_url=global_config[f'{args.dataset_id}_base_uri'],
            aws_access_key_id=row['Access Key Id'],
            aws_secret_access_key=row['Secret Access Key'],
        )

    # Create connection to Wasabi / S3
    # Get bucket object
    my_bucket = s3.Bucket(global_config[f'{args.dataset_id}_bucket_id'])
    # Download remote object "myfile.txt" to local file "test.txt"
    my_bucket.download_file('reanalysis-era5-sfc-daily-1950-01-05_mean_t2m_c.tif', "test.tif")


if __name__ == '__main__':
    main()
