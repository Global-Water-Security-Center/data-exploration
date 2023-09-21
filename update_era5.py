"""See `python scriptname.py --help"""
from functools import partial
from concurrent.futures import ThreadPoolExecutor
import argparse
import datetime
import logging
import os
import sys

from ecoshard import fetch_data
from rasterio.transform import Affine
import numpy
import rasterio
import xarray

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
LOGGER.setLevel(logging.DEBUG)
logging.getLogger('fetch_data').setLevel(logging.INFO)


def process_era5_netcat_to_geotiff(netcat_path, date_str, target_path_pattern):
    """Convert era5 netcat files to geotiff

    Args:
        netcat_path (str): path to netcat file
        date_str (str): formatted version of the date to use in the target
            file
        target_path_pattern (str): pattern that will allow the replacement
            of `variable` and `date` strings with the appropriate variable
            date strings in the netcat variables.

    Returns:
        list of (file, variable_id) tuples created by this process
    """
    LOGGER.info(f'processing {netcat_path}')
    dataset = xarray.open_dataset(netcat_path)

    res_list = []
    coord_list = []
    for coord_id, field_id in zip(['x', 'y'], ['longitude', 'latitude']):
        coord_array = dataset.coords[field_id]
        res_list.append(float(
            (coord_array[-1] - coord_array[0]) / len(coord_array)))
        coord_list.append(coord_array)

    transform = Affine.translation(
        *[a[0] for a in coord_list]) * Affine.scale(*res_list)

    target_path_variable_id_list = []
    for variable_id, data_array in dataset.items():
        target_path = target_path_pattern.format(**{
            'date': date_str,
            'variable': variable_id
            })
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        with rasterio.open(
            target_path,
            mode="w",
            driver="GTiff",
            height=len(coord_list[1]),
            width=len(coord_list[0]),
            count=1,
            dtype=numpy.float32,
            nodata=None,
            crs="+proj=latlong",
            transform=transform,
            **{
                'tiled': 'YES',
                'COMPRESS': 'LZW',
                'PREDICTOR': 2}) as new_dataset:
            new_dataset.write(data_array)
        target_path_variable_id_list.append((target_path, variable_id))
    return target_path_variable_id_list


def download_and_repack(date_str, target_path_pattern):
    try:
        LOGGER.info(f'fetching {date_str}')
        netcat_path = fetch_data.fetch_file(
            'aer_era5_netcat_daily', {'date': date_str})
        LOGGER.info(f'downloaded to {netcat_path}')
        geotiff_path_variable_id_list = process_era5_netcat_to_geotiff(
            netcat_path, date_str, target_path_pattern)
        for geotiff_path, variable_id in geotiff_path_variable_id_list:
            remote_path = fetch_data.put_file(geotiff_path, 'era5_daily', {
                'date': date_str,
                'variable': variable_id})
            LOGGER.info(f'uploaded to {remote_path}')
    except FileNotFoundError:
        LOGGER.error(f'No file found for {date_str}, skipping')


def main():
    parser = argparse.ArgumentParser(description=(
        'Synchronize the files in AER era5 to GWSC wasabi hot storage.'))
    parser.add_argument(
        'start_date', type=str, help='Pick a date to start downloading.')
    parser.add_argument(
        'end_date', type=str, help='Pick a date to start downloading.')
    parser.add_argument(
        '--local_workspace', type=str, default='era5_process_workspace',
        help='Directory to downloand and work in.')
    args = parser.parse_args()

    start_day = datetime.datetime.strptime(args.start_date, '%Y-%m-%d')
    end_day = datetime.datetime.strptime(args.end_date, '%Y-%m-%d')

    target_path_pattern = os.path.join(
        args.local_workspace,
        fetch_data.GLOBAL_CONFIG['era5_daily']['file_format'])

    current_day = start_day
    date_list = []
    while current_day <= end_day:
        date_str = current_day.strftime('%Y-%m-%d')
        date_list.append(date_str)
        current_day = current_day + datetime.timedelta(days=1)

    with ThreadPoolExecutor(max_workers=50) as executor:
        _ = list(executor.map(partial(
            download_and_repack, target_path_pattern=target_path_pattern),
            date_list))


if __name__ == '__main__':
    main()
