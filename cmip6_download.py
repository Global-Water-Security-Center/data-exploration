from concurrent.futures import ThreadPoolExecutor
import argparse
import logging
import os
import requests
import shutil
import sys
from functools import partial
from threading import Lock
import traceback

from tenacity import retry, stop_after_attempt, wait_random_exponential
from rasterio.transform import Affine
import numpy
import rasterio
import xarray
import pandas

BASE_URL = 'https://esgf-node.llnl.gov/esg-search/search'
BASE_SEARCH_URL = 'https://esgf-node.llnl.gov/search_files'
VARIANT_SUFFIX = 'i1p1f1'
LOCAL_CACHE_DIR = '_cmip6_local_cache'
os.makedirs(LOCAL_CACHE_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
LOGGER.setLevel(logging.DEBUG)
logging.getLogger('fetch_data').setLevel(logging.INFO)

ERROR_LOCK = Lock()


def handle_retry_error(retry_state):
    # retry_state.outcome is a built-in tenacity method that contains the result or exception information from the last call
    last_exception = retry_state.outcome.exception()
    LOGGER.error(last_exception)
    with ERROR_LOCK:
        with open('cmip6_download_error_log.txt', 'a') as error_log:
            error_log.write(
                f"{retry_state.args[0]}," +
                str(last_exception).replace('\n', '<enter>')+"\n")
            error_log.write(
                f"\t{retry_state.args[0]}," +
                str(last_exception.statistics).replace('\n', '<enter>')+"\n")


def _download_and_process_file(args):
    variable, scenario, model, variant, url = args
    netcdf_path = _download_file(LOCAL_CACHE_DIR, url)
    target_path_pattern = r'cmip6/{variable}/{scenario}/{model}/{variant}/cmip6-{variable}-{scenario}-{model}-{variant}-{date}.tif'

    target_vars = {
        'variable': variable,
        'scenario': scenario,
        'model': model,
        'variant': variant,
    }

    process_cmip6_netcdf_to_geotiff(
        netcdf_path, target_vars, target_path_pattern)


@retry(stop=stop_after_attempt(5),
       wait=wait_random_exponential(multiplier=1, min=1, max=10),
       retry_error_callback=handle_retry_error)
def _download_file(target_dir, url):
    try:
        stream_path = os.path.join(
            LOCAL_CACHE_DIR, 'streaming', os.path.basename(url))
        target_path = os.path.join(
            LOCAL_CACHE_DIR, target_dir, os.path.basename(stream_path))
        if os.path.exists(target_path):
            print(f'{target_path} exists, skipping')
            return target_path
        os.makedirs(os.path.dirname(stream_path), exist_ok=True)
        LOGGER.info(f'downloading {url} to {target_path}')

        # Get the total file size
        file_stream_response = requests.get(url, stream=True)
        file_size = int(file_stream_response.headers.get(
            'Content-Length', 0))

        # Download the file with progress
        progress = 0
        with open(stream_path, 'wb') as file:
            for chunk in file_stream_response.iter_content(
                    chunk_size=2**20):
                if chunk:  # filter out keep-alive new chunks
                    file.write(chunk)
                # Update the progress
                progress += len(chunk)
                print(
                    f'Downloaded {progress:{len(str(file_size))}d} of {file_size} bytes '
                    f'({100. * progress / file_size:5.1f}%) of '
                    f'{stream_path} {target_path}')
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        shutil.move(stream_path, target_path)
        return target_path
    except Exception:
        LOGGER.exception(
            f'failed to download {url} to {target_dir}, possibly retrying')
        raise


def process_cmip6_netcdf_to_geotiff(
        netcdf_path, target_vars, target_path_pattern):
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
    try:
        LOGGER.info(f'processing {netcdf_path}')
        dataset = xarray.open_dataset(netcdf_path)
        LOGGER.info(f"{dataset['time']}")
        # suppose ds is your xarray.Dataset
        time_var = dataset['time']
        print(time_var)
        variable = target_vars['variable']
        for i, date in enumerate(time_var):
            LOGGER.debug(date.item())
            # 'tas' data for the given date
            daily_data = dataset[variable].isel(time=i)
            # convert to numpy array

            # print or process date and 2D data
            date_str = date.item().strftime('%Y-%m-%d')
            LOGGER.info(f"Date: {date_str}")
            LOGGER.info(f"2D Data: {daily_data}")

            coord_list = []
            res_list = []
            for coord_id, field_options in zip(['x', 'y'], [
                    ['longitude', 'long', 'lon'],
                    ['latitude', 'lat']]):
                for field_id in field_options:
                    try:
                        coord_array = dataset.coords[field_id]
                        res_list.append(float(
                            (coord_array[-1] - coord_array[0]) /
                            len(coord_array)))
                        coord_list.append(coord_array)
                    except KeyError:
                        LOGGER.warn(f'{field_id} not in dataset')
            if len(coord_list) != 2:
                raise ValueError(
                    f'coord list not fully defined for {netcdf_path}')
            LOGGER.debug(coord_list)
            LOGGER.debug(len(coord_list[0]))
            LOGGER.debug(len(coord_list[1]))

            transform = Affine.translation(
                *[a[0] for a in coord_list]) * Affine.scale(*res_list)

            target_path_variable_id_list = []
            target_path = target_path_pattern.format(
                **{**target_vars, **{'date': date_str}})
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
                new_dataset.write(daily_data.values)
                target_path_variable_id_list.append(target_path)
        return target_path_variable_id_list
    except Exception:
        LOGGER.exception(f'error on {netcdf_path}')


def main():
    parser = argparse.ArgumentParser(description=(
        'Process CMIP6 raw urls to geotiff.'))
    parser.add_argument('url_list_path', help='url list')
    args = parser.parse_args()

    # pr,historical,SAM0-UNICON,r1i1p1f1,http://aims3.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/SNU/SAM0-UNICON/historical/r1i1p1f1/day/pr/gn/v20190323/pr_day_SAM0-UNICON_historical_r1i1p1f1_gn_19000101-19001231.nc

    with open(args.url_list_path, 'r') as file:
        param_and_url_list = [line.rstrip().split(',') for line in file]

    with ThreadPoolExecutor(50) as executor:
        print('executing')
        executor.map(_download_and_process_file, param_and_url_list[0:1])


if __name__ == '__main__':
    main()
