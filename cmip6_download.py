from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
import argparse
import random
import logging
import os
import requests
import shutil
import sys
from functools import partial
from threading import Lock
import time

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


def handle_retry_error(retry_state):
    # retry_state.outcome is a built-in tenacity method that contains the result or exception information from the last call
    last_exception = retry_state.outcome.exception()
    LOGGER.error(last_exception)
    with open(f'cmip6_download_error_log_{__name__}.txt', 'a') as error_log:
        error_log.write(
            f"{retry_state.args[0]}," +
            str(last_exception).replace('\n', '<enter>')+"\n")
        error_log.write(
            f"\t{retry_state.args[0]}," +
            str(last_exception.statistics).replace('\n', '<enter>')+"\n")


def _download_and_process_file(args):
    try:
        with ProcessPoolExecutor(10) as executor:
            variable, scenario, model, variant, url = args
            netcdf_path = _download_file(LOCAL_CACHE_DIR, url)
            target_path_pattern = r'cmip6/{variable}/{scenario}/{model}/{variant}/cmip6-{variable}-{scenario}-{model}-{variant}-{date}.tif'

            target_vars = {
                'variable': variable,
                'scenario': scenario,
                'model': model,
                'variant': variant,
            }
            LOGGER.info(f'process {target_path_pattern}')
            process_cmip6_netcdf_to_geotiff(
                executor, netcdf_path, target_vars, target_path_pattern)
            LOGGER.info(f'done processing {target_path_pattern}')
    except Exception:
        LOGGER.exception(f'error on _download_and_process_file {args}')
        raise


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
        executor, netcdf_path, target_vars, target_path_pattern):
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
        dataset = xarray.open_dataset(netcdf_path)
        time_var = dataset['time']
        variable = target_vars['variable']
        future_list = []
        for i, date in enumerate(time_var):
            daily_data = dataset[variable].isel(time=i)
            date_str = None
            for conversion_fn in [
                    lambda d: d.item().strftime('%Y-%m-%d'),
                    lambda d: d.time.strftime('%Y-%m-%d'),
                    lambda d: pandas.to_datetime(d.values).strftime('%Y-%m-%d'),
                    lambda d: pandas.to_datetime(d.item(), unit='D'),
                    lambda d: pandas.to_datetime(d, unit='D'),
                    lambda d: d.strftime('%Y-%m-%d'),
                    ]:
                try:
                    date_str = conversion_fn(date)
                    break
                except Exception:
                    #LOGGER.exception(f'could not convert, trying again')
                    continue
            if date_str is None:
                raise ValueError(f'could not convert {date}')
            data_values = daily_data.values[numpy.newaxis, ...]
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
                        pass
            if len(coord_list) != 2:
                raise ValueError(
                    f'coord list not fully defined for {netcdf_path}')
            transform = Affine.translation(
                *[a[0] for a in coord_list]) * Affine.scale(*res_list)

            target_path = target_path_pattern.format(
                **{**target_vars, **{'date': date_str}})
            future = executor.submit(
                _write_raster, data_values, coord_list, transform, target_path)
            future_list.append(future)
        raster_list_result = [future.result() for future in future_list]
        if len(raster_list_result) != len(future_list):
            raise ValueError('DIFFERENT VALUES')
        return
    except Exception:
        LOGGER.exception(f'error on {netcdf_path}')


def _write_raster(data_values, coord_list, transform, target_path):
    try:
        #LOGGER.info(f'processing {target_path}')
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
            new_dataset.write(data_values)
        return target_path
    except Exception:
        LOGGER.exception(f'error on _write_raster for {target_path}')
        raise


def main():
    parser = argparse.ArgumentParser(description=(
        'Process CMIP6 raw urls to geotiff.'))
    parser.add_argument('url_list_path', help='url list')
    args = parser.parse_args()

    # pr,historical,SAM0-UNICON,r1i1p1f1,http://aims3.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/SNU/SAM0-UNICON/historical/r1i1p1f1/day/pr/gn/v20190323/pr_day_SAM0-UNICON_historical_r1i1p1f1_gn_19000101-19001231.nc

    with open(args.url_list_path, 'r') as file:
        param_and_url_list = [line.rstrip().split(',') for line in file]

    random.seed(1)
    param_and_url_list = random.sample(param_and_url_list)
    # for index, val in enumerate(param_and_url_list):
    #     print(f'{index}: {val}')
    # return
    #param_and_url_list = [param_and_url_list[2]]
    start_time = time.time()
    with ProcessPoolExecutor(5) as global_executor:
        print('executing')
        list(global_executor.map(
            _download_and_process_file, param_and_url_list))
    LOGGER.info(f'all done took {time.time()-start_time:.2f}s')

if __name__ == '__main__':
    main()
