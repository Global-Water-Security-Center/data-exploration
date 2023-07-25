from concurrent.futures import as_completed
from concurrent.futures import ProcessPoolExecutor
import argparse
import collections
import logging
import multiprocessing
import os
import pickle
import random
import requests
import shutil
import sys
import time
import zipfile

from rasterio.transform import Affine
from scipy.interpolate import griddata
import numpy
import pandas
import rasterio
import xarray

BASE_URL = 'https://esgf-node.llnl.gov/esg-search/search'
BASE_SEARCH_URL = 'https://esgf-node.llnl.gov/search_files'
VARIANT_SUFFIX = 'i1p1f1'
LOCAL_CACHE_DIR = '_cmip6_local_cache'
HOT_DIR = 'D:/hot_cache'
PROCESSED_FILES_PICKLE = os.path.join(HOT_DIR, 'processed_files.pkl')
for dir_path in [LOCAL_CACHE_DIR, HOT_DIR]:
    os.makedirs(dir_path, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
LOGGER.setLevel(logging.DEBUG)
logging.getLogger('fetch_data').setLevel(logging.INFO)


class ProcessedFiles:
    def __init__(self, pickle_path, manager):
        self.lock = manager.Lock()
        self.pickle_path = pickle_path
        if os.path.exists(self.pickle_path):
            LOGGER.debug(f'loading from {self.pickle_path}')
            with open(self.pickle_path, 'rb') as f:
                self.set = pickle.load(f)
                LOGGER.debug(self.set)
        else:
            self.set = set()

    def add(self, file):
        with self.lock:
            self.set.add(file)
            with open(self.pickle_path, 'wb') as f:
                pickle.dump(self.set, f)

    def __contains__(self, file):
        with self.lock:
            return file in self.set


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


def _download_and_process_file(processed_files, args):
    try:
        local_geotiff_directory = None
        netcdf_path = None
        LOGGER.info(f'********* processing {args}')
        variable, scenario, model, variant, url = args
        LOGGER.debug(f'checking {url}')
        if url in processed_files:
            return True
        netcdf_path = _download_file(HOT_DIR, url)
        if not os.path.exists(netcdf_path):
            raise ValueError(
                f'expected a file at {netcdf_path} but nothing was there downloaded from {url}')
        base_path_pattern = (
            r'cmip6/{variable}/{scenario}/{model}/{variant}/'
            r'cmip6-{variable}-{scenario}-{model}-{variant}-{date}.tif')
        local_geotiff_directory = os.path.join(
            HOT_DIR, base_path_pattern)
        target_vars = {
            'variable': variable,
            'scenario': scenario,
            'model': model,
            'variant': variant,
        }
        LOGGER.info(f'process {os.path.basename(url)}')

        with ProcessPoolExecutor(5) as executor:
            raster_by_year_map = process_cmip6_netcdf_to_geotiff(
                executor, netcdf_path, target_vars,
                local_geotiff_directory)
            for year, file_list in raster_by_year_map.items():
                zip_path_pattern = base_path_pattern.format(
                    **{**target_vars, **{'date': year}}).replace(
                    '.tif', '.zip')
                local_zip_path = os.path.join(HOT_DIR, zip_path_pattern)
                target_zip_path = os.path.join(
                    LOCAL_CACHE_DIR, zip_path_pattern)
                if not os.path.exists(target_zip_path):
                    zip_files(file_list, local_zip_path, target_zip_path)
            LOGGER.info(f'done processing {os.path.basename(url)}')
        hot_dir = os.path.dirname(local_zip_path)
        LOGGER.info(f'removing directory {os.path.dirname(hot_dir)}')
        processed_files.add(url)
        return True
    except Exception:
        LOGGER.exception(f'error on _download_and_process_file {args}')
        raise
    finally:
        if netcdf_path and os.path.exists(netcdf_path):
            os.remove(netcdf_path)
        if local_geotiff_directory and os.path.exists(local_geotiff_directory):
            shutil.rmtree(local_geotiff_directory)


def zip_files(file_list, local_path, target_path):
    with zipfile.ZipFile(local_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for file_path in file_list:
            zipf.write(file_path, arcname=os.path.basename(file_path))
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    shutil.move(local_path, target_path)
    LOGGER.info(f'zipped to {target_path}')


def _download_file(target_dir, url):
    try:
        stream_path = os.path.join(
            target_dir, 'streaming', os.path.basename(url))
        target_path = os.path.join(
            target_dir, os.path.basename(stream_path))
        if os.path.exists(target_path):
            LOGGER.info(f'{target_path} exists, skipping')
            return target_path
        os.makedirs(os.path.dirname(stream_path), exist_ok=True)
        LOGGER.info(f'downloading {url} to {target_path}')

        # Get the total file size
        # LOGGER.debug(f'get file size for {url}')
        file_stream_response = requests.get(url, stream=True, timeout=3)
        # file_size = int(file_stream_response.headers.get(
        #     'Content-Length', 0))

        # Download the file with progress
        progress = 0
        last_update = time.time()
        with open(stream_path, 'wb') as file:
            for chunk in file_stream_response.iter_content(
                    chunk_size=2**20):
                if chunk:  # filter out keep-alive new chunks
                    file.write(chunk)
                # Update the progress
                progress += len(chunk)
                if time.time()-last_update > 2:
                    print(
                        f'Downloaded downloaded {progress} bytes of '
                        f'{stream_path} {url}')
                    last_update = time.time()
            file.flush()
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        shutil.move(stream_path, target_path)
        return target_path
    except Exception:
        LOGGER.exception(
            f'failed to download {url} to {target_dir}, possibly retrying')
        if os.path.exists(stream_path):
            os.remvoe(stream_path)
        if os.path.exists(target_path):
            os.remvoe(target_path)
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
        raster_by_year = collections.defaultdict(list)
        previous_year = None
        LOGGER.debug(f'processing {netcdf_path} for time {time_var}')
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
            year = date_str[0:4]

            target_path = target_path_pattern.format(
                **{**target_vars, **{'date': date_str}})
            raster_by_year[year].append(target_path)
            if os.path.exists(target_path):
                LOGGER.debug(f'{target_path} exists no need to re-make')
                continue

            if previous_year != year:
                LOGGER.info(f'iterating on {year}: {netcdf_path}')
                previous_year = year
            data_values = daily_data.values[numpy.newaxis, ...]
            coord_list = []
            coord_id_list = []
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
                        coord_id_list.append(field_id)
                        break
                    except KeyError:
                        pass
            if len(coord_list) != 2:
                raise ValueError(
                    f'coord list not fully defined for {netcdf_path}')
            if len(daily_data.values.shape) == 1:
                # this is 1D data, need to re-create 2D grid
                lon = coord_list[0].values
                lat = coord_list[1].values
                # Define the grid
                grid_size = 0.25
                n_lat = int(numpy.round((lat.max()-lat.min())/grid_size))
                n_lon = int(numpy.round((lon.max()-lon.min())/grid_size))
                lats = numpy.linspace(lat.min(), lat.max(), n_lat)
                lons = numpy.linspace(lon.min(), lon.max(), n_lon)
                lon_grid, lat_grid = numpy.meshgrid(lons, lats)

                res_list = [grid_size, grid_size]
                coord_list = [lons, lats]

                # Perform the 2D interpolation
                data_values = griddata(
                    (lon, lat), daily_data, (lon_grid, lat_grid),
                    method='linear')
                data_values = data_values[numpy.newaxis, ...]
            transform = Affine.translation(
                *[a[0] for a in coord_list]) * Affine.scale(*res_list)
            future = executor.submit(
                _write_raster, data_values, coord_list, transform, target_path)
            future_list.append(future)
        for future in future_list:
            try:
                # This will raise an exception if the worker function failed
                _ = future.result()
            except Exception:
                LOGGER.exception(
                    f'something failed on process CMIP6 data {target_vars}')
                executor.shutdown(wait=False)
                raise
        return raster_by_year
    except Exception:
        LOGGER.exception(f'error on {netcdf_path}')
        raise


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
        LOGGER.debug(f'wrote the file {target_path}')
        return target_path
    except Exception:
        LOGGER.exception(f'error on _write_raster for {target_path}')
        raise


def main():
    parser = argparse.ArgumentParser(description=(
        'Process CMIP6 raw urls to geotiff.'))
    parser.add_argument('url_list_path', help='url list')
    args = parser.parse_args()
    with multiprocessing.Manager() as manager:
        processed_files = ProcessedFiles(PROCESSED_FILES_PICKLE, manager)

        # pr,historical,SAM0-UNICON,r1i1p1f1,http://aims3.llnl.gov/thredds/fileServer/css03_data/CMIP6/CMIP/SNU/SAM0-UNICON/historical/r1i1p1f1/day/pr/gn/v20190323/pr_day_SAM0-UNICON_historical_r1i1p1f1_gn_19000101-19001231.nc

        with open(args.url_list_path, 'r') as file:
            param_and_url_list = [line.rstrip().split(',') for line in file]

        random.seed(1)
        random.shuffle(param_and_url_list)
        # for index, val in enumerate(param_and_url_list):
        #     print(f'{index}: {val}')
        # return
        #param_and_url_list = [param_and_url_list[2]]
        start_time = time.time()
        with ProcessPoolExecutor(5) as global_executor:
            future_list = {
                (global_executor.submit(
                    _download_and_process_file, processed_files, param_and_url_arg),
                 param_and_url_arg)
                for param_and_url_arg in param_and_url_list[0:5]}

            for future, param_and_url_arg in as_completed(future_list):
                try:
                    _ = future.result()  # This will raise an exception if the worker function failed
                except Exception:
                    LOGGER.exception(
                        f'something failed on download and process for '
                        f'{param_and_url_arg} but still continuing')
                    #global_executor.shutdown(wait=False)
                    #raise  # Re-raise the original exception
            LOGGER.debug(f'about to quit executor')
    LOGGER.info(f'all done took {time.time()-start_time:.2f}s')

if __name__ == '__main__':
    main()
