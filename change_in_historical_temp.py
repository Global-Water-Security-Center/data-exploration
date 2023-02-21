"""Utility to extract CIMP5 data from GEE."""
import argparse
import collections
import datetime
import concurrent
import glob
import logging
import sys
import os
import pickle
import time
import requests

import ee
import geemap
import geopandas


logging.basicConfig(
    level=logging.WARNING,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)

CACHE_DIR = '_cimp5_cache_dir'
DATASET_ID = 'NASA/NEX-GDDP'
DATASET_CRS = 'EPSG:4326'
DATASET_SCALE = 27830
SCENARIO_LIST = ['historical', 'rcp45', 'rcp85']
BAND_NAMES = ['tasmin', 'tasmax', 'pr']
MODEL_LIST = ['ACCESS1-0', 'bcc-csm1-1', 'BNU-ESM', 'CanESM2', 'CCSM4', 'CESM1-BGC', 'CNRM-CM5', 'CSIRO-Mk3-6-0', 'GFDL-CM3', 'GFDL-ESM2G', 'GFDL-ESM2M', 'inmcm4', 'IPSL-CM5A-LR', 'IPSL-CM5A-MR', 'MIROC-ESM', 'MIROC-ESM-CHEM', 'MIROC5', 'MPI-ESM-LR', 'MPI-ESM-MR', 'MRI-CGCM3', 'NorESM1-M']
MODELS_BY_DATE_CACHEFILE = '_cimp5_models_by_date.dat'
QUOTA_READS_PER_MINUTE = 3000


def _throttle_query():
    """Sleep to avoid exceeding quota request."""
    time.sleep(1/QUOTA_READS_PER_MINUTE*60)


def fetch_models_by_date(base_dataset, date_list):
    """Query GEE for names of CIMP5 models for the given date_by_year list."""
    try:
        with open(MODELS_BY_DATE_CACHEFILE, 'rb') as models_by_date_file:
            models_by_date = pickle.load(models_by_date_file)
    except Exception:
        models_by_date = dict()

    LOGGER.info(
        f'it will take {1/QUOTA_READS_PER_MINUTE*60*len(date_list):.2f}s to '
        f'query {len(date_list)} dates')

    with concurrent.futures.ThreadPoolExecutor() as executor:
        model_by_date_executor = {}
        for date in date_list:
            if date not in models_by_date:
                model_by_date_executor[date] = executor.submit(
                    lambda date_dataset: [
                        x['properties']['model']
                        for x in date_dataset.getInfo()['features']],
                    base_dataset.filter(ee.Filter.date(date)))
                _throttle_query()

        # evaluate available models
        models_by_date = models_by_date | {
            date: future.result()
            for date, future in model_by_date_executor.items()}
    with open(MODELS_BY_DATE_CACHEFILE, 'wb') as models_by_date_file:
        pickle.dump(models_by_date, models_by_date_file)
    return models_by_date


def process_date_chunk(ee_result, cache_file_pattern, cache_path_lock):
    """Process value by date dict asychronously and then save to cache."""
    result = ee_result.getInfo()
    with cache_path_lock:
        # search for the most recent pattern and then make a new file
        try:
            next_index = 1 + max([
                int(os.path.splitext(path.split('_')[-1])[0])
                for path in glob.glob(cache_file_pattern)])
        except Exception:
            next_index = 0
        cache_path = (
            f'{"_".join(cache_file_pattern.split("_")[:-1])}_{next_index}.dat')
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        # result_by_month indexes by YYYY-MM into a dictonary of dates to results
        LOGGER.debug(f'writing cache file {cache_path}')
        with open(cache_path, 'wb') as result_by_date_file:
            pickle.dump(result, result_by_date_file)


def load_cached_results(cache_file_pattern):
    """Load all files that match the file pattern and dump into one dictionary."""
    cached_results = dict()
    for file_path in glob.glob(cache_file_pattern):
        with open(file_path, 'rb') as cache_file:
            local_cached_results = pickle.load(cache_file)
            cached_results = cached_results | local_cached_results
    return cached_results


def download_image(image, poly_bounds, target_path):
    url = image.clip(poly_bounds).getDownloadUrl({
        'region': poly_bounds.geometry().bounds(),
        'scale': 27830,
        'format': 'GEO_TIFF'
    })
    LOGGER.debug(f'saving {target_path}')
    response = requests.get(url)
    with open(target_path, 'wb') as fd:
        fd.write(response.content)


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(
        description='Examine historical change of temp in gregion.')
    parser.add_argument(
        'aoi_vector_path', help='Path to vector/shapefile of area of interest')
    parser.add_argument(
        '--authenticate', action='store_true',
        help='Pass this flag if you need to reauthenticate with GEE')
    args = parser.parse_args()
    aoi_vector = geopandas.read_file(args.aoi_vector_path)

    if args.authenticate:
        ee.Authenticate()
        return
    ee.Initialize()

    LOGGER.info('querying which models are available by date')
    start_day = datetime.datetime.strptime('1985-01-01', '%Y-%m-%d')
    end_day = datetime.datetime.strptime('2005-12-31', '%Y-%m-%d')
    date_list = [
        (start_day + datetime.timedelta(days=delta_day)).strftime('%Y-%m-%d')
        for delta_day in range((end_day-start_day).days+1)]

    cmip5_dataset = ee.ImageCollection(DATASET_ID)
    models_by_date = fetch_models_by_date(cmip5_dataset, date_list)

    vector_basename = os.path.basename(os.path.splitext(
        args.aoi_vector_path)[0])

    aoi_vector = geopandas.read_file(args.aoi_vector_path)
    local_shapefile_path = f'_local_ok_to_delete_{vector_basename}.json'
    aoi_vector = aoi_vector.to_crs('EPSG:4326')
    aoi_vector.to_file(local_shapefile_path, driver='GeoJSON')
    aoi_vector = None
    ee_poly = geemap.geojson_to_ee(local_shapefile_path)
    os.remove(local_shapefile_path)

    # Find the lowest mean daily temperature for each year.
    # calculate the average of these low mean daily temperatures.
    min_by_time_range = {}
    time_range_list = [(1986, 1987), (2069, 2070)]
    for start_year, end_year in time_range_list:
        yearly_min_by_model = collections.defaultdict(list)
        for year in range(start_year, end_year+1):
            start_day = datetime.datetime.strptime(f'{year}-01-01', '%Y-%m-%d')
            # debug, just do one day
            # end_day = datetime.datetime.strptime(f'{year}-12-31', '%Y-%m-%d')
            end_day = datetime.datetime.strptime(f'{year}-02-01', '%Y-%m-%d')
            date_list = [
                (start_day + datetime.timedelta(days=delta_day)).strftime('%Y-%m-%d')
                for delta_day in range((end_day-start_day).days+1)]

            daily_mean_list_by_model = collections.defaultdict(list)
            for date in date_list:
                model_list = models_by_date[date]
                for model_id in model_list:
                    LOGGER.debug(f'{date}-{model_id}')
                    daily_mean = (
                        cmip5_dataset.
                        filter(ee.Filter.eq('model', model_id)).
                        filter(ee.Filter.date(date)).
                        first().
                        select(['tasmax', 'tasmin']).
                        reduce('mean').
                        add(-272.15))  # convert to C
                    daily_mean_list_by_model[model_id].append(daily_mean)
            for model_id in model_list:
                daily_mean_collection = ee.ImageCollection.fromImages(
                    daily_mean_list_by_model[model_id])
                yearly_minimum_mean = daily_mean_collection.min()
                yearly_min_by_model[model_id].append(yearly_minimum_mean)

        mean_yearly_low_temp_by_model = {}
        for model_id in model_list:
            LOGGER.debug(f'{model_id} has {len(yearly_min_by_model[model_id])} years')
            mean_yearly_low_temp_by_model[model_id] = (
                ee.ImageCollection.fromImages(yearly_min_by_model[model_id])).mean()
        min_by_time_range[(start_year, end_year)] = (
            mean_yearly_low_temp_by_model)

    for model_id in model_list:
        future_time_range = time_range_list[-1]
        past_time_range = time_range_list[0]
        temperature_difference = (
            min_by_time_range[future_time_range][model_id].
            subtract(min_by_time_range[past_time_range][model_id]))
        raster_path = f"""{vector_basename}_{model_id}_{
            future_time_range}_{past_time_range}_mean_min_temp_diff.tif"""
        download_image(
            temperature_difference, ee_poly, raster_path)
    # mean_yearly_low_temp_by_model is ready to go

    # For any future period (for 50 years out, this would be the 10 year
    # window around 2023+50 = 2073, so 2069-2078), find the lowest mean
    # daily temperature for each one of those 10 years. Calculate the average
    # future low temperature. Subtract that average future low temp from the
    # average historic low temp calculated in the previous step. This gives
    # you the average change in future low daily mean temperature for that
    # model.

    LOGGER.info('done!')

    """
    For temperature:
        For each model we have a min daily temp and a max daily temp.

        If the analysis is watershed based, you can find average values across
        the watershed before proceeding to make the number crunching easier.

        BUT if Rich is doing this distributed by grid cell, it’s actually
        better if we take watershed averages of those final outputs

        xFor the historic period (ideally 30 years ending at the most recent
        xdata (1986-2005), but going back to 1950 is fine if that’s already
        xcalculated), find the lowest mean daily temperature for each year.
        xCalculate the average of these low mean daily temperatures.

        For any future period (for 50 years out, this would be the 10 year
        window around 2023+50 = 2073, so 2069-2078), find the lowest mean
        daily temperature for each one of those 10 years. Calculate the average
        future low temperature. Subtract that average future low temp from the
        average historic low temp calculated in the previous step. This gives
        you the average change in future low daily mean temperature for that
        model.

        Repeat for each model.
            Calculate the average across all 20 models of the average change in
            future low daily mean temperature. Note the maximum and minimum change
            Report this average change in average low temperature, along with the
            maximum and minimum average change in future low daily mean
            temperature


        For Precip
        Drop ACCESS1-0 – it’s super unstable for some of the runs, so better to get rid of it for all of the runs
        For each model, calculate historic mean annual precip
        For each model, for each future period (for 50 years out, this would be the 10 year window around 2023+50 = 2073, so 2069-2078), calculate the difference between future annual rainfall and historic mean annual precip. Divide by historic mean annual precip to get % change. You should have 10 values for each model for each future period
        For each time period and future period, take all 200 of the % change values (20 models * 10 years) and make a box plot
    """


if __name__ == '__main__':
    main()
