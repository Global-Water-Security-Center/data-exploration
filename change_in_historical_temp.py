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

from ecoshard import geoprocessing
from osgeo import gdal
import matplotlib.pyplot as plt
import numpy
import ee
import geemap
import geopandas
import pandas

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
    if not os.path.exists(target_path):
        url = image.getDownloadUrl({
            'region': poly_bounds.geometry().bounds(),
            'scale': 27830,
            'format': 'GEO_TIFF'
        })
        LOGGER.debug(f'saving {target_path}')
        response = requests.get(url)
        with open(target_path, 'wb') as fd:
            fd.write(response.content)
    else:
        LOGGER.info(f'{target_path} already exists, not overwriting')


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
    end_day = datetime.datetime.strptime('2079-12-31', '%Y-%m-%d')
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

    # Find the lowest mean daily temperature for each year.
    # calculate the average of these low mean daily temperatures.
    time_range_list = [
        (1986, 2005, 'historical'),
        (2069, 2079, 'rcp45'),
        (2069, 2079, 'rcp85')]

    # calc_historical_temp(
    #     time_range_list, models_by_date, cmip5_dataset, vector_basename,
    #     ee_poly, local_shapefile_path)

    # For any future period (for 50 years out, this would be the 10 year
    # window around 2023+50 = 2073, so 2069-2078), find the lowest mean
    # daily temperature for each one of those 10 years. Calculate the average
    # future low temperature. Subtract that average future low temp from the
    # average historic low temp calculated in the previous step. This gives
    # you the average change in future low daily mean temperature for that
    # model.

    # Drop ACCESS1-0 – it’s super unstable for some of the runs, so better to
    # get rid of it for all of the runs
    # For each model, calculate historic mean annual precip

    # For each model, for each future period (for 50 years out, this would be
    # the 10 year window around 2023+50 = 2073, so 2069-2078), calculate the
    # difference between future annual rainfall and historic mean annual
    # precip. Divide by historic mean annual precip to get % change. You
    # should have 10 values for each model for each future period

    # For each time period and future period, take all 200 of the % change
    # values (20 models * 10 years) and make a box plot

    calc_historical_precip(
        time_range_list, models_by_date, cmip5_dataset, vector_basename,
        local_shapefile_path, ee_poly)
    # os.remove(local_shapefile_path)
    LOGGER.info('done!')


def calc_historical_temp(
        time_range_list, models_by_date, cmip5_dataset, vector_basename,
        ee_poly, local_shapefile_path):
    workspace_dir = 'historical_temp_workspace'
    os.makedirs(workspace_dir, exist_ok=True)

    temp_model_by_time_range = collections.defaultdict(dict)
    for time_range in time_range_list:
        start_year, end_year, scenario_id = time_range
        yearly_min_by_model = collections.defaultdict(list)
        for year in range(start_year, end_year+1):
            start_day = datetime.datetime.strptime(f'{year}-01-01', '%Y-%m-%d')
            end_day = datetime.datetime.strptime(f'{year}-12-31', '%Y-%m-%d')
            date_list = [
                (start_day + datetime.timedelta(days=delta_day)).strftime(
                    '%Y-%m-%d')
                for delta_day in range((end_day-start_day).days+1)]

            daily_mean_list_by_model = collections.defaultdict(list)
            for date in date_list:
                model_list = models_by_date[date]
                for model_id in model_list:
                    daily_mean = (
                        cmip5_dataset.
                        filter(ee.Filter.eq('model', model_id)).
                        filter(ee.Filter.eq('scenario', scenario_id)).
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
            LOGGER.debug(
                f'{model_id} has {len(yearly_min_by_model[model_id])} years')
            mean_yearly_low_temp_by_model[model_id] = (
                ee.ImageCollection.fromImages(
                    yearly_min_by_model[model_id])).mean()
            raster_path = os.path.join(
                workspace_dir, f"""{vector_basename}_{
                model_id}_{start_year}_{end_year}_{
                scenario_id}_mean_min_temp.tif""")
            download_image(
                mean_yearly_low_temp_by_model[model_id], ee_poly, raster_path)
            temp_model_by_time_range[model_id][time_range] = raster_path

    # for each model, subtract the two time periods
    change_in_temp_list_by_scenario = collections.defaultdict(list)
    nodata_target = -9999
    for model_id in model_list:
        historic_temp_path = temp_model_by_time_range[model_id][time_range_list[0]]
        nodata = geoprocessing.get_raster_info(historic_temp_path)['nodata'][0]
        for future_time_range in time_range_list[1:]:
            _, _, scenario_id = future_time_range
            target_raster_path = os.path.join(
                workspace_dir, f'{model_id}_{scenario_id}_change_in_temp.tif')
            future_temp_path = temp_model_by_time_range[
                model_id][future_time_range]

            def _sub_op(raster_a, raster_b):
                valid_mask = (raster_a != nodata) & (raster_b != nodata)
                result = numpy.empty(raster_a.shape, dtype=numpy.float32)
                result[:] = nodata
                result[valid_mask] = raster_a[valid_mask]-raster_b[valid_mask]
                return result

            geoprocessing.raster_calculator(
                [(future_temp_path, 1), (historic_temp_path, 1)], _sub_op,
                target_raster_path, gdal.GDT_Float32, nodata_target)
            change_in_temp_list_by_scenario[scenario_id].append(
                target_raster_path)

    # change_in_temp_list_by_scenario indexes by rcp45 and 85 and gives a
    # list of all the model runs of precip in a list, now we take the mean
    for future_time_range in time_range_list[1:]:
        _, _, scenario_id = future_time_range

        def _mean_op(*raster_list):
            valid_mask = numpy.ones(raster_list[0].shape, dtype=bool)
            for raster in raster_list:
                valid_mask &= raster != nodata_target
            result = numpy.zeros(raster_list[0].shape, dtype=numpy.float32)
            for raster in raster_list:
                result[valid_mask] += raster[valid_mask]
            result[valid_mask] /= len(raster_list)
            result[~valid_mask] = nodata_target
            return result

        historic_mean_temp_difference_path = os.path.join(
            workspace_dir, f'historic_mean_temp_difference_{scenario_id}_BASE.tif')
        geoprocessing.raster_calculator(
            [(path, 1) for path in change_in_temp_list_by_scenario[
             scenario_id]], _mean_op, historic_mean_temp_difference_path,
            gdal.GDT_Float32, nodata_target)

        raster_info = geoprocessing.get_raster_info(
            historic_mean_temp_difference_path)
        historic_mean_temp_difference_clip_path = (
            f'historic_mean_temp_difference_{scenario_id}.tif')
        geoprocessing.warp_raster(
            historic_mean_temp_difference_path, raster_info['pixel_size'],
            historic_mean_temp_difference_clip_path, 'near',
            vector_mask_options={'mask_vector_path': local_shapefile_path})
        raster = gdal.OpenEx(
            historic_mean_temp_difference_clip_path, gdal.OF_RASTER)
        band = raster.GetRasterBand(1).ReadAsArray()
        band_min = band[band != nodata_target].min()
        band_max = band[band != nodata_target].max()
        LOGGER.debug(
            f'min/max for {historic_mean_temp_difference_clip_path}: '
            f'{band_min} / {band_max}')


def calc_historical_precip(
        time_range_list, models_by_date, cmip5_dataset, vector_basename,
        vector_path, ee_poly):
    # calcualte historical precip
    historic_time_range = time_range_list[0]
    historic_start_year, historic_end_year, historic_scenario_id = (
        historic_time_range)

    annual_precip_list_by_model = collections.defaultdict(list)

    workspace_dir = 'historical_precip_workspace'
    os.makedirs(workspace_dir, exist_ok=True)
    for year in range(historic_start_year, historic_end_year+1):
        start_day = f'{year}-01-01'
        end_day = f'{year}-12-31'

        # for date in date_list:
        model_list = models_by_date[start_day]
        for model_id in model_list:
            annual_precip = (
                cmip5_dataset.
                filter(ee.Filter.eq('model', model_id)).
                filter(ee.Filter.eq('scenario', historic_scenario_id)).
                filter(ee.Filter.date(start_day, end_day)).
                sum().
                select(['pr']).
                multiply(86400))  # convert to mm/day
            annual_precip_list_by_model[model_id].append(annual_precip)

    historic_precip_by_model = {}
    for model_id in model_list:
        raster_path = os.path.join(
            workspace_dir, f'annual_precip_historical_{model_id}.tif')
        historic_annual_precip = ee.ImageCollection.fromImages(
            annual_precip_list_by_model[model_id]).mean()
        download_image(historic_annual_precip, ee_poly, raster_path)
        historic_precip_by_model[model_id] = raster_path

    future_precip_by_model_scenario_and_year = collections.defaultdict(
        lambda: collections.defaultdict(dict))
    for start_year, end_year, scenario_id in time_range_list[1:]:
        LOGGER.debug(f'{start_year}, {end_year}, {scenario_id}')
        for year in range(start_year, end_year+1):
            LOGGER.debug(f'working on {year}')
            start_day = f'{year}-01-01'
            end_day = f'{year}-12-31'
            model_list = models_by_date[start_day]
            LOGGER.debug(model_list)
            for model_id in set(model_list):
                LOGGER.debug(f'working on model {model_id}')
                future_annual_precip = (
                    cmip5_dataset.
                    filter(ee.Filter.eq('model', model_id)).
                    filter(ee.Filter.eq('scenario', scenario_id)).
                    filter(ee.Filter.date(start_day, end_day)).
                    sum().
                    select(['pr']).
                    multiply(86400))  # convert to mm/day
                raster_path = os.path.join(
                    workspace_dir,
                    f'annual_precip_{scenario_id}_{year}_{model_id}.tif')
                LOGGER.debug(f'about to save {raster_path}')
                download_image(future_annual_precip, ee_poly, raster_path)
                future_precip_by_model_scenario_and_year[
                    model_id][scenario_id][year] = raster_path

    # rasterize watersheds onto a raster
    vector_mask_raster_path = os.path.join(
        workspace_dir, f'{vector_basename}.tif')
    geoprocessing.new_raster_from_base(
        raster_path, vector_mask_raster_path, gdal.GDT_Float64, [-1])
    geoprocessing.rasterize(
        vector_path, vector_mask_raster_path,
        option_list=["ATTRIBUTE=HYBAS_ID"])
    mask_array = gdal.OpenEx(vector_mask_raster_path).ReadAsArray()

    watershed_id_to_name = {
        4030050220: 'Amu Darya',
        2030065840: 'Atrek',
        4030050230: 'Balkhash-Alakol',
        3030001840: 'Ob',
        4030050240: 'Syr Darya',
        4030050210: 'Tarim',
        2030066850: 'Ural-Emba',
        }

    # loop through historic precip
    data_by_scenario = {}
    values_by_scenario_then_watershed = collections.defaultdict(
        lambda: collections.defaultdict(list))
    for model_id, raster_path in historic_precip_by_model.items():
        historic_array = gdal.OpenEx(raster_path, gdal.OF_RASTER).ReadAsArray()
        raster_info = geoprocessing.get_raster_info(raster_path)
        values_by_scenario = collections.defaultdict(list)
        for start_year, end_year, scenario_id in time_range_list[1:]:
            for year in range(start_year, end_year+1):
                future_array = gdal.OpenEx(
                    future_precip_by_model_scenario_and_year[
                        model_id][scenario_id][year],
                    gdal.OF_RASTER).ReadAsArray()
                percent_change = (1-future_array/historic_array)*100
                percent_change[mask_array == -1] = -1
                # at a particular scenario and year, break out by watershed
                values_by_scenario_then_watershed[scenario_id]['all'].append(
                    percent_change[mask_array != -1].mean())
                geoprocessing.numpy_array_to_raster(
                    percent_change, -1, raster_info['pixel_size'],
                    [raster_info['geotransform'][i] for i in (0, 3)],
                    raster_info['projection_wkt'],
                    os.path.join(
                        workspace_dir,
                        f'percent_change_{model_id}_{year}_{scenario_id}.tif'))
                for watershed_id, watershed_name in watershed_id_to_name.items():
                    values_by_scenario_then_watershed[scenario_id][watershed_name].append(
                        percent_change[mask_array == watershed_id].mean())

    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(14, 10))
    for index, (scenario_id, value_dict) in enumerate(
            sorted(values_by_scenario_then_watershed.items())):
        subplot_ax = axes[index]
        df = pandas.DataFrame.from_dict(value_dict)
        df.to_csv(f'{scenario_id}_precip_change.csv')
        boxplot = df.boxplot(
            ax=subplot_ax)
        boxplot.set_ylabel(r'% change in precip')
        boxplot.set_title(scenario_id)

    fig.suptitle("Percent increase of precipitation from historical mean 1985-2005 to future 2069-2078")
    fig.savefig(f'precip_change.png')

    """
        For Precip
        Drop ACCESS1-0 – it’s super unstable for some of the runs, so better to get rid of it for all of the runs
        For each model, calculate historic mean annual precip
        For each model, for each future period (for 50 years out, this would be the 10 year window around 2023+50 = 2073, so 2069-2078), calculate the difference between future annual rainfall and historic mean annual precip. Divide by historic mean annual precip to get % change. You should have 10 values for each model for each future period
        For each time period and future period, take all 200 of the % change values (20 models * 10 years) and make a box plot
    """


if __name__ == '__main__':
    main()
