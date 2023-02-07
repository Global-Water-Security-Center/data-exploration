"""Utility to extract CIMP5 data from GEE."""
import argparse
import datetime
import concurrent
import logging
import sys
import os
import pickle
import shutil
import tempfile
import time
import threading

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


def process_date_chunk(value_by_date_dict_ee, cache_path, cache_path_lock):
    """Process value by date dict asychronously and then save to cache."""
    value_by_date = value_by_date_dict_ee.getInfo()
    with cache_path_lock:
        try:
            # result_by_month indexes by YYYY-MM into a dictonary of dates to results
            with open(cache_path, 'rb') as result_by_date_file:
                cached_result_by_date = pickle.load(result_by_date_file)

        except Exception:
            cached_result_by_date = dict()
        value_by_date = value_by_date | cached_result_by_date
        with open(cache_path, 'wb') as result_by_date_file:
            pickle.dump(value_by_date, result_by_date_file)


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(
        description='Extract CIMP5 data from GEE.')
    parser.add_argument(
        'aoi_vector_path', help='Path to vector/shapefile of area of interest')
    parser.add_argument('--aggregate_by_field', help=(
        'If provided, this aggregates results by the unique values found in '
        'the field in `aoi_vector_path`'))
    parser.add_argument('start_date', type=str, help='start date YYYY-MM-DD')
    parser.add_argument('end_date', type=str, help='end date YYYY-MM-DD')
    parser.add_argument(
        '--authenticate', action='store_true',
        help='Pass this flag if you need to reauthenticate with GEE')
    args = parser.parse_args()
    aoi_vector = geopandas.read_file(args.aoi_vector_path)
    unique_id_set = [(slice(-1), None)]  # default to all features
    if args.aggregate_by_field:
        if args.aggregate_by_field not in aoi_vector:
            raise ValueError(
                f'`{args.aggregate_by_field}` was passed in as a query field '
                f'but was not found in `{args.aoi_vector_path}`, but instead '
                f'these fields are present: {", ".join(aoi_vector.columns)}')
        unique_id_set = [
            (aoi_vector[args.aggregate_by_field] == unique_id, unique_id)
            for unique_id in set(aoi_vector[args.aggregate_by_field])]

    start_day = datetime.datetime.strptime(args.start_date, '%Y-%m-%d')
    end_day = datetime.datetime.strptime(args.end_date, '%Y-%m-%d')
    date_list = [
        (start_day + datetime.timedelta(days=delta_day)).strftime('%Y-%m-%d')
        for delta_day in range((end_day-start_day).days+1)]

    if args.authenticate:
        ee.Authenticate()
        return
    ee.Initialize()

    vector_basename = os.path.basename(os.path.splitext(
        args.aoi_vector_path)[0])
    base_dataset = ee.ImageCollection(DATASET_ID)

    #LOGGER.info('querying which models are available by date')
    models_by_date = fetch_models_by_date(base_dataset, date_list)

    result_path_by_unique_id = dict()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for unique_id_index, unique_id_value in unique_id_set:
            # create tag that's either the vector basename, or if filtering on
            # a field is requested, the basename, field, and field value
            unique_id = (
                vector_basename if args.aggregate_by_field is None else
                f'{vector_basename}_{args.aggregate_by_field}_{unique_id_value}')
            result_by_date_path = f'{unique_id}_result_by_date.dat'
            result_path_by_unique_id[unique_id] = result_by_date_path
            try:
                # result_by_date indexes by YYYY-MM into a dictonary of dates to results
                with open(result_by_date_path, 'rb') as result_by_date_file:
                    result_by_date = pickle.load(result_by_date_file)
                    cached_result_by_date = result_by_date.keys()
                    result_by_date = None
                    LOGGER.debug(f'there are {len(cached_result_by_date)} cached values')
            except Exception:
                LOGGER.debug('there are 0 cached values')
                cached_result_by_date = set()
            result_by_month_lock = threading.Lock()

            # save to shapefile and load into EE vector
            filtered_aoi = aoi_vector[unique_id_index]
            local_shapefile_path = f'_local_ok_to_delete_{unique_id}.gpkg'
            filtered_aoi = filtered_aoi.to_crs('EPSG:4326')
            filtered_aoi.to_file(local_shapefile_path)
            filtered_aoi = None
            ee_poly = geemap.gpkg_to_ee(local_shapefile_path)

            base_dataset = ee.ImageCollection(DATASET_ID)
            last_year_mo = None
            value_by_date = ee.Dictionary({})
            for date in date_list:
                if date in cached_result_by_date:
                    continue
                year_mo = date[:7]
                if year_mo != last_year_mo:
                    if last_year_mo is not None:
                        executor.submit(
                            process_date_chunk, value_by_date,
                            result_by_date_path, result_by_month_lock)
                        value_by_date = ee.Dictionary({})
                    last_year_mo = year_mo
                    LOGGER.debug(f'processing {year_mo} for {unique_id}')
                year = int(date[:4])
                if year < 2006:
                    scenario_list = ['historical']
                else:
                    scenario_list = ['rcp45', 'rcp85']
                model_list = models_by_date[date]
                date_dataset = base_dataset.filter(ee.Filter.date(date))
                date_dataset = base_dataset
                value_by_scenario = ee.Dictionary({})
                for scenario_id in scenario_list:
                    scenario_dataset = date_dataset.filter(
                        ee.Filter.eq('scenario', scenario_id))
                    value_by_model = ee.Dictionary({})
                    for model_id in model_list:
                        asset = scenario_dataset.filter(
                            ee.Filter.eq('model', model_id)).first()
                        reduced_value = asset.reduceRegion(**{
                            'reducer': 'mean',
                            'geometry': ee_poly,
                            'crs': DATASET_CRS,
                            'scale': DATASET_SCALE,
                            })
                        value_by_model = value_by_model.set(
                            model_id, reduced_value)
                    value_by_scenario = value_by_scenario.set(
                        scenario_id, value_by_model)
                value_by_date = value_by_date.set(date, value_by_scenario)
            executor.submit(
                process_date_chunk, value_by_date, result_by_date_path,
                result_by_month_lock)
            value_by_date = None

    for unique_id, result_path in result_path_by_unique_id.items():
        with open(result_path, 'rb') as result_file:
            value_by_date = pickle.load(result_file)
            table_path = (f'CIMP5_{unique_id}.csv')
            LOGGER.info(f'saving to {table_path}')
            with open(table_path, 'w') as csv_table:
                header_fields = [
                    f'{band_id}_{model_id}'
                    for band_id in BAND_NAMES
                    for model_id in MODEL_LIST]
                csv_table.write('date,')
                for scenario_id in SCENARIO_LIST:
                    csv_table.write(f'_{scenario_id},'.join(header_fields)+f'_{scenario_id},')
                csv_table.write('\n')
                last_year_mo = None
                for date in sorted(value_by_date):
                    year_mo = date[:7]
                    if year_mo != last_year_mo:
                        LOGGER.info(f'processing {year_mo}')
                        last_year_mo = year_mo

                    value_by_scenario = value_by_date[date].result()
                    csv_table.write(f'{date},')
                    for scenario_id in SCENARIO_LIST:
                        if scenario_id not in value_by_scenario:
                            csv_table.write(','.join(['n/a']*len(MODEL_LIST)*len(BAND_NAMES))+',')
                            continue
                        value_by_model = value_by_scenario[scenario_id]
                        for model_id in MODEL_LIST:
                            if model_id not in value_by_model:
                                csv_table.write(','.join(['n/a']*len(BAND_NAMES))+',')
                                continue
                            band_values = value_by_model[model_id]
                            for band_id in BAND_NAMES:
                                if band_id in band_values:
                                    csv_table.write(f'{band_values[band_id]},')
                                else:
                                    csv_table.write(f'n/a,')
                    csv_table.write('\n')
            LOGGER.info('done!')

    aoi_vector = None
    shutil.rmtree(workspace_dir)


if __name__ == '__main__':
    main()
