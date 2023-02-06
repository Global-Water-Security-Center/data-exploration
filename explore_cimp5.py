"""Utility to extract CIMP5 data from GEE."""
import argparse
import datetime
import concurrent
import collections
import os
import pickle
import shutil
import tempfile
import time

import ee
import geemap
import geopandas


DATASET_ID = 'NASA/NEX-GDDP'
DATASET_CRS = 'EPSG:4326'
DATASET_SCALE = 27830
SCENARIO_LIST = ['historical', 'rcp45', 'rcp85']
MODEL_LIST = ['ACCESS1-0', 'bcc-csm1-1', 'BNU-ESM', 'CanESM2', 'CCSM4', 'CESM1-BGC', 'CNRM-CM5', 'CSIRO-Mk3-6-0', 'GFDL-CM3', 'GFDL-ESM2G', 'GFDL-ESM2M', 'inmcm4', 'IPSL-CM5A-LR', 'IPSL-CM5A-MR', 'MIROC-ESM', 'MIROC-ESM-CHEM', 'MIROC5', 'MPI-ESM-LR', 'MPI-ESM-MR', 'MRI-CGCM3', 'NorESM1-M']
MODELS_BY_DATE_CACHEFILE = '_cimp5_models_by_date.dat'
QUOTA_READS_PER_MINUTE = 3000


def fetch_models_by_date(base_dataset, date_by_year_lists):
    try:
        with open(MODELS_BY_DATE_CACHEFILE, 'rb') as models_by_date_file:
            models_by_date = pickle.load(models_by_date_file)
    except Exception:
        models_by_date = dict()

    total_dates = sum([
        len(date_list) for date_list in date_by_year_lists.values()])
    print(
        f'it will take {1/QUOTA_READS_PER_MINUTE*60*total_dates:.2f}s to '
        f'query {total_dates} dates')

    with concurrent.futures.ThreadPoolExecutor() as executor:
        model_by_date_executor = {}
        for year, date_list in date_by_year_lists.items():
            for date in date_list:
                if date not in models_by_date:
                    model_by_date_executor[date] = executor.submit(
                        lambda date_dataset: [
                            x['properties']['model']
                            for x in date_dataset.getInfo()['features']],
                        base_dataset.filter(ee.Filter.date(date)))
                    time.sleep(1/QUOTA_READS_PER_MINUTE*60)

        # evaluate available models
        models_by_date = models_by_date | {
            date: future.result()
            for date, future in model_by_date_executor.items()}
    with open(MODELS_BY_DATE_CACHEFILE, 'wb') as models_by_date_file:
        pickle.dump(models_by_date, models_by_date_file)
    return models_by_date


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
    date_by_year_lists = collections.defaultdict(list)
    n_days = 0
    for delta_day in range((end_day-start_day).days+1):
        current_date = (
            start_day + datetime.timedelta(days=delta_day)).strftime('%Y-%m-%d')
        year = int(current_date[:4])
        date_by_year_lists[year].append(current_date)
        n_days += 1

    if args.authenticate:
        ee.Authenticate()
        return
    ee.Initialize()

    vector_basename = os.path.basename(os.path.splitext(
        args.aoi_vector_path)[0])
    workspace_dir = tempfile.mkdtemp(prefix='_ok_to_delete_', dir='.')
    base_dataset = ee.ImageCollection(DATASET_ID)

    models_by_date = fetch_models_by_date(base_dataset, date_by_year_lists)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        print(models_by_date)
        return
        for unique_id_index, unique_id_value in unique_id_set:
            # create tag that's either the vector basename, or if filtering on
            # a field is requested, the basename, field, and field value
            unique_id_tag = (
                vector_basename if args.aggregate_by_field is None else
                f'{vector_basename}_{args.aggregate_by_field}_{unique_id_value}')

            # save to shapefile and load into EE vector
            filtered_aoi = aoi_vector[unique_id_index]
            local_shapefile_path = os.path.join(
                workspace_dir, f'_local_ok_to_delete_{unique_id_tag}.shp')
            filtered_aoi = filtered_aoi.to_crs('EPSG:4326')
            filtered_aoi.to_file(local_shapefile_path)
            filtered_aoi = None
            ee_poly = geemap.shp_to_ee(local_shapefile_path)

            band_names = ['tasmin', 'tasmax', 'pr']
            print(f'querying data for {n_days} days')

            base_dataset = ee.ImageCollection(DATASET_ID)
            value_by_year = []
            for year, date_list in date_by_year_lists.items():
                models_by_date = []
                for date in date_list:
                    models_by_date.append(executor.submit(lambda date_dataset: [
                        x['properties']['model']
                        for x in date_dataset.getInfo()['features']],
                        base_dataset.filter(ee.Filter.date(date))))
                print(f'setting up year {year}')
                value_by_date = []  # each entry is a date this year
                for date in date_list:
                    if year < 2006:
                        scenario_list = ['historical']
                    else:
                        scenario_list = ['rcp45', 'rcp85']
                    model_list = models_by_date.pop(0).result()
                    date_dataset = base_dataset.filter(ee.Filter.date(date))
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
                    value_by_date.append((executor.submit(
                        lambda x: x.getInfo(), value_by_scenario), date))
                value_by_year.append((value_by_date, year))

            table_path = (
                f'CIMP5_{args.country_name}_{args.start_date}_{args.end_date}.csv')
            print(f'saving to {table_path}')
            print(
                f'waiting for GEE to compute, if the date range is large this '
                f'may take a while')
            with open(table_path, 'w') as csv_table:
                header_fields = [
                    f'{band_id}_{model_id}'
                    for band_id in band_names
                    for model_id in MODEL_LIST]
                csv_table.write('date,')
                for scenario_id in SCENARIO_LIST:
                    csv_table.write(f'_{scenario_id},'.join(header_fields)+f'_{scenario_id},')
                csv_table.write('\n')
                for value_by_date, year in value_by_year:
                    print(f'waiting for year {year} to process')
                    for value_by_scenario_task, date in value_by_date:
                        value_by_scenario = value_by_scenario_task.result()
                        csv_table.write(f'{date},')
                        print(date)
                        for scenario_id in SCENARIO_LIST:
                            if scenario_id not in value_by_scenario:
                                csv_table.write(','.join(['n/a']*len(MODEL_LIST)*len(band_names))+',')
                                continue
                            value_by_model = value_by_scenario[scenario_id]
                            for model_id in MODEL_LIST:
                                if model_id not in value_by_model:
                                    csv_table.write(','.join(['n/a']*len(band_names))+',')
                                    continue
                                band_values = value_by_model[model_id]
                                for band_id in band_names:
                                    if band_id in band_values:
                                        csv_table.write(f'{band_values[band_id]},')
                                    else:
                                        csv_table.write(f'n/a,')
                        csv_table.write('\n')
            print('done!')

    aoi_vector = None
    shutil.rmtree(workspace_dir)


if __name__ == '__main__':
    main()
