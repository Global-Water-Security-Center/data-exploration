"""Utility to extract CIMP5 data from GEE."""
import argparse
import datetime
import concurrent
import collections
import os
import shutil
import tempfile

import ee
import geemap
import geopandas


DATASET_ID = 'NASA/NEX-GDDP'
DATASET_CRS = 'EPSG:4326'
DATASET_SCALE = 27830
SCENARIO_LIST = ['historical', 'rcp45', 'rcp85']
MODEL_LIST = ['ACCESS1-0', 'bcc-csm1-1', 'BNU-ESM', 'CanESM2', 'CCSM4', 'CESM1-BGC', 'CNRM-CM5', 'CSIRO-Mk3-6-0', 'GFDL-CM3', 'GFDL-ESM2G', 'GFDL-ESM2M', 'inmcm4', 'IPSL-CM5A-LR', 'IPSL-CM5A-MR', 'MIROC-ESM', 'MIROC-ESM-CHEM', 'MIROC5', 'MPI-ESM-LR', 'MPI-ESM-MR', 'MRI-CGCM3', 'NorESM1-M']


def _reduceregion(image):
    """Helper function to check for None on reduce regions."""

def main():
    """Entry point."""
    parser = argparse.ArgumentParser(
        description='Extract CIMP5 data from GEE.')
    parser.add_argument(
        'country_name', help='Path to vector/shapefile of watersheds')
    parser.add_argument('start_date', type=str, help='start date YYYY-MM-DD')
    parser.add_argument('end_date', type=str, help='end date YYYY-MM-DD')
    parser.add_argument(
        '--authenticate', action='store_true',
        help='Pass this flag if you need to reauthenticate with GEE')
    args = parser.parse_args()

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

    # countries.gpkg can be downloaded from https://github.com/tsamsonov/r-geo-course/blob/master/data/ne/countries.gpkg
    countries_vector = geopandas.read_file('base_data/countries.gpkg')
    country_shape = countries_vector[
        countries_vector.name == args.country_name]
    workspace_dir = tempfile.mkdtemp(prefix='_ok_to_delete_', dir='.')
    local_shapefile_path = os.path.join(
        workspace_dir, '_local_ok_to_delete.shp')

    country_shape.to_file(local_shapefile_path)
    country_shape = None
    countries_vector = None
    ee_poly = geemap.shp_to_ee(local_shapefile_path)

    band_names = ['tasmin', 'tasmax', 'pr']
    print(f'querying data for {n_days} days')

    base_dataset = ee.ImageCollection(DATASET_ID)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        value_by_year = []
        for year, date_list in date_by_year_lists.items():
            print(f'setting up year {year}')
            year_dataset = base_dataset.filter(ee.Filter.date(f'{year}-01-01'))
            model_list = [
                x['properties']['model']
                for x in year_dataset.getInfo()['features']]
            print(model_list)
            value_by_date = ee.List([])  # each entry is a date this year
            for date in date_list:
                if year < 2006:
                    scenario_list = ['historical']
                else:
                    scenario_list = ['rcp45', 'rcp85']
                value_by_scenario = ee.Dictionary({})
                for scenario_id in scenario_list:
                    value_by_model = ee.Dictionary({})
                    for model_id in model_list:
                        asset_id = f'NASA/NEX-GDDP/{scenario_id}_{model_id}_{date.replace("-", "")}'
                        asset = ee.Image(asset_id)

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
                value_by_date = value_by_date.add(
                    (value_by_scenario, date))
            value_by_year.append(
                (executor.submit(lambda x: x.getInfo(), value_by_date), year))

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
        for value_by_date_task, year in value_by_year:
            print(f'waiting for year {year} to process')
            value_by_date = value_by_date_task.result()
            for value_by_scenario, date in value_by_date:
                csv_table.write(f'{date},')
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
    shutil.rmtree(workspace_dir)
    print('done!')


if __name__ == '__main__':
    main()
