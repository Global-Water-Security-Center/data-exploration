"""Utility to extract CIMP5 data from GEE."""
import argparse
import datetime
import concurrent
import collections

import ee
import geemap
from rasterio.transform import Affine
import geopandas
import numpy
import pandas
import rasterio
import xarray

COUNTRY_NAME = 'Kenya'
START_DATE = '2012-01-01'
END_DATE = '2022-03-01'


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
    local_shapefile_path = '_local_ok_to_delete.shp'
    country_shape.to_file(local_shapefile_path)
    country_shape = None
    countries_vector = None
    ee_poly = geemap.shp_to_ee(local_shapefile_path)

    band_names = ['tasmin', 'tasmax', 'pr']
    reduction_types = ['min', 'max', 'mean']

    print(f'querying data for {n_days} days')

    with concurrent.futures.ThreadPoolExecutor() as executor:
        query_by_year_list = []
        for year, date_list in date_by_year_lists.items():
            print(f'setting up year {year}')
            value_by_date = ee.List([])
            for date in date_list:
                reduction_collection = None
                for reduction_id in reduction_types:
                    gddp_dataset = ee.ImageCollection('NASA/NEX-GDDP').filter(
                        ee.Filter.date(date)).reduce(reduction_id)
                    reduced_value = gddp_dataset.reduceRegion(**{
                        'reducer': 'mean',
                        'geometry': ee_poly
                        })
                    if not reduction_collection:
                        reduction_collection = reduced_value
                    else:
                        reduction_collection = reduction_collection.combine(
                            reduced_value)
                value_by_date = value_by_date.add(reduction_collection)
            query_by_year_list.append(
                (executor.submit(lambda x: x.getInfo(), value_by_date), year))
    table_path = (
        f'CIMP5_{args.country_name}_{args.start_date}_{args.end_date}.csv')
    print(f'saving to {table_path}')
    print(
        f'waiting for GEE to compute, if the date range is large this '
        f'may take a while')
    with open(table_path, 'w') as csv_table:
        header_fields = [
            f'{band_name}_{reduction_type}'
            for band_name in band_names
            for reduction_type in reduction_types]
        csv_table.write('date,'+','.join(header_fields)+'\n')
        for future, year in query_by_year_list:
            print(f'waiting for year {year} to process')
            value_by_date = future.result()
            for values, date in zip(value_by_date, date_by_year_lists[year]):
                csv_table.write(f'{date},')
                for field_name in header_fields:
                    csv_table.write(f'{values[field_name]},')
                csv_table.write('\n')
    print('done!')

if __name__ == '__main__':
    main()
