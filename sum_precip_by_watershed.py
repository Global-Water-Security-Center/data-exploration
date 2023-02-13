"""Sum precipitation in a watershed(s) over a given time period.

Usage:
call like this:

python sum_precip_by_watershed.py europe_basins 2021-03-01 2022-03-01

Then it will query the AER dataset
http://h2o-sandbox1.aer-aws-nonprod.net/thredds/dodsC/era5/daily-summary.nc
for the normal_sum_tp_mm value over that time period and generates two things:
1) a geotiff named after the watershed file (in this case
   europe_basins_precip_sum_2021-03-01_2022-03-01.tif) passed in and the
   date range that's a summation of all the precip per pixel over the time
   period provided.
2) a CSV of TOTAL sum per time snapshot with the same naming convention.
"""
import hashlib
import argparse
import os

import ee
import geemap
import geopandas
import requests
import numpy

ERA5_RESOLUTION_M = 27830
ERA5_FILE_PREFIX = 'era5_monthly'
ERA5_TOTAL_PRECIP_BAND_NAME = 'total_precipitation'
ERA5_MEAN_AIR_TEMP_BAND_NAME = 'mean_2m_air_temperature'
ERA5_BANDS_TO_REPORT = [
    ERA5_TOTAL_PRECIP_BAND_NAME, ERA5_MEAN_AIR_TEMP_BAND_NAME]
CSV_BANDS_TO_DISPLAY = ['mean_precip (mm)', 'mean_2m_air_temp (C)']
CSV_BANDS_SCALAR_CONVERSION = [
    lambda precip_m: precip_m*1000, lambda K_val: K_val-273.15]
ANNUAL_CSV_BANDS_SCALAR_CONVERSION = [
    lambda precip_m: precip_m*1000, lambda K_val: K_val/12-273.15]


def main():
    parser = argparse.ArgumentParser(
        description='Sum precip by watershed in a time range.')
    parser.add_argument(
        'path_to_watersheds', help='Path to vector/shapefile of watersheds')
    parser.add_argument(
        'start_date', help='start date for summation (YYYY-MM-DD) format')
    parser.add_argument(
        'end_date', help='start date for summation (YYYY-MM-DD) format')
    parser.add_argument(
        '--authenticate', action='store_true',
        help='Pass this flag if you need to reauthenticate with GEE')
    args = parser.parse_args()

    if args.authenticate:
        ee.Authenticate()
        return
    ee.Initialize()

    # convert to GEE polygon
    gp_poly = geopandas.read_file(args.path_to_watersheds).to_crs('EPSG:4326')
    unique_id = hashlib.md5((
        args.path_to_watersheds+args.start_date+args.end_date).encode(
        'utf-8')).hexdigest()
    local_shapefile_path = f'_local_ok_to_delete_{unique_id}.json'
    gp_poly.to_file(local_shapefile_path)
    gp_poly = None
    ee_poly = geemap.geojson_to_ee(local_shapefile_path)
    os.remove(local_shapefile_path)

    era5_monthly_collection = ee.ImageCollection("ECMWF/ERA5/MONTHLY")
    era5_monthly_collection = era5_monthly_collection.filterDate(
                    args.start_date, args.end_date)
    era5_monthly_collection = era5_monthly_collection.select(
        ERA5_BANDS_TO_REPORT)
    poly_mask = ee.Image.constant(1).clip(ee_poly).mask()

    def clip_and_sum(image, list_so_far):
        list_so_far = ee.List(list_so_far)
        reduced_dict = image.clip(ee_poly).mask(
            poly_mask).reduceRegion(**{
                'geometry': ee_poly,
                'reducer': ee.Reducer.mean()
            })
        list_so_far = list_so_far.add(
            [image.get(key) for key in ['year', 'month']] +
            [reduced_dict.get(key) for key in ERA5_BANDS_TO_REPORT])

        return list_so_far
    mean_per_band = era5_monthly_collection.iterate(
        clip_and_sum, ee.List([]))
    print()

    vector_basename = os.path.splitext(
        os.path.basename(args.path_to_watersheds))[0]
    target_base = f"{vector_basename}_monthly_precip_temp_mean_{args.start_date}_{args.end_date}"
    target_table_path = f"{target_base}.csv"
    print(f'generating summary table to {target_table_path}')
    with open(target_table_path, 'w') as table_file:
        table_file.write('date,' + ','.join(CSV_BANDS_TO_DISPLAY) + '\n')
        for payload in mean_per_band.getInfo():
            table_file.write(
                f'{payload[0]}-{payload[1]:02d},' +
                ','.join([str(_conv(x)) for x, _conv in
                          zip(payload[2:], CSV_BANDS_SCALAR_CONVERSION)]) +
                '\n')

    era5_monthly_precp_collection = era5_monthly_collection.select(
        ERA5_TOTAL_PRECIP_BAND_NAME).toBands()
    era5_precip_sum = era5_monthly_precp_collection.reduce('sum').clip(
        ee_poly).mask(poly_mask).multiply(1000)
    url = era5_precip_sum.getDownloadUrl({
        'region': ee_poly.geometry().bounds(),
        'scale': ERA5_RESOLUTION_M,
        'format': 'GEO_TIFF'
    })
    response = requests.get(url)
    precip_path = f"{vector_basename}_precip_mm_sum_{args.start_date}_{args.end_date}.tif"
    print(f'calculate total precip sum to {precip_path}')
    with open(precip_path, 'wb') as fd:
        fd.write(response.content)

    era5_monthly_temp_collection = era5_monthly_collection.select(
        ERA5_MEAN_AIR_TEMP_BAND_NAME).toBands()
    era5_temp_mean = era5_monthly_temp_collection.reduce('mean').clip(
        ee_poly).mask(poly_mask).subtract(273.15)
    url = era5_temp_mean.getDownloadUrl({
        'region': ee_poly.geometry().bounds(),
        'scale': ERA5_RESOLUTION_M,
        'format': 'GEO_TIFF'
    })
    response = requests.get(url)
    temp_path = f"{vector_basename}_temp_C_monthly_mean_{args.start_date}_{args.end_date}.tif"
    print(f'calculate mean temp to {temp_path}')
    with open(temp_path, 'wb') as fd:
        fd.write(response.content)

    # get annual mean of precip
    target_base = f"{vector_basename}_annual_precip_mean_{args.start_date}_{args.end_date}"
    target_table_path = f"{target_base}.csv"
    print(f'generating summary table to {target_table_path}')
    with open(target_table_path, 'w') as table_file:
        table_file.write('date,' + ','.join(CSV_BANDS_TO_DISPLAY) + '\n')
        previous_year = None
        running_sum = None
        for payload in mean_per_band.getInfo():
            year = payload[0]
            if previous_year != year:
                if previous_year is not None:
                    table_file.write(
                        f'{previous_year},' +
                        ','.join([str(_conv(x)) for x, _conv in
                                  zip(running_sum, ANNUAL_CSV_BANDS_SCALAR_CONVERSION)]) +
                        '\n')
                previous_year = year
                running_sum = numpy.zeros(len(ANNUAL_CSV_BANDS_SCALAR_CONVERSION))
            running_sum += payload[2:]


if __name__ == '__main__':
    main()
