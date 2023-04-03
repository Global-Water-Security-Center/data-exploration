"""See `python scriptname.py --help"""
import argparse
import calendar
import collections
import concurrent
import datetime
import hashlib
import os

import ee
import geemap
import geopandas
import numpy
import requests

ERA5_RESOLUTION_M = 11132
ERA5_FILE_PREFIX = 'era5_monthly'
ERA5_TOTAL_PRECIP_BAND_NAME = 'total_precipitation'
ERA5_AIR_TEMP_BAND_NAME = 'temperature_2m'
CSV_BANDS_TO_DISPLAY = ['mean_precip (mm)', 'mean_2m_air_temp (C)']
CSV_BANDS_SCALAR_CONVERSION = [
    lambda precip_m: precip_m*1000, lambda K_val: K_val-273.15]
ANNUAL_CSV_BANDS_SCALAR_CONVERSION = [
    lambda precip_m: precip_m*1000]


def build_monthly_ranges(start_date, end_date):
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')

    current_day = start_date
    date_range_list = []
    while current_day < end_date:
        current_year = current_day.year
        current_month = current_day.month
        last_day = datetime.datetime(
            year=current_year, month=current_month,
            day=calendar.monthrange(current_year, current_month)[1])
        last_day = min(last_day, end_date)
        date_range_list.append(
            (current_day.strftime('%Y-%m-%d'),
             last_day.strftime('%Y-%m-%d')))
        # this kicks it to next month
        current_day = last_day+datetime.timedelta(days=1)
    return date_range_list


def get_monthly_precip_temp_mean(path_to_ee_poly, start_date, end_date):
    ee_poly = geemap.geojson_to_ee(path_to_ee_poly)
    poly_mask = ee.Image.constant(1).clip(ee_poly).mask()

    era5_hourly_collection = ee.ImageCollection("ECMWF/ERA5/HOURLY")
    era5_hourly_precip_collection = era5_hourly_collection.select(
        ERA5_TOTAL_PRECIP_BAND_NAME)
    monthly_precip_sum = era5_hourly_precip_collection.filterDate(
        start_date, end_date).sum()

    era5_hourly_temp_collection = era5_hourly_collection.select(
        ERA5_AIR_TEMP_BAND_NAME)
    monthly_mean_temp = era5_hourly_temp_collection.filterDate(
        start_date, end_date).mean()

    def clip_and_mean(image):
        clipped_image = ee.Image(image).clip(ee_poly).mask(
            poly_mask)
        reduced_dict = clipped_image.reduceRegion(**{
                'geometry': ee_poly,
                'scale': ERA5_RESOLUTION_M,
                'reducer': ee.Reducer.mean()
            })
        return clipped_image, reduced_dict

    # reduce the images down to a single value per ee_poly via mean
    (clipped_reduced_monthly_precip_image,
     clipped_reduced_monthly_precip_mean) = clip_and_mean(monthly_precip_sum)
    (clipped_reduced_monthly_temp_image,
     clipped_reduced_monthly_temp_mean) = clip_and_mean(monthly_mean_temp)

    return (
        clipped_reduced_monthly_precip_image,
        clipped_reduced_monthly_temp_image,
        clipped_reduced_monthly_precip_mean.getInfo()[
            ERA5_TOTAL_PRECIP_BAND_NAME],
        clipped_reduced_monthly_temp_mean.getInfo()[
            ERA5_AIR_TEMP_BAND_NAME])


def main():
    parser = argparse.ArgumentParser(description=(
        'Given a region and a time period, create two tables (1)  monthly '
        'precip and mean temporature and (2)  showing annual '
        'rainfall, as well as two rasters (3) total precip sum in AOI and (4) '
        'overall monthly temperture mean in the AOI.'))
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

    monthly_date_range_list = build_monthly_ranges(
        args.start_date, args.end_date)
    print(monthly_date_range_list)

    if args.authenticate:
        ee.Authenticate()
        return
    ee.Initialize()

    gp_poly = geopandas.read_file(args.path_to_watersheds).to_crs('EPSG:4326')
    unique_id = hashlib.md5((
        args.path_to_watersheds+args.start_date+args.end_date).encode(
        'utf-8')).hexdigest()
    local_shapefile_path = f'_local_ok_to_delete_{unique_id}.json'
    gp_poly.to_file(local_shapefile_path)
    gp_poly = None

    monthly_mean_image_list = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for start_date, end_date in monthly_date_range_list:
            monthly_mean_image_list.append(
                executor.submit(
                    get_monthly_precip_temp_mean, local_shapefile_path,
                    start_date, end_date))
        monthly_mean_image_list = [x.result() for x in monthly_mean_image_list]

    vector_basename = os.path.splitext(
        os.path.basename(args.path_to_watersheds))[0]
    target_base = (
        f"{vector_basename}_monthly_precip_temp_mean_"
        f"{args.start_date}_{args.end_date}")
    target_table_path = f"{target_base}.csv"
    print(f'generating summary table to {target_table_path}')
    precip_by_year = collections.defaultdict(list)
    precip_image_list = []
    temp_image_list = []
    with open(target_table_path, 'w') as table_file:
        table_file.write('date,' + ','.join(CSV_BANDS_TO_DISPLAY) + '\n')
        for (precip_image, temp_image, precip_mean, temp_mean), date in zip(
                monthly_mean_image_list, monthly_date_range_list):
            precip_image_list.append(precip_image)
            temp_image_list.append(temp_image)
            print(temp_mean)
            year = date[0][:4]
            converted_precip, converted_temp = [
                _conv(x) for x, _conv in zip(
                    (precip_mean, temp_mean),
                    CSV_BANDS_SCALAR_CONVERSION)]
            precip_by_year[year].append(converted_precip)
            table_file.write(
                f'{date[0][:7]},{converted_precip},{converted_temp}\n')

    ee_poly = geemap.geojson_to_ee(local_shapefile_path)
    poly_mask = ee.Image.constant(1).clip(ee_poly).mask()
    os.remove(local_shapefile_path)

    era5_monthly_precp_collection = ee.ImageCollection(
        precip_image_list).toBands()
    print(era5_monthly_precp_collection.getInfo())
    era5_precip_sum = era5_monthly_precp_collection.reduce('sum').clip(
        ee_poly).mask(poly_mask).multiply(1000)
    url = era5_precip_sum.getDownloadUrl({
        'region': ee_poly.geometry().bounds(),
        'scale': ERA5_RESOLUTION_M,
        'format': 'GEO_TIFF'
    })
    response = requests.get(url)
    precip_path = (
        f"{vector_basename}_precip_mm_sum_"
        f"{args.start_date}_{args.end_date}.tif")
    print(f'calculate total precip sum to {precip_path}')
    with open(precip_path, 'wb') as fd:
        fd.write(response.content)

    era5_monthly_temp_collection = ee.ImageCollection(
        temp_image_list).toBands()
    era5_temp_mean = era5_monthly_temp_collection.reduce('mean').clip(
        ee_poly).mask(poly_mask).subtract(273.15)
    url = era5_temp_mean.getDownloadUrl({
        'region': ee_poly.geometry().bounds(),
        'scale': ERA5_RESOLUTION_M,
        'format': 'GEO_TIFF'
    })
    response = requests.get(url)
    temp_path = (
        f"{vector_basename}_temp_C_monthly_mean_"
        f"{args.start_date}_{args.end_date}.tif")
    print(f'calculate mean temp to {temp_path}')
    with open(temp_path, 'wb') as fd:
        fd.write(response.content)

    # get annual mean of precip
    target_base = (
        f"{vector_basename}_annual_precip_mean_"
        f"{args.start_date}_{args.end_date}")
    target_table_path = f"{target_base}.csv"
    print(f'generating summary table to {target_table_path}')
    with open(target_table_path, 'w') as table_file:
        table_file.write(f'date,yearly sum of {CSV_BANDS_TO_DISPLAY[0]}\n')
        total_sum = 0
        total_months = 0
        for year, precip_list in sorted(precip_by_year.items()):
            yearly_sum = numpy.sum(precip_list)
            yearly_months = len(precip_list)
            total_sum += yearly_sum
            total_months += yearly_months

            table_file.write(
                f'{year},{yearly_sum}\n')
        table_file.write(
            f'total annual mean (adjusted to 12 months),'
            f'{total_sum/total_months*12}\n')


if __name__ == '__main__':
    main()
