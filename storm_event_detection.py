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
import argparse
import datetime
import os

import ee
import geemap
import geopandas
import requests

ERA5_RESOLUTION_M = 27830
ERA5_TOTAL_PRECIP_BAND_NAME = 'total_precipitation'


def main():
    parser = argparse.ArgumentParser(description=(
        'Detect storm events in a 48 hour window using a threshold for '
        'precip. Result is a geotiff raster whose pixels show the count of '
        'detected rain events within a 48 hour period.'
        ))
    parser.add_argument(
        'path_to_watersheds', help='Path to vector/shapefile of watersheds')
    parser.add_argument(
        'start_date', help='start date for summation (YYYY-MM-DD) format')
    parser.add_argument(
        'end_date', help='start date for summation (YYYY-MM-DD) format')
    parser.add_argument(
        '--authenticate', action='store_true',
        help='Pass this flag if you need to reauthenticate with GEE')
    parser.add_argument(
        '--rain_event_threshold', default=0.1, type=float,
        help='amount of rain (mm) in a day to count as a rain event')
    args = parser.parse_args()

    if args.authenticate:
        ee.Authenticate()
        return
    ee.Initialize()

    # convert to GEE polygon
    gp_poly = geopandas.read_file(args.path_to_watersheds).to_crs('EPSG:4326')
    local_shapefile_path = '_local_ok_to_delete.shp'
    gp_poly.to_file(local_shapefile_path)
    gp_poly = None
    ee_poly = geemap.shp_to_ee(local_shapefile_path)

    start_day = datetime.datetime.strptime(args.start_date, '%Y-%m-%d')
    end_day = datetime.datetime.strptime(args.start_date, '%Y-%m-%d')

    start_day_offset = start_day + datetime.timedelta(days=1)
    end_day_offset = end_day + datetime.timedelta(days=1)

    era5_daily_collection = ee.ImageCollection("ECMWF/ERA5/DAILY")

    daily_precip_list = []
    for working_start_day, working_end_day in [
            (start_day, end_day), (start_day_offset, end_day_offset)]:
        era5_daily_collection = era5_daily_collection.filterDate(
            args.start_date, args.end_date)
        # convert to mm and divide by 2 for average
        era5_daily_precip = era5_daily_collection.select(
            ERA5_TOTAL_PRECIP_BAND_NAME).toBands().multiply(1000/2)
        daily_precip_list.append(era5_daily_precip)

    average_24_precip = daily_precip_list[0].add(
        daily_precip_list[1])

    era5_daily_precip = average_24_precip.where(
        average_24_precip.lt(args.rain_event_threshold), 0).where(
        average_24_precip.gte(args.rain_event_threshold), 1)

    poly_mask = ee.Image.constant(1).clip(ee_poly).mask()

    era5_precip_event_sum = era5_daily_precip.reduce('sum').clip(
        ee_poly).mask(poly_mask)
    url = era5_precip_event_sum.getDownloadUrl({
        'region': ee_poly.geometry().bounds(),
        'scale': ERA5_RESOLUTION_M,
        'format': 'GEO_TIFF'
    })
    response = requests.get(url)
    vector_basename = os.path.basename(os.path.splitext(args.path_to_watersheds)[0])
    precip_path = f'''{vector_basename}_48hr_avg_precip_events_{
        args.start_date}_{args.end_date}.tif'''
    print(f'calculate total precip event {precip_path}')
    with open(precip_path, 'wb') as fd:
        fd.write(response.content)


if __name__ == '__main__':
    main()
