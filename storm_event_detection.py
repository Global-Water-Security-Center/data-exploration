"""See `python scriptname.py --help"""
import argparse
import concurrent
import datetime
import os
import shutil

from osgeo import gdal
from utils import build_monthly_ranges
import ee
import geemap
import geopandas
import numpy
import requests

from . import fetch_data

ERA5_RESOLUTION_M = 27830
ERA5_TOTAL_PRECIP_BAND_NAME = 'total_precipitation'


def _download_url(url, target_path):
    response = requests.get(url)
    with open(target_path, 'wb') as fd:
        fd.write(response.content)
    return target_path


def main():
    parser = argparse.ArgumentParser(description=(
        'Detect storm events in a 48 hour window using a threshold for '
        'precip. Result is located in a directory called '
        '`workspace_{vector name}` and contains rasters for each month over '
        'the time period showing nubmer of precip events per pixel, a raster '
        'prefixed with "overall_" showing the overall storm event per pixel, '
        'and a CSV table prefixed with the vector basename and time range '
        'showing number of events in the region per month.'
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
    vector_basename = os.path.basename(
        os.path.splitext(args.path_to_watersheds)[0])
    project_basename = (
        f'''{vector_basename}_{args.start_date}_{args.end_date}''')
    workspace_dir = f'workspace_{project_basename}'
    os.makedirs(workspace_dir, exist_ok=True)
    gp_poly = geopandas.read_file(args.path_to_watersheds).to_crs('EPSG:4326')
    local_shapefile_path = os.path.join(
        workspace_dir, '_local_ok_to_delete.shp')
    gp_poly.to_file(local_shapefile_path)
    gp_poly = None
    ee_poly = geemap.shp_to_ee(local_shapefile_path)
    os.remove(local_shapefile_path)

    monthly_date_range_list = build_monthly_ranges(
        args.start_date, args.end_date)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        raster_download_list = []
        for start_date, end_date in monthly_date_range_list:
            start_day = datetime.datetime.strptime(start_date, '%Y-%m-%d')
            end_day = datetime.datetime.strptime(end_date, '%Y-%m-%d')

            start_day_offset = start_day + datetime.timedelta(days=1)
            end_day_offset = end_day + datetime.timedelta(days=1)

            daily_precip_list = []
            for working_start_day, working_end_day in [
                    (start_day, end_day), (start_day_offset, end_day_offset)]:
                era5_daily_collection = ee.ImageCollection("ECMWF/ERA5/DAILY")
                working_start_day_str = working_start_day.strftime('%Y-%m-%d')
                working_end_day_str = working_end_day.strftime('%Y-%m-%d')
                if working_start_day_str != working_end_day_str:
                    era5_daily_collection = era5_daily_collection.filterDate(
                        working_start_day_str,
                        working_end_day_str)
                else:
                    # just one day
                    era5_daily_collection = era5_daily_collection.filterDate(
                        working_start_day_str)
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
            precip_path = os.path.join(
                workspace_dir, f'''{vector_basename}_48hr_avg_precip_events_{
                start_date}_{end_date}.tif''')
            raster_download_list.append(
                (start_date[:7],
                 executor.submit(_download_url, url, precip_path)))

        with open(os.path.join(
                workspace_dir, f'{os.path.basename(precip_path)}.csv'),
                'w') as csv_table:
            csv_table.write('year-month,number of storm events in region\n')
            running_sum = None
            for month_date, download_future in raster_download_list:
                print(f'download {month_date}')
                raster_path = download_future.result()
                r = gdal.OpenEx(raster_path)
                b = r.GetRasterBand(1)
                array = b.ReadAsArray()
                nodata = b.GetNoDataValue()
                csv_table.write(
                    f'{month_date},{numpy.sum(array[array != nodata])}\n')

                if running_sum is None:
                    running_sum = array
                    valid_mask = array != nodata
                else:
                    running_sum += array
                    valid_mask |= array != nodata

        # write the final overall raster
        precip_over_all_time_path = os.path.join(
            workspace_dir, f'''overall_{project_basename}_48hr_avg_precip_events_{
            args.start_date}_{args.end_date}.tif''')
        shutil.copyfile(raster_path, precip_over_all_time_path)
        r = gdal.OpenEx(
            precip_over_all_time_path, gdal.OF_RASTER | gdal.GA_Update)
        b = r.GetRasterBand(1)
        # mask out nodata
        b.WriteArray(running_sum)
        b = None
        r = None
    print(f'all done, results in {workspace_dir}')


if __name__ == '__main__':
    main()
