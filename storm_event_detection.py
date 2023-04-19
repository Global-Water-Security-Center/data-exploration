"""See `python scriptname.py --help"""
import argparse
import datetime
import logging
import multiprocessing
import os
import shutil
import sys
import time

from osgeo import gdal
from utils import build_monthly_ranges
from utils import daterange
from ecoshard import geoprocessing
from ecoshard import taskgraph
import numpy
import requests

from fetch_data import fetch_data


logging.basicConfig(
    level=logging.WARNING,
    stream=sys.stdout,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
LOGGER.setLevel(logging.DEBUG)
logging.getLogger('fetch_data').setLevel(logging.INFO)

ERA5_RESOLUTION_M = 27830
ERA5_TOTAL_PRECIP_BAND_NAME = 'total_precipitation'

DATASET_ID = 'aer_era5_daily'
VARIABLE_ID = 'sum_tp_mm'


def _download_url(url, target_path):
    response = requests.get(url)
    with open(target_path, 'wb') as fd:
        fd.write(response.content)
    return target_path


def _process_month(
        clip_path_band_list, nodata, rain_event_threshold,
        target_monthly_precip_path):
    def _process_month_op(*precip_array):
        result = numpy.zeros(precip_array[0].shape, dtype=int)
        valid_mask = numpy.zeros(result.shape, dtype=bool)
        if len(precip_array) >= 2:
            for precip_a, precip_b in zip(
                    precip_array[:-1], precip_array[1:]):
                # convert to mm and average by /2 for 48 hr period
                local_mask = (precip_a+precip_b)/2 >= rain_event_threshold
                result += local_mask
                valid_mask |= local_mask
        else:
            local_mask = precip_array[0] >= rain_event_threshold
            result += local_mask
            valid_mask |= local_mask
        result[~valid_mask] = nodata
        return result

    LOGGER.info(f'about to process {target_monthly_precip_path}')
    geoprocessing.raster_calculator(
        clip_path_band_list,
        _process_month_op,
        target_monthly_precip_path, gdal.GDT_Int32, nodata)


def main():
    start_time = time.time()
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
        '--rain_event_threshold', default=0.1, type=float,
        help='amount of rain (mm) in a day to count as a rain event')
    args = parser.parse_args()

    vector_basename = os.path.basename(
        os.path.splitext(args.path_to_watersheds)[0])
    project_basename = (
        f'''{vector_basename}_{args.start_date}_{args.end_date}''')
    workspace_dir = f'workspace_{project_basename}'

    task_graph = taskgraph.TaskGraph(
        workspace_dir, multiprocessing.cpu_count(), 15.0)

    monthly_date_range_list = build_monthly_ranges(
        args.start_date, args.end_date)
    LOGGER.debug(monthly_date_range_list)

    clip_dir = os.path.join(workspace_dir, 'clip')
    os.makedirs(clip_dir, exist_ok=True)
    mask_nodata = -1
    monthly_precip_path_list = []
    for start_date, end_date in monthly_date_range_list:
        start_day = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        end_day = datetime.datetime.strptime(end_date, '%Y-%m-%d')

        clip_path_band_list = []
        clip_task_list = []
        for date in daterange(start_day, end_day):
            date_str = date.strftime('%Y-%m-%d')
            clip_path = os.path.join(
                clip_dir, f'clip_{DATASET_ID}_{VARIABLE_ID}_{date_str}')

            fetch_and_clip_task = task_graph.add_task(
                func=fetch_data.fetch_and_clip,
                args=(
                    DATASET_ID, VARIABLE_ID, date_str,
                    (ERA5_RESOLUTION_M, -ERA5_RESOLUTION_M),
                    args.path_to_watersheds, clip_path),
                kwargs={'all_touched': True, 'target_mask_value': mask_nodata},
                target_path_list=[clip_path],
                task_name=f'fetch and clip {clip_path}')
            clip_path_band_list.append((clip_path, 1))
            clip_task_list.append(fetch_and_clip_task)

        monthly_precip_path = os.path.join(
            workspace_dir, f'''{vector_basename}_48hr_avg_precip_events_{
            start_date}_{end_date}.tif''')
        monthly_precip_task = task_graph.add_task(
            func=_process_month,
            args=(
                clip_path_band_list, mask_nodata, args.rain_event_threshold,
                monthly_precip_path),
            target_path_list=[monthly_precip_path],
            dependent_task_list=clip_task_list,
            task_name=f'process month {monthly_precip_path}')
        monthly_precip_path_list.append(
            (monthly_precip_task, start_date[:7], monthly_precip_path))

    precip_over_all_time_path = os.path.join(
        workspace_dir, f'''overall_{project_basename}_48hr_avg_precip_events_{
        args.start_date}_{args.end_date}.tif''')
    with open(
            f'{os.path.splitext(precip_over_all_time_path)[0]}.csv', 'w') as \
            csv_table:
        csv_table.write('year-month,number of storm events in region\n')
        running_sum = None
        for monthly_precip_task, month_date, raster_path in \
                monthly_precip_path_list:
            monthly_precip_task.join()
            r = gdal.OpenEx(raster_path)
            b = r.GetRasterBand(1)
            array = b.ReadAsArray()
            nodata = b.GetNoDataValue()
            b = None
            r = None
            csv_table.write(
                f'{month_date},{numpy.sum(array[array != nodata])}\n')
            if running_sum is None:
                running_sum = array
                valid_mask = array != nodata
            else:
                local_valid_mask = array != nodata
                running_sum[local_valid_mask] += array[local_valid_mask]
                valid_mask |= local_valid_mask
            array = None

    # write the final overall raster
    shutil.copyfile(raster_path, precip_over_all_time_path)
    r = gdal.OpenEx(precip_over_all_time_path, gdal.OF_RASTER | gdal.GA_Update)
    b = r.GetRasterBand(1)
    running_sum[~valid_mask] = mask_nodata
    b.WriteArray(running_sum)
    b = None
    r = None
    print(
        f'all done ({time.time()-start_time:.2f}s), results in '
        f'{workspace_dir}')

    task_graph.join()
    task_graph.close()


if __name__ == '__main__':
    main()
