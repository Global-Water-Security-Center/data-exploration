"""See `python scriptname.py --help"""
import argparse
import collections
import multiprocessing
import datetime
import glob
import logging
import os
import sys

from ecoshard import geoprocessing
from ecoshard import taskgraph
from osgeo import gdal
from utils import build_monthly_ranges
from utils import daterange
import numpy

from fetch_data import fetch_data

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
LOGGER.setLevel(logging.DEBUG)
logging.getLogger('fetch_data').setLevel(logging.INFO)

DATASET_ID = 'aer_era5_daily'
VARIABLE_ID_LIST = ['sum_tp_mm', 'mean_t2m_c']
MASK_NODATA = -9999

ERA5_RESOLUTION_M = 11132
CSV_BANDS_TO_DISPLAY = ['mean_precip (mm)', 'mean_2m_air_temp (C)']


def _sum_op(*array_list):
    LOGGER.debug(len(array_list))
    valid_mask = array_list[0] != MASK_NODATA
    LOGGER.debug(valid_mask.shape)
    result = numpy.sum(array_list, axis=0)
    LOGGER.debug(result.shape)
    result[~valid_mask] = MASK_NODATA
    return result


def _mean_op(*array_list):
    valid_mask = array_list[0] != MASK_NODATA
    result = numpy.sum(array_list, axis=0) / len(array_list)
    result[~valid_mask] = MASK_NODATA
    return result


def mean_of_raster_op(raster_path):
    raster_array = gdal.OpenEx(raster_path, gdal.OF_RASTER).ReadAsArray()
    valid_array = raster_array[raster_array != MASK_NODATA]
    if valid_array.size > 0:
        return numpy.mean(valid_array)
    return -9999


def main():
    parser = argparse.ArgumentParser(description=(
        'Given a region and a time period create four tables (1) monthly '
        'precip and mean temperature and (2) annual '
        'rainfall, (3) monthly normal temp, and (4) monthly normal precip '
        'over the query time period as well as two rasters: (5) total precip '
        'sum over AOI and (6) '
        'overall monthly temperture mean in the AOI.'))
    parser.add_argument(
        'path_to_watersheds', help='Path to vector/shapefile of watersheds')
    parser.add_argument(
        'start_date', help='start date for summation (YYYY-MM-DD) format')
    parser.add_argument(
        'end_date', help='start date for summation (YYYY-MM-DD) format')
    args = parser.parse_args()

    monthly_date_range_list = build_monthly_ranges(
        args.start_date, args.end_date)

    vector_basename = os.path.basename(
        os.path.splitext(args.path_to_watersheds)[0])
    project_basename = (
        f'''{vector_basename}_{args.start_date}_{args.end_date}''')
    workspace_dir = f'workspace_{project_basename}'
    task_graph = taskgraph.TaskGraph(
        workspace_dir, multiprocessing.cpu_count(), 15.0)

    clip_dir = os.path.join(workspace_dir, 'clip')
    os.makedirs(clip_dir, exist_ok=True)
    monthly_precip_dir = os.path.join(
        workspace_dir, 'monthly_precip_rasters')
    os.makedirs(monthly_precip_dir, exist_ok=True)
    monthly_temp_dir = os.path.join(
        workspace_dir, 'monthly_temp_rasters')
    os.makedirs(monthly_temp_dir, exist_ok=True)

    monthly_mean_list = []
    for start_date, end_date in monthly_date_range_list:
        month_start_day = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        month_end_day = datetime.datetime.strptime(end_date, '%Y-%m-%d')

        raster_list_set = {
            variable_id: collections.defaultdict(list)
            for variable_id in VARIABLE_ID_LIST}
        for date in daterange(month_start_day, month_end_day):
            for variable_id in VARIABLE_ID_LIST:
                date_str = date.strftime('%Y-%m-%d')
                clip_path = os.path.join(
                    clip_dir, f'clip_{DATASET_ID}_{variable_id}_{date_str}')

                clip_task = task_graph.add_task(
                    func=fetch_data.fetch_and_clip,
                    args=(
                        DATASET_ID, variable_id, date_str,
                        (ERA5_RESOLUTION_M, -ERA5_RESOLUTION_M),
                        args.path_to_watersheds, clip_path),
                    kwargs={
                        'all_touched': True,
                        'target_mask_value': MASK_NODATA},
                    target_path_list=[clip_path],
                    task_name=f'fetch and clip {clip_path}')
                raster_list_set[variable_id]['rasters'].append((clip_path, 1))
                raster_list_set[variable_id]['tasks'].append(clip_task)

        # calculate monthly precip sum
        month_precip_path = os.path.join(monthly_precip_dir, (
            f"{vector_basename}_monthly_precip_sum_"
            f"{start_date}_{end_date}.tif"))

        month_precip_task = task_graph.add_task(
            func=geoprocessing.raster_calculator,
            args=(
                raster_list_set['sum_tp_mm']['rasters'], _sum_op,
                month_precip_path, gdal.GDT_Float32, MASK_NODATA),
            dependent_task_list=raster_list_set['sum_tp_mm']['tasks'],
            target_path_list=[month_precip_path],
            task_name=f'sum {month_precip_path}')

        precip_mean_task = task_graph.add_task(
            func=mean_of_raster_op,
            args=(month_precip_path,),
            store_result=True,
            dependent_task_list=[month_precip_task],
            task_name=f'mean of {month_precip_path}')

        # calculate monthly temp mean
        month_temp_path = os.path.join(monthly_temp_dir, (
            f"{vector_basename}_monthly_temp_mean_"
            f"{start_date}_{end_date}.tif"))
        month_temp_task = task_graph.add_task(
            func=geoprocessing.raster_calculator,
            args=(
                raster_list_set['mean_t2m_c']['rasters'], _mean_op,
                month_temp_path, gdal.GDT_Float32, MASK_NODATA),
            dependent_task_list=raster_list_set['mean_t2m_c']['tasks'],
            target_path_list=[month_temp_path],
            task_name=f'sum {month_temp_path}')

        temp_mean_task = task_graph.add_task(
            func=mean_of_raster_op,
            args=(month_temp_path,),
            store_result=True,
            dependent_task_list=[month_temp_task],
            task_name=f'mean of {month_temp_path}')

        year = start_date[:4]
        month = start_date[5:7]
        monthly_mean_list.append(
            (year, month, precip_mean_task, temp_mean_task))

    # create table that lists precip and temp means per month
    target_base = (
        f"{vector_basename}_monthly_precip_temp_mean_"
        f"{args.start_date}_{args.end_date}")
    target_table_path = os.path.join(workspace_dir, f"{target_base}.csv")

    # report precip and temp cronologically by month
    precip_by_year = collections.defaultdict(list)
    precip_by_month = collections.defaultdict(list)
    temp_by_month = collections.defaultdict(list)
    with open(target_table_path, 'w') as monthly_table_file:
        monthly_table_file.write(
            'date,' + ','.join(CSV_BANDS_TO_DISPLAY) + '\n')
        for year, month, precip_task, temp_task in monthly_mean_list:
            precip_val = precip_task.get()
            temp_val = temp_task.get()
            monthly_table_file.write(
                f'{year}-{month},{precip_val},{temp_val}\n')
            precip_by_year[year].append(precip_val)
            precip_by_month[month].append(precip_val)
            temp_by_month[month].append(temp_val)

    # create a table for precip and temp that is by MONTH showing a mean
    # value for that month over every year in the time period
    for table_type, dict_by_month in [
            ('precip', precip_by_month), ('temp', temp_by_month)]:
        monthly_normal_table_path = (
            f"{vector_basename}_monthly_{table_type}_normal_"
            f"{args.start_date}_{args.end_date}.csv")
        target_table_path = os.path.join(workspace_dir, f"{target_base}.csv")
        with open(monthly_normal_table_path, 'w') as \
                monthly_normal_table:
            monthly_normal_table.write(f'month,avg {table_type}\n')
            for month_id, data_list in sorted(dict_by_month.items()):
                monthly_normal_table.write(
                    f'{month_id},{numpy.average(data_list)}\n')

    target_base = (
        f"{vector_basename}_annual_precip_mean_"
        f"{args.start_date}_{args.end_date}")
    target_table_path = os.path.join(workspace_dir, f"{target_base}.csv")
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

    # generate total precip sum raster
    monthly_precip_dir
    monthly_temp_dir
    precip_raster_path_list = [
        (path, 1) for path in glob.glob(os.path.join(
            monthly_precip_dir, '*.tif'))]
    temp_raster_path_list = [
        (path, 1) for path in glob.glob(os.path.join(
            monthly_temp_dir, '*.tif'))]

    total_precip_path = os.path.join(
        workspace_dir,
        (f"{vector_basename}_precip_mm_sum_"
         f"{args.start_date}_{args.end_date}.tif"))

    total_temp_mean_path = os.path.join(
        workspace_dir,
        (f"{vector_basename}_temp_C_monthly_mean_"
         f"{args.start_date}_{args.end_date}.tif"))

    _ = task_graph.add_task(
        func=geoprocessing.raster_calculator,
        args=(
            precip_raster_path_list, _sum_op,
            total_precip_path, gdal.GDT_Float32, MASK_NODATA),
        target_path_list=[total_precip_path],
        task_name=f'generate total precip {total_precip_path}')

    # calculate monthly temp mean
    _ = task_graph.add_task(
        func=geoprocessing.raster_calculator,
        args=(
            temp_raster_path_list, _mean_op,
            total_temp_mean_path, gdal.GDT_Float32, MASK_NODATA),
        dependent_task_list=raster_list_set['mean_t2m_c']['tasks'],
        target_path_list=[total_temp_mean_path],
        task_name=f'man total temp {total_temp_mean_path}')

    task_graph.join()
    task_graph.close()

    LOGGER.info(f'********** ALL DONE -> results are in {workspace_dir}')


if __name__ == '__main__':
    main()
