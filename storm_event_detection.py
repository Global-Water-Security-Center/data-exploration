"""See `python scriptname.py --help"""
from pathlib import Path
import argparse
import calendar
import datetime
import logging
import multiprocessing
import os
import shutil
import sys
import time

from osgeo import gdal
from osgeo import osr
from ecoshard import geoprocessing
from ecoshard import taskgraph
import numpy
import requests

try:
    from ecoshard import fetch_data
except RuntimeError as e:
    print(f'Error when loading fetch_data: {e}')

logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stdout,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
LOGGER.setLevel(logging.DEBUG)
logging.getLogger('ecoshard.fetch_data').setLevel(logging.INFO)

ERA5_RESOLUTION_M = 27830
ERA5_RESOLUTION_DEG = 0.25
ERA5_TOTAL_PRECIP_BAND_NAME = 'total_precipitation'
MASK_NODATA = -9999

DATASET_ID = 'era5_daily'
VARIABLE_ID = 'sum_tp_mm'


def build_monthly_ranges(start_date, end_date):
    """Create a list of time range tuples that span months over the range.

    Args:
        start_date: (str) start date in the form YYYY-MM-DD
        end_date: (str) end date in the form YYYY-MM-DD

    Returns:
        list of start/end date tuples inclusive that span the time range
        defined by `start_date`-`end_date`
    """
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')

    current_day = start_date
    date_range_list = []
    while current_day <= end_date:
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


def daterange(start_date, end_date):
    """Generator produces all ``datetimes`` between start and end."""
    if start_date == end_date:
        yield start_date
        return
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


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
        '--date_range', default=[], action='append', required=True, nargs=2,
        help='Pass a pair of start/end dates in the (YYYY-MM-DD) format')
    parser.add_argument(
        '--rain_event_threshold', default=0.1, type=float,
        help='amount of rain (mm) in a day to count as a rain event')
    args = parser.parse_args()

    result_workspace_path_list = []
    for start_date, end_date in args.date_range:
        result_workspace_path = process_date_range(
            args.path_to_watersheds, start_date, end_date,
            args.rain_event_threshold)
        result_workspace_path_list.append(result_workspace_path)

    LOGGER.info(
        f'******** ALL DONE ({time.time()-start_time:.2f}s), results '
        'in:\n\t* ' + '\n\t* '.join(result_workspace_path_list))


def process_date_range(
        path_to_watersheds, start_date, end_date,
        rain_event_threshold):
    """Process a given date range for storm event detection."""
    vector_basename = os.path.basename(
        os.path.splitext(path_to_watersheds)[0])
    project_basename = (
        f'''storm_event_detection_{vector_basename}_{start_date}_{end_date}''')
    workspace_dir = Path(f'workspace_{project_basename}')
    workspace_dir.mkdir(parents=True, exist_ok=True)

    task_graph = taskgraph.TaskGraph(
        workspace_dir, multiprocessing.cpu_count(), 15.0)

    monthly_date_range_list = build_monthly_ranges(
        start_date, end_date)
    LOGGER.debug(monthly_date_range_list)

    clip_dir = os.path.join(workspace_dir, 'clip')
    os.makedirs(clip_dir, exist_ok=True)

    monthly_precip_path_list = []
    for start_date, end_date in monthly_date_range_list:
        month_start_day = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        month_end_day = datetime.datetime.strptime(end_date, '%Y-%m-%d')

        existance_set = {}
        for date in daterange(month_start_day, month_end_day):
            date_str = date.strftime('%Y-%m-%d')
            clip_path = os.path.join(
                clip_dir, f'clip_{DATASET_ID}_{VARIABLE_ID}_{date_str}')

            vector_info = geoprocessing.get_vector_info(path_to_watersheds)
            vector_projection = osr.SpatialReference()
            vector_projection.ImportFromWkt(vector_info['projection_wkt'])
            if vector_projection.IsProjected():
                clip_cell_size = (ERA5_RESOLUTION_M, -ERA5_RESOLUTION_M)
            else:
                clip_cell_size = (ERA5_RESOLUTION_DEG, -ERA5_RESOLUTION_DEG)

            dataset_args = {
                'date': date_str,
                'variable': VARIABLE_ID
            }

            fetch_and_clip_args = (
                DATASET_ID, dataset_args,
                clip_cell_size,
                path_to_watersheds, clip_path)

            exists_task = task_graph.add_task(
                func=fetch_data.file_exists,
                args=(DATASET_ID, dataset_args),
                store_result=True,
                transient_run=True,
                task_name=(
                    f'test if {DATASET_ID}/{VARIABLE_ID}/{date_str} '
                    'exists'))
            existance_set[f'{DATASET_ID}/{VARIABLE_ID}/{date_str}'] = (
                exists_task, VARIABLE_ID, (fetch_and_clip_args, clip_path))

        missing_file_list = [
            variable_str
            for variable_str, (_, exist_task, _) in existance_set.items()
            if not exists_task.get()]
        if missing_file_list:
            task_graph.join()
            task_graph.close()
            raise RuntimeError(
                'The following data cannot be found in the cloud: ' +
                ', '.join(missing_file_list))

        clip_path_band_list = []
        clip_task_list = []
        for _, variable_id, (fetch_args, clip_path) in existance_set.values():
            LOGGER.info(f'clip path: {clip_path}')
            clip_task = task_graph.add_task(
                func=fetch_data.fetch_and_clip,
                args=fetch_args,
                kwargs={
                    'all_touched': True,
                    'target_mask_value': MASK_NODATA},
                target_path_list=[clip_path],
                task_name=f'fetch and clip {clip_path}')
            clip_path_band_list.append((clip_path, 1))
            clip_task_list.append(clip_task)

        monthly_precip_path = os.path.join(
            workspace_dir, f'''{vector_basename}_48hr_avg_precip_events_{
            start_date}_{end_date}.tif''')
        monthly_precip_task = task_graph.add_task(
            func=_process_month,
            args=(
                clip_path_band_list, MASK_NODATA, rain_event_threshold,
                monthly_precip_path),
            target_path_list=[monthly_precip_path],
            dependent_task_list=clip_task_list,
            task_name=f'process month {monthly_precip_path}')
        monthly_precip_path_list.append(
            (monthly_precip_task, start_date[:7], Path(monthly_precip_path)))

    precip_over_all_time_path = Path(os.path.join(
        workspace_dir, f'''overall_{project_basename}_48hr_avg_precip_events_{
        start_date}_{end_date}.tif'''))

    table_path = r'\\?\{}'.format(
        Path(f'{os.path.splitext(precip_over_all_time_path)[0]}.csv').
        resolve())

    with open(table_path, 'w') as csv_table:
        csv_table.write('year-month,number of storm events in region\n')
        running_sum = None
        for monthly_precip_task, month_date, raster_path in \
                monthly_precip_path_list:
            monthly_precip_task.join()
            absolute_raster_path = r'\\?\{}'.format(raster_path.resolve())
            r = gdal.OpenEx(str(absolute_raster_path))
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
    r = gdal.OpenEx(
        r'\\?\{}'.format(precip_over_all_time_path.resolve()),
        gdal.OF_RASTER | gdal.GA_Update)
    b = r.GetRasterBand(1)
    running_sum[~valid_mask] = MASK_NODATA
    b.WriteArray(running_sum)
    b = None
    r = None

    task_graph.join()
    task_graph.close()
    return workspace_dir


if __name__ == '__main__':
    main()
