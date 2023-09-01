"""See `python scriptname.py --help"""
from functools import partial
from concurrent.futures import ThreadPoolExecutor
import argparse
import datetime
import logging
import os
import sys
import tempfile

from osgeo import gdal
from ecoshard import fetch_data
from ecoshard import geoprocessing
from rasterio.transform import Affine
import geopandas
import numpy
import rasterio
import xarray

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
LOGGER.setLevel(logging.DEBUG)
logging.getLogger('fetch_data').setLevel(logging.INFO)


def process_era5_anomaly_to_geotiff(
        netcdf_path, year_month_str, target_path_pattern):
    """Convert era5 anomaly to geotiff

    Args:
        netcdf_path (str): path to netcat file
        year_month_str (str): formatted version of the date to use in the target
            file
        target_path_pattern (str): pattern that will allow the replacement
            of `variable` and `date` strings with the appropriate variable
            date strings in the netcat variables.

    Returns:
        list of (file, variable_id) tuples created by this process
    """
    LOGGER.info(f'processing {netcdf_path}')
    dataset = xarray.open_dataset(netcdf_path)

    res_list = []
    coord_list = []
    for coord_id, field_id in zip(['x', 'y'], ['longitude', 'latitude']):
        coord_array = dataset.coords[field_id]
        res_list.append(float(
            (coord_array[-1] - coord_array[0]) / len(coord_array)))
        coord_list.append(coord_array)

    transform = Affine.translation(
        *[a[0] for a in coord_list]) * Affine.scale(*res_list)

    target_path_variable_id_list = []
    for variable_id, data_array in dataset.items():
        target_path = target_path_pattern.format(**{
            'year_month': year_month_str,
            'variable': variable_id
            })
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        with rasterio.open(
            target_path,
            mode="w",
            driver="GTiff",
            height=len(coord_list[1]),
            width=len(coord_list[0]),
            count=1,
            dtype=numpy.float32,
            nodata=None,
            crs="+proj=latlong",
            transform=transform,
            **{
                'tiled': 'YES',
                'COMPRESS': 'LZW',
                'PREDICTOR': 2}) as new_dataset:
            new_dataset.write(data_array)
        target_path_variable_id_list.append((target_path, variable_id))
    return target_path_variable_id_list


def download_and_repack(year_month, target_path_pattern, aoi_path, clip_id):
    try:
        LOGGER.info(f'fetching {year_month}')
        netcdf_path = fetch_data.fetch_file(
            'era5_anomaly', {'year_month': year_month})
        LOGGER.info(f'downloaded to {netcdf_path}')
        geotiff_path_variable_id_list = process_era5_anomaly_to_geotiff(
            netcdf_path, year_month, target_path_pattern)
        for geotiff_path, variable_id in geotiff_path_variable_id_list:
            # do the clip here
            print(geotiff_path, variable_id)
            raster_info = geoprocessing.get_raster_info(geotiff_path)
            aoi_info = geoprocessing.get_vector_info(aoi_path)
            target_path = f'%s_{clip_id}%s' % os.path.splitext(geotiff_path)
            r = gdal.OpenEx(geotiff_path, gdal.OF_RASTER)
            b = r.GetRasterBand(1)
            b.SetNoDataValue(-9999)
            b = None
            r = None
            geoprocessing.warp_raster(
                geotiff_path, raster_info['pixel_size'],
                target_path,
                'near', target_bb=aoi_info['bounding_box'],
                vector_mask_options={
                    'mask_vector_path': aoi_path,
                    })
            os.remove(geotiff_path)

    except FileNotFoundError:
        LOGGER.error(f'No file found for {year_month}, skipping')


def main():
    parser = argparse.ArgumentParser(description=(
        'Fetch and clip AER ERA anomaly data.'))
    parser.add_argument(
        'start_date', type=str, help='Pick a date to start downloading YYYY-MM.')
    parser.add_argument(
        'end_date', type=str, help='Pick a date to start downloading YYYY_MM.')
    parser.add_argument(
        '--local_workspace', type=str, default='era5_process_workspace',
        help='Directory to downloand and work in.')
    parser.add_argument('--path_to_aoi', required=True, help='Path to clip AOI from')
    parser.add_argument(
        '--filter_aoi_by_field', help=(
            'an argument of the form FIELDNAME=VALUE such as `sov_a3=AFG`'))
    args = parser.parse_args()

    temp_dir = None
    if args.filter_aoi_by_field:
        temp_dir = tempfile.mkdtemp(dir='.')
        filtered_aoi_path = os.path.join(
            temp_dir,
            f'{os.path.basename(os.path.splitext(args.path_to_aoi)[0])}'
            f'{args.filter_aoi_by_field}.gpkg')
        aoi_vector = geopandas.read_file(args.path_to_aoi)
        field_id, value = args.filter_aoi_by_field.split('=')
        aoi_vector = aoi_vector[aoi_vector[field_id] == value]
        aoi_vector.to_file(filtered_aoi_path, driver='GPKG')
        aoi_vector = None
        aoi_path = filtered_aoi_path
    else:
        aoi_path = args.path_to_aoi

    start_date = datetime.datetime.strptime(args.start_date, '%Y-%m')
    end_date = datetime.datetime.strptime(args.end_date, '%Y-%m')

    target_path_pattern = '%s_{variable}.tif' % (os.path.splitext(os.path.join(
        args.local_workspace,
        fetch_data.GLOBAL_CONFIG['era5_anomaly']['file_format']))[0])
    print(target_path_pattern)

    current_date = start_date
    date_list = []
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m')
        date_list.append(date_str)
        current_date += datetime.timedelta(days=31)  # Move to the next month
        current_date = current_date.replace(day=1)  # Ensure we are on the first day of the month

    with ThreadPoolExecutor(max_workers=50) as executor:
        _ = list(executor.map(partial(
            download_and_repack, target_path_pattern=target_path_pattern,
            aoi_path=aoi_path, clip_id=args.filter_aoi_by_field), date_list))


if __name__ == '__main__':
    main()
