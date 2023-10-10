"""See `python scriptname.py --help"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import argparse
import collections
import logging
import os
import re
import sys

from dateutil.relativedelta import relativedelta
from ecoshard import fetch_data
from rasterio.transform import Affine
import geopandas
import numpy
import pandas
import rasterio
import xarray

logging.basicConfig(
    level=logging.INFO,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)

GDM_DATASET = 'https://h2o.aer.com/thredds/dodsC/gwsc/gdm'
WORKSPACE_DIR = 'extract_drought_thresholds_workspace'


def get_file_basename(path):
    """Return base file path, or last directory."""
    basename = os.path.basename(os.path.splitext(path)[0])
    if basename == '':
        # do last directory
        basename = os.path.normpath(path).split(os.sep)[-1]
    return basename


class ValidateYearMonthFormat(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        if not re.match(r'^\d{4}-\d{1,2}$', values):
            raise argparse.ArgumentError(
                None, f"{values} Invalid date format. Should be YYYY-MM")
        year, month = map(int, values.split("-"))
        if year < 1950 or year > 2100 or month < 1 or month > 12:
            raise argparse.ArgumentError(
                None,
                "Invalid date. Year should be 1950-2100 and month "
                "should be 01-12")
        # Ensure month is two digits
        setattr(args, self.dest, f"{year}-{month:02d}")


def process_netcat_to_geotiff(netcat_path, date_str, target_path_pattern):
    """Convert era5 netcat files to geotiff

    Args:
        netcat_path (str): path to netcat file
        date_str (str): formatted version of the date to use in the target
            file
        target_path_pattern (str): pattern that will allow the replacement
            of `variable` and `date` strings with the appropriate variable
            date strings in the netcat variables.

    Returns:
        list of (file, variable_id) tuples created by this process
    """
    LOGGER.info(f'processing {netcat_path}')
    dataset = xarray.open_dataset(netcat_path)

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
            'date': date_str,
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


def main():
    parser = argparse.ArgumentParser(description=(
        f'Extract SPEI12 thresholds from {GDM_DATASET} and produce a CSV '
        'that breaks down analysis by year to highlight how many months '
        'experience drought in 1/3, 1/2, and 2/3 of region. Results are in '
        'three files: (1) spei12_drought_info_raw_{aoi}.csv contains '
        'month by month aggregates, '
        '(2) spei12_drought_events_by_pixel_{aoi}.tif contains pixels whose '
        'values are the number of months drought during the query time range '
        'and (3) spei12_drought_info_by_year_{aoi}.csv, summaries of total '
        'number of drought events per year in the AOI.'))
    parser.add_argument(
        'aoi_vector_path', help='Path to vector/shapefile of area of interest')
    parser.add_argument(
        'start_date', type=str, help='start date YYYY-MM',
        action=ValidateYearMonthFormat)
    parser.add_argument(
        'end_date', type=str, help='end date YYYY-MM',
        action=ValidateYearMonthFormat)
    parser.add_argument(
        '--filter_aoi_by_field', help=(
            'an argument of the form FIELDNAME=VALUE such as `sov_a3=AFG`'))
    args = parser.parse_args()

    # aoi_vector_path = 'drycorridor.shp'
    # start_date = '1979-01-01'
    # end_date = '2021-12-21'

    # fetch all the files
    start_date = datetime.strptime(args.start_date, '%Y-%m')
    end_date = datetime.strptime(args.end_date, '%Y-%m')

    netcat_file_list = []

    fetch_args_list = []
    current_date = start_date
    while current_date <= end_date:
        year, month = current_date.strftime('%Y-%m').split('-')
        fetch_args_list.append(
            ('aer_drought_netcat_monthly', {
                'year': year, 'month': month}))
        current_date += relativedelta(months=1)

    os.makedirs(WORKSPACE_DIR, exist_ok=True)

    with ThreadPoolExecutor() as executor:
        netcat_file_list = list(executor.map(
            lambda fetch_args: fetch_data.fetch_file(*fetch_args),
            fetch_args_list))

    aoi_vector = geopandas.read_file(args.aoi_vector_path)
    aoi_vector = aoi_vector.to_crs('EPSG:4236')
    if args.filter_aoi_by_field:
        field_id, value = args.filter_aoi_by_field.split('=')
        aoi_vector = aoi_vector[aoi_vector[field_id] == value]

    date_range = pandas.date_range(
        start=args.start_date, end=args.end_date, freq='MS')
    year_list = list(sorted(set(date_range.year.tolist())))

    minx, miny, maxx, maxy = aoi_vector.total_bounds

    lat_slice = slice(float(maxy), float(miny))
    lon_slice = slice(float(minx), float(maxx))

    if args.filter_aoi_by_field is not None:
        filter_str = f'{args.filter_aoi_by_field}_'
    else:
        filter_str = ''

    table_path = f'''spei12_drought_info_raw_{
        get_file_basename(args.aoi_vector_path)}_{filter_str}{
        args.start_date}_{args.end_date}.csv'''
    drought_months = collections.defaultdict(
        lambda: collections.defaultdict(int))
    LOGGER.info('start processing')
    with open(table_path, 'w') as table_file:
        table_file.write(
            'date,total_pixels,D0_-_Abnormally_Dry,D1_-_Moderate_Drought,D2_-'
            '_Severe_Drought,D3_-_Extreme_Drought,D4_-_Exceptional_Drought,\n')
        for month_date, netcat_path in zip(date_range, netcat_file_list):

            gdm_dataset = xarray.open_dataset(netcat_path)
            local_slice = gdm_dataset.sel(lat=lat_slice, lon=lon_slice)

            # add a mask that counts the number of valid pixels
            dims = (local_slice.dims['lon'], local_slice.dims['lat'])
            one_mask = xarray.DataArray(numpy.ones(dims), dims=['lon', 'lat'])
            local_slice['mask'] = one_mask

            local_slice = local_slice.rio.set_spatial_dims(
                x_dim='lon', y_dim='lat')
            local_slice = local_slice.rio.write_crs('EPSG:4326')
            local_slice = local_slice.rio.clip(aoi_vector.geometry.values)

            valid_mask = None
            running_drought_count_array = None
            # for month_date, result in work_list:
            monthly_drought_pixel_count = 0
            table_file.write(f'{month_date.strftime("%Y-%m")}')
            valid_pixel_count = numpy.count_nonzero(
                local_slice.mask.values == 1)
            table_file.write(f',{valid_pixel_count}')
            drought_values = local_slice.spei12.values
            for drought_category in [0, 1, 2, 3, 4]:
                valid_drought_pixels = (drought_values == drought_category)
                if drought_category >= 3:
                    if running_drought_count_array is None:
                        running_drought_count_array = (
                            valid_drought_pixels.astype(numpy.int32))
                        valid_mask = local_slice.mask == 1
                    else:
                        running_drought_count_array += valid_drought_pixels
                        valid_mask |= local_slice.mask == 1
                    monthly_drought_pixel_count += numpy.count_nonzero(
                        valid_drought_pixels)
                    table_file.write(f',{monthly_drought_pixel_count}')
                else:
                    table_file.write(
                        f',{numpy.count_nonzero(valid_drought_pixels)}')

            LOGGER.debug(
                f'{month_date} - {monthly_drought_pixel_count} '
                'drought pixels found')
            for threshold_id, area_threshold in [
                    ('1/3', 1/3), ('1/2', 1/2), ('2/3', 2/3)]:
                if monthly_drought_pixel_count/valid_pixel_count >= \
                        area_threshold:
                    drought_months[month_date.year][threshold_id] += 1
            table_file.write('\n')

    file_basename = (
        f'{get_file_basename(args.aoi_vector_path)}_{filter_str}'
        f'{args.start_date}_{args.end_date}')
    table_path = f'''spei12_drought_info_by_year_{file_basename}.csv'''
    with open(table_path, 'w') as table_file:
        table_file.write(
            'year,n months with 1/3 drought in region,n months with 1/2 '
            'drought in region,n months with 2/3 drought in region\n')
        for year in year_list:
            threshold_dict = drought_months[year]
            table_file.write(f'{year}')
            for threshold_id in ['1/3', '1/2', '2/3']:
                table_file.write(f',{threshold_dict[threshold_id]}')
            table_file.write('\n')

    res = (maxx-minx) / running_drought_count_array.shape[1]
    transform = Affine.translation(
        minx - res / 2,
        maxy + res / 2) * Affine.scale(res, -res)
    raster_path = (
        f'spei12_drought_events_by_pixel_{file_basename}.tif')
    nodata = -1
    # for some reason the mask is transposed in this netcat file
    LOGGER.debug(valid_mask.shape)
    LOGGER.debug(running_drought_count_array.shape)
    running_drought_count_array[~(valid_mask.transpose())] = nodata
    new_dataset = rasterio.open(
        raster_path,
        'w',
        driver='GTiff',
        height=running_drought_count_array.shape[0],
        width=running_drought_count_array.shape[1],
        count=1,
        dtype=running_drought_count_array.dtype,
        crs='+proj=latlong',
        transform=transform,
        nodata=nodata,
    )
    new_dataset.write(running_drought_count_array, 1)
    new_dataset.close()
    new_dataset = None

    LOGGER.info(
        f'All done\n'
        f'\tRaster with total drought events per pixel at: {raster_path}\n'
        f'\tTable with drought info by month: {table_path}\n')


if __name__ == '__main__':
    main()
