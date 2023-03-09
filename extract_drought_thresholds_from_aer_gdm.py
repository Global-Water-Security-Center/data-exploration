"""Extract drought monitoring data from user provided AOI."""
import concurrent
import collections
import argparse
import logging
import sys
import os

from rasterio.transform import Affine
import utils
import geopandas
import numpy
import pandas
import rasterio
import xarray


logging.basicConfig(
    level=logging.WARNING,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)

GDM_DATASET = 'https://h2o.aer.com/thredds/dodsC/gwsc/gdm'


def main():
    parser = argparse.ArgumentParser(description=(
        f'Extract drought thresholds from {GDM_DATASET} and produce a CSV '
        'that breaks down analysis by year to highlight how many months '
        'experience drought in 1/3, 1/2, and 2/3 of region. Results '))
    parser.add_argument(
        'aoi_vector_path', help='Path to vector/shapefile of area of interest')
    parser.add_argument('start_date', type=str, help='start date YYYY-MM-DD')
    parser.add_argument('end_date', type=str, help='end date YYYY-MM-DD')
    args = parser.parse_args()

    # aoi_vector_path = 'drycorridor.shp'
    # start_date = '1979-01-01'
    # end_date = '2021-12-21'

    aoi_vector = geopandas.read_file(args.aoi_vector_path)
    aoi_vector = aoi_vector.to_crs('EPSG:4236')

    date_range = pandas.date_range(
        start=args.start_date, end=args.end_date, freq='MS')

    minx, miny, maxx, maxy = aoi_vector.total_bounds

    # lat slice goes from pos to neg because it's 90 to -90 degrees so max first
    lat_slice = slice(float(maxy), float(miny))
    # lng slice is from -180 to 180 so min first
    lon_slice = slice(float(minx), float(maxx))

    LOGGER.info(f'preprocess {GDM_DATASET}')
    gdm_dataset = xarray.open_dataset(GDM_DATASET)
    gdm_dataset['time'] = pandas.DatetimeIndex(
        gdm_dataset.time.str.decode('utf-8').astype('datetime64'))
    local_slice = gdm_dataset.sel(lat=lat_slice, lon=lon_slice)

    # add a mask that counts the number of valid pixels
    dims = (local_slice.dims['lon'], local_slice.dims['lat'])
    one_mask = xarray.DataArray(numpy.ones(dims), dims=['lon', 'lat'])
    local_slice['mask'] = one_mask

    table_path = f'''drought_info_raw_{
        os.path.basename(os.path.splitext(args.aoi_vector_path)[0])}_{
        args.start_date}_{args.end_date}.csv'''
    drought_months = collections.defaultdict(lambda: collections.defaultdict(int))
    year_set = set()
    LOGGER.info('start processing')
    with open(table_path, 'w') as table_file:
        table_file.write('date,total_pixels,D0_-_Abnormally_Dry,D1_-_Moderate_Drought,D2_-_Severe_Drought,D3_-_Extreme_Drought,D4_-_Exceptional_Drought,\n')
        with concurrent.futures.ThreadPoolExecutor() as executor:
            work_list = []
            for month_date in date_range:
                LOGGER.debug(f'schedule {month_date}')

                def calc_month_values(month_date, local_slice):
                    year_set.add(month_date.year)
                    month_slice = local_slice.sel(time=month_date)
                    month_slice = month_slice.rio.set_spatial_dims(x_dim='lon', y_dim='lat')
                    month_slice = month_slice.rio.write_crs('EPSG:4326')
                    month_slice = month_slice.rio.clip(aoi_vector.geometry.values)
                    return month_slice
                work_list.append((month_date, executor.submit(
                    calc_month_values, month_date, local_slice)))
            valid_mask = None
            running_drought_count_array = None
            for month_date, result in work_list:
                monthly_drought_pixel_count = 0
                table_file.write(f'{month_date.strftime("%Y-%m")}')
                month_slice = result.result()
                valid_pixel_count = numpy.count_nonzero(
                    month_slice.mask.values == 1)
                table_file.write(f',{valid_pixel_count}')
                drought_values = month_slice.drought.values
                for drought_category in [0, 1, 2, 3, 4]:
                    valid_drought_pixels = (drought_values == drought_category)
                    if drought_category >= 3:
                        if running_drought_count_array is None:
                            running_drought_count_array = valid_drought_pixels.astype(numpy.int32)
                            valid_mask = month_slice.mask == 1
                        else:
                            running_drought_count_array += valid_drought_pixels
                            valid_mask |= month_slice.mask == 1
                        monthly_drought_pixel_count += numpy.count_nonzero(
                            valid_drought_pixels)
                    table_file.write(f',{monthly_drought_pixel_count}')

                LOGGER.debug(f'{month_date} - {monthly_drought_pixel_count} drought pixels found')
                for threshold_id, area_threshold in [
                        ('1/3', 1/3), ('1/2', 1/2), ('2/3', 2/3)]:
                    if monthly_drought_pixel_count/valid_pixel_count >= area_threshold:
                        drought_months[month_date.year][threshold_id] += 1
                table_file.write('\n')
            LOGGER.debug('done with scheduling')

    file_basename = (
        f'{utils.file_basename(args.aoi_vector_path)}_'
        f'{args.start_date}_{args.end_date}')
    table_path = f'''drought_info_by_year_{file_basename}.csv'''
    with open(table_path, 'w') as table_file:
        table_file.write('year,n months with 1/3 drought in region,n months with 1/2 drought in region,n months with 2/3 drought in region\n')
        for year in sorted(year_set):
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
        f'drought_events_by_pixel_{file_basename}.tif')
    nodata = -1
    # for some reason the mask is transposed in this netcat file
    running_drought_count_array[~valid_mask.transpose()] = nodata
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
