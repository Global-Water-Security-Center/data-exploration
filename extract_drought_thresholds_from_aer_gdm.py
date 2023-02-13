"""Extract drought monitoring data from user provided AOI."""
import collections
import argparse
import logging
import sys
import os

from rasterio.transform import Affine
import geopandas
import numpy
import pandas
import rasterio
import xarray
import rioxarray


logging.basicConfig(
    level=logging.WARNING,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)

GDM_DATASET = 'http://h2o-dev.aer-aws-nonprod.net/thredds/dodsC/gwsc/gdm'


def main():
    parser = argparse.ArgumentParser(
        description=f'Extract drought thresholds from {GDM_DATASET}')
    parser.add_argument(
        'aoi_vector_path', help='Path to vector/shapefile of area of interest')
    parser.add_argument('start_date', type=str, help='start date YYYY-MM-DD')
    parser.add_argument('end_date', type=str, help='end date YYYY-MM-DD')
    args = parser.parse_args()

    aoi_vector = geopandas.read_file(args.aoi_vector_path)
    aoi_vector = aoi_vector.to_crs('EPSG:4236')

    date_range = pandas.date_range(
        start=args.start_date, end=args.end_date, freq='MS')

    minx, miny, maxx, maxy = aoi_vector.total_bounds

    # lat slice goes from pos to neg because it's 90 to -90 degrees so max first
    lat_slice = slice(float(maxy), float(miny))
    # lng slice is from -180 to 180 so min first
    lon_slice = slice(float(minx), float(maxx))
    LOGGER.debug(lat_slice)
    LOGGER.debug(lon_slice)

    gdm_dataset = xarray.open_dataset(GDM_DATASET)
    gdm_dataset['time'] = pandas.DatetimeIndex(gdm_dataset.time.str.decode('utf-8').astype('datetime64'))
    local_slice = gdm_dataset.sel(lat=lat_slice, lon=lon_slice)

    # add a mask that counts the number of valid pixels
    dims = (local_slice.dims['lon'], local_slice.dims['lat'])
    LOGGER.debug(dims)
    one_mask = xarray.DataArray(numpy.ones(dims), dims=['lon', 'lat'])
    local_slice['mask'] = one_mask

    table_path = f'drought_info_raw_{os.path.basename(os.path.splitext(args.aoi_vector_path)[0])}_{args.start_date}_{args.end_date}.csv'
    drought_months = collections.defaultdict(lambda: collections.defaultdict(int))
    year_set = set()
    with open(table_path, 'w') as table_file:
        table_file.write('date,total_pixels,D0_-_Abnormally_Dry,D1_-_Moderate_Drought,D2_-_Severe_Drought,D3_-_Extreme_Drought,D4_-_Exceptional_Drought,\n')
        for month in date_range:
            LOGGER.info(f'processing {month}')
            year_set.add(month.year)
            table_file.write(f'{month.strftime("%Y-%m")}')
            LOGGER.debug('sel time')
            month_slice = local_slice.sel(time=month)
            LOGGER.debug('set dims')
            month_slice = month_slice.rio.set_spatial_dims(x_dim='lon', y_dim='lat')
            LOGGER.debug('sel crs')
            month_slice = month_slice.rio.write_crs('EPSG:4326')
            LOGGER.debug('clip')
            month_slice = month_slice.rio.clip(aoi_vector.geometry.values)
            LOGGER.debug(month_slice)
            LOGGER.debug('sel do values')
            LOGGER.debug(month_slice.mask)
            area_pixels = numpy.count_nonzero(month_slice.mask.values==1)
            table_file.write(f',{area_pixels}')
            drought_values = month_slice.drought.values
            for drought_flag_val in range(5):
                drought_pixels = numpy.count_nonzero(drought_values==drought_flag_val)
                table_file.write(f',{drought_pixels}')
                if drought_flag_val >= 3:  # D3 or D4 category
                    for threshold_id, area_threshold in [('1/3', 1/3), ('1/2', 1/2), ('2/3', 2/3)]:
                        if drought_pixels/area_pixels >= area_threshold:
                            drought_months[month.year][area_threshold] += 1
            table_file.write('\n')
            LOGGER.debug('done')
            break

    LOGGER.debug(drought_months)
    table_path = f'drought_info_by_year_{os.path.basename(os.path.splitext(args.aoi_vector_path)[0])}_{args.start_date}_{args.end_date}.csv'
    with open(table_path, 'w') as table_file:
        table_file.write('year,n months with 1/3 drought in region,n months with 1/2 drought in region,n months with 2/3 drought in region\n')
        for year in sorted(year_set):
            threshold_dict = drought_months[year]
            table_file.write(f'{year}')
            for threshold_id in ['1/3', '1/2', '2/3']:
                table_file.write(f',{threshold_dict[threshold_id]}')
            table_file.write('\n')

    #LOGGER.debug(numpy.unique(local_slice.time))
    return

    netcdf_path = f'{COUNTRY_NAME}_drought_{START_DATE}_{END_DATE}.nc'
    print(f'saving to {netcdf_path}')
    local_slice.to_netcdf(netcdf_path)
    print(f'verifying {netcdf_path} saved correctly')
    local_dataset = xarray.open_dataset(netcdf_path)
    print(local_dataset)
    return

    # get exact coords for correct geotransform
    lat_slice = gdm_dataset.sel(lat=lat_slice).lat
    lon_slice = gdm_dataset.sel(lon=lon_slice).lon
    xres = float((lon_slice[-1] - lon_slice[0]) / len(lon_slice))
    yres = float((lat_slice[-1] - lat_slice[0]) / len(lat_slice))
    transform = Affine.translation(
        lon_slice[0], lat_slice[0]) * Affine.scale(xres, yres)

    with rasterio.open(
        f"{COUNTRY_NAME}_drought_{START_DATE}_{END_DATE}.tif",
        mode="w",
        driver="GTiff",
        height=len(lat_slice),
        width=len(lon_slice),
        count=len(date_range),
        dtype=numpy.uint8,
        nodata=0,
        crs="+proj=latlong",
        transform=transform,
        kwargs={
            'tiled': 'YES',
            'COMPRESS': 'LZW',
            'PREDICTOR': 2}) as new_dataset:
        for year_index in range(len(date_range)):
            print(f'writing band {year_index} of {len(date_range)}')
            new_dataset.write(
                local_slice.drought[year_index, :], 1+year_index)


if __name__ == '__main__':
    main()
