"""Sum precipitation in a watershed(s) over a given time period."""
import argparse
import os

from rasterio.transform import Affine
import geopandas
import numpy
import pandas
import rasterio
import rioxarray
import xarray
import matplotlib.pyplot as plt


WEBDAP_PATH = (
    'http://h2o-sandbox1.aer-aws-nonprod.net/'
    'thredds/dodsC/era5/daily-summary.nc')

def main():
    parser = argparse.ArgumentParser(
        description='Sum precip by watershed in a time range.')
    parser.add_argument(
        'path_to_watersheds', help='Path to vector/shapefile of watersheds')
    parser.add_argument(
        'start_date', help='start date for summation (YYYY-MM-DD) format')
    parser.add_argument(
        'end_date', help='start date for summation (YYYY-MM-DD) format')
    args = parser.parse_args()

    print(f'loading webdap netcat from {WEBDAP_PATH}')
    gdm_dataset = xarray.open_dataset(WEBDAP_PATH)
    gdm_dataset = gdm_dataset.rio.write_crs('epsg:4326')
    gdm_dataset['time'] = pandas.DatetimeIndex(
        gdm_dataset.time.str.decode('utf-8').astype('datetime64'))
    date_range = pandas.date_range(
        start=args.start_date, end=args.end_date, freq='MS')

    # convert lon that goes from 0 to 360 to -180 to 180
    # gdm_dataset.coords['longitude'] = (
    #     gdm_dataset.coords['longitude'] + 180) % 360 - 180
    # gdm_dataset = gdm_dataset.sortby(gdm_dataset.longitude)

    print(f'loading vector {args.path_to_watersheds}')
    vector = geopandas.read_file(args.path_to_watersheds)
    vector = vector.to_crs('epsg:4326')
    vector = vector.simplify(0.25/2) # simplify to the resolution of the raster

    # max first since lat slice goes from 90 to -90
    lat_slice = slice(
        float(numpy.max(vector.bounds.maxy)),
        float(numpy.min(vector.bounds.miny)))
    # lng slice is from -180 to 180 so min first
    min_lon = float(numpy.min(vector.bounds.minx))
    max_lon = float(numpy.max(vector.bounds.maxx))

    sum_per_time_list = []
    sum_over_time_list = []
    # this loop handles some serious slowdown when querying across the meridian
    # and instead breaks it up into western/eastern hemisphere and merges at
    # the end
    for lon_slice in [slice(0, max_lon), slice(min_lon, 0)]:
        if lon_slice == slice(0, 0):
            # it doesn't overlap meridian
            continue

        active_slice = lon_slice
        local_vector = vector
        if lon_slice.start < 0:
            # transform geometry 360 degrees
            local_vector = local_vector.translate(xoff=360)
            print(local_vector.bounds.minx)
            active_slice = slice(360+lon_slice.start, 360)

        print(
            f'subsetting to the following:'
            f'\n\tdate_range: {date_range}'
            f'\n\tlat slice: {lat_slice}'
            f'\n\tlon slice: {active_slice}')

        local_slice = gdm_dataset.sel(
            latitude=lat_slice,
            longitude=active_slice,
            time=date_range).sum_tp_mm
        vector_basename = os.path.splitext(
            os.path.basename(args.path_to_watersheds))[0]
        print(f'clipping subset to {vector_basename}')
        try:
            local_slice = local_slice.rio.clip(local_vector.geometry.values)
        except rioxarray.exceptions.NoDataInBounds:
            print(f'no data in bounds for slice {lat_slice}/{lon_slice}')
            continue

        if lon_slice.start < 0:
            # transform back into -180, 180 so it can merge
            local_slice.coords['longitude'] = (
                local_slice.coords['longitude'] + 180) % 360 - 180
            local_slice = local_slice.sortby(local_slice.longitude)

        sum_per_time = local_slice.sum(
            dim=['longitude', 'latitude'], skipna=True, min_count=1)
        sum_per_time_list.append(sum_per_time)

        sum_over_time = local_slice.sum(dim='time', skipna=True, min_count=1)
        sum_over_time_list.append(sum_over_time)

    target_base = f"{vector_basename}_precip_sum_{args.start_date}_{args.end_date}"
    target_table_path = f"{target_base}.csv"

    sum_per_time = sum(sum_per_time_list)
    with open(target_table_path, 'w') as csv_table:
        csv_table.write('time,sum_tp_mm\n')
        for val in sum_per_time:
            csv_table.write(f'{val.time.data},{val.data}\n')

    #sum_over_time = xarray.merge(sum_over_time_list)
    if len(sum_over_time_list) > 1:
        sum_over_time = sum_over_time_list[0].combine_first(
            sum_over_time_list[1])
    else:
        sum_over_time = sum_over_time_list[0]
    sum_over_time.plot()

    target_raster_path = f"{target_base}.tif"
    print(f'writing result to {target_raster_path}')

    # get exact coords for correct geotransform
    xres = float(
        (sum_over_time.longitude[-1] - sum_over_time.longitude[0]) /
        len(sum_over_time.longitude))
    yres = float(
        (sum_over_time.latitude[-1] - sum_over_time.latitude[0]) /
        len(sum_over_time.latitude))
    transform = Affine.translation(
        lon_slice[0], lat_slice[0]) * Affine.scale(xres, yres)

    print(f'making raster with bounds\n\t{lat_slice}:{lon_slice}\n\t{xres}*{yres}')

    with rasterio.open(
        target_raster_path,
        mode="w",
        driver="GTiff",
        height=len(lat_slice),
        width=len(lon_slice),
        count=1,
        dtype=numpy.float32,
        nodata=numpy.nan,
        crs="+proj=latlong",
        transform=transform,
        kwargs={
            'tiled': 'YES',
            'COMPRESS': 'LZW',
            'PREDICTOR': 2}) as new_dataset:
        new_dataset.write(sum_over_time, 1)
        #new_dataset.write(sum_over_time, 1)

    plt.show()


if __name__ == '__main__':
    main()
