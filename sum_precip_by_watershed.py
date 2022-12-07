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

    # convert lon that goes from 0 to 360 to -180 to 180
    gdm_dataset.coords['longitude'] = (
        gdm_dataset.coords['longitude'] + 180) % 360 - 180
    gdm_dataset = gdm_dataset.sortby(gdm_dataset.longitude)
    date_range = pandas.date_range(
        start=args.start_date, end=args.end_date, freq='MS')

    print(f'loading vector {args.path_to_watersheds}')
    vector = geopandas.read_file(args.path_to_watersheds)
    vector = vector.to_crs('epsg:4326')
    vector = vector.simplify(0.25/2) # simplify to the resolution of the raster

    # max first since lat slice goes from 90 to -90
    lat_slice = slice(
        float(numpy.max(vector.bounds.maxy)),
        float(numpy.min(vector.bounds.miny)))
    # lng slice is from -180 to 180 so min first
    lon_slice = slice(
        float(numpy.min(vector.bounds.minx)),
        float(numpy.max(vector.bounds.maxx)))

    print(
        f'subsetting to the following:'
        f'\n\tdate_range: {date_range}'
        f'\n\tlat slice: {lat_slice}'
        f'\n\tlon slice: {lon_slice}')

    local_slice = gdm_dataset.sel(
        latitude=lat_slice,
        longitude=lon_slice,
        time=date_range).sum_tp_mm

    vector_basename = os.path.splitext(
        os.path.basename(args.path_to_watersheds))[0]
    print(f'clipping subset to {vector_basename}')

    local_slice = local_slice.rio.clip(vector.geometry.values)
    local_slice = local_slice.sum(dim='time', skipna=True, min_count=1)
    local_slice.plot()

    target_path = f"{vector_basename}_precip_sum_{args.start_date}_{args.end_date}.tif"
    print(f'writing result to {target_path}')
    # get exact coords for correct geotransform
    lat_slice = gdm_dataset.sel(latitude=lat_slice).latitude
    lon_slice = gdm_dataset.sel(longitude=lon_slice).longitude
    xres = float((lon_slice[-1] - lon_slice[0]) / len(lon_slice))
    yres = float((lat_slice[-1] - lat_slice[0]) / len(lat_slice))
    transform = Affine.translation(
        lon_slice[0], lat_slice[0]) * Affine.scale(xres, yres)

    with rasterio.open(
        target_path,
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
        new_dataset.write(local_slice, 1)

    plt.show()


if __name__ == '__main__':
    main()
