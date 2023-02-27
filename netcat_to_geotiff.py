"""Netcat to COG geotiff."""
import glob
import os

import argparse
import pandas
import numpy
import rasterio
from rasterio.transform import Affine
import xarray


def main():
    """Entrypoint."""
    parser = argparse.ArgumentParser(description=(
        'not a command line script -- used to process local '
        '`Kenya_drought_2012-01-01_2022-03-01_v2.nc`'))
    for nc_path in glob.glob(r"D:\repositories\data-exploration\Kenya_drought_2012-01-01_2022-03-01_v2.nc"):

        gdm_dataset = xarray.open_dataset(nc_path)
        basename = os.path.basename(os.path.splitext(nc_path)[0])

        #print(basename, gdm_dataset)
        # get exact coords for correct geotransform
        xres = float((gdm_dataset.lon[-1] - gdm_dataset.lon[0]) / len(gdm_dataset.lon))
        yres = float((gdm_dataset.lat[-1] - gdm_dataset.lat[0]) / len(gdm_dataset.lat))
        transform = Affine.translation(
            gdm_dataset.lon[0], gdm_dataset.lat[0]) * Affine.scale(xres, yres)

        with rasterio.open(
            f"{basename}_from_nc.tif",
            mode="w",
            driver="GTiff",
            height=len(gdm_dataset.lat),
            width=len(gdm_dataset.lon),
            count=len(gdm_dataset.time),
            dtype=numpy.float32,
            nodata=0,
            crs="+proj=latlong",
            transform=transform,
            kwargs={
                'tiled': 'YES',
                'COMPRESS': 'LZW',
                'PREDICTOR': 2}) as new_dataset:
            gdm_dataset = gdm_dataset.sel(
                time=pandas.date_range(start='2012-01-01', end='2022-03-01', freq='MS'))
            for date_index in range(len(gdm_dataset.time)):
                # there's only one so just get the first one

                data_key = next(iter(gdm_dataset.isel(time=date_index).data_vars))
                #print(next(iter(gdm_dataset.isel(time=date_index).data_vars)))
                print(f'writing band {basename} {date_index} of {len(gdm_dataset.time)}')
                #print(gdm_dataset.isel(time=date_index)[data_key])
                #new_dataset.write(gdm_dataset.isel(time=date_index)[data_key], 1+date_index)
                new_dataset.write(gdm_dataset.isel(time=date_index)[data_key], 1+date_index)


if __name__ == '__main__':
    main()