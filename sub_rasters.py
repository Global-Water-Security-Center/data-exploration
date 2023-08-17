"""See `python scriptname.py --help"""
import argparse

from osgeo import gdal
from ecoshard import geoprocessing

ERA5_RESOLUTION_M = 27830


def subtract(nodata, array_a, array_b):
    nodata_mask = array_a != nodata
    result = array_a
    result[nodata_mask] -= array_b[nodata_mask]
    return result


def main():
    parser = argparse.ArgumentParser(description=(
        'Calculate raster_a - raster_b.'))
    parser.add_argument('raster_a', help='Path to raster A')
    parser.add_argument('raster_b', help='Path to raster B')
    parser.add_argument('--target_path', help='Path to target raster.')
    args = parser.parse_args()

    array_path_band_list = [
        (path, 1) for path in [args.raster_a, args.raster_b]]
    nodata = geoprocessing.get_raster_info(
        array_path_band_list[0][0])['nodata'][0]
    geoprocessing.raster_calculator(
        [(nodata, 'raw')]+array_path_band_list, subtract, args.target_path,
        gdal.GDT_Float32, nodata)


if __name__ == '__main__':
    main()
