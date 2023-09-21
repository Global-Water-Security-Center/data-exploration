"""See `python scriptname.py --help"""
import argparse
import glob

from osgeo import gdal
from ecoshard import geoprocessing

ERA5_RESOLUTION_M = 27830


def average(nodata, *array_list):
    nodata_mask = array_list[0] != nodata
    running_sum = array_list[0]
    for index, array in enumerate(array_list[1:]):
        running_sum[nodata_mask] += array[nodata_mask]
        print(index, array)
    print(len(array_list))
    running_sum[nodata_mask] /= len(array_list)-1
    return running_sum


def main():
    parser = argparse.ArgumentParser(description=(
        'Average the rasters in the argument list.'))
    parser.add_argument(
        'raster_path_pattern', help='Path to rasters')
    parser.add_argument(
        '--target_path', help='Path to target raster.')
    args = parser.parse_args()

    array_path_band_list = [(path, 1) for path in glob.glob(
        args.raster_path_pattern)]
    nodata = geoprocessing.get_raster_info(
        array_path_band_list[0][0])['nodata'][0]
    geoprocessing.raster_calculator(
        [(nodata, 'raw')]+array_path_band_list, average, args.target_path,
        gdal.GDT_Float32, nodata)


if __name__ == '__main__':
    main()
