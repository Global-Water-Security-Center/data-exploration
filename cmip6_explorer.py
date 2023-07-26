import argparse
import datetime
import os
import shutil
import zipfile

from osgeo import gdal
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy


CACHE_DIR = '_cmip6_local_cache'
LOCAL_WORKSPACE = r'D:\cmip6_workspace'
os.makedirs(LOCAL_WORKSPACE, exist_ok=True)


def iterate_files(directory, start_year, end_year):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if not file.endswith('.zip'):
                continue
            year = file[-8:-4]
            if start_year <= year <= end_year:
                yield(os.path.join(root, file))


def unzip_and_iterate(zip_filepath, point):
    val_list = []
    with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
        for filename in zip_ref.namelist():
            # Check if it's a file (not a directory)
            if not filename.endswith('/'):
                # Extract each file to dest_dir
                zip_ref.extract(filename, LOCAL_WORKSPACE)

                # Get the full path of the extracted file
                full_path = os.path.join(LOCAL_WORKSPACE, filename)
                print(full_path)
                date = full_path[-14:-4]
                val = get_val(full_path, point)
                date_as_int = [int(v) for v in date.split('-')]
                val_list.append((datetime.date(*date_as_int), val))
                print(date, val)
                os.remove(full_path)
    return val_list


def get_val(file_path, point):
    raster = gdal.Open(file_path)
    gt = raster.GetGeoTransform()
    x_pixel = int((point[0] - gt[0]) / gt[1])
    y_pixel = int((point[1] - gt[3]) / gt[5])
    array = raster.GetRasterBand(1).ReadAsArray()
    pixel_value = array[y_pixel, x_pixel]
    return pixel_value


def main():
    parser = argparse.ArgumentParser(description=(
        'Process CMIP6 raw urls to geotiff.'))
    parser.add_argument('date_range', nargs=2, help='url list')
    parser.add_argument('point', nargs=2, help='lat/lng point to analyze')
    args = parser.parse_args()

    for file_path in iterate_files(CACHE_DIR, *args.date_range):
        print(file_path)
        _, _, variable, scenario, model, variant, _ = file_path.split(os.path.sep)
        print(variable, scenario, model, variant)
        shutil.copy(file_path, LOCAL_WORKSPACE)
        zip_path = os.path.join(LOCAL_WORKSPACE, os.path.basename(file_path))
        val_list = unzip_and_iterate(zip_path, [float(v) for v in args.point])
        print(val_list)
        break

    # create a new figure
    fig, ax = plt.subplots()

    # plot the data
    dates, values = zip(*val_list)
    ax.plot(dates, numpy.cumsum(values))

    # format the ticks
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=30))  # adjust interval for your needs
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    # rotate dates for better display
    plt.gcf().autofmt_xdate()

    plt.show()


if __name__ == '__main__':
    main()
