import argparse
import collections
import datetime
import os
import shutil
import zipfile

from osgeo import gdal
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.lines as mlines
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
                date = full_path[-14:-4]
                val = get_val(full_path, point)
                date_as_int = [int(v) for v in date.split('-')]
                val_list.append((datetime.date(*date_as_int), val))
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
    parser.add_argument('date_range', nargs='+', help='list of years to analyze')
    parser.add_argument('point', nargs=2, help='lat/lng point to analyze')
    args = parser.parse_args()

    variable_to_process = 'pr'
    for year in args.date_range:
        model_to_variant_data = collections.defaultdict(list)
        colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']
        model_to_color = {}
        for file_path in iterate_files(CACHE_DIR, year, year):
            print(file_path)
            _, _, variable, scenario, model, variant, _ = file_path.split(
                os.path.sep)
            if variable != variable_to_process:
                continue
            if model not in model_to_color:
                if colors:
                    model_to_color[model] = colors.pop()
                else:
                    continue
            shutil.copy(file_path, LOCAL_WORKSPACE)
            zip_path = os.path.join(LOCAL_WORKSPACE, os.path.basename(file_path))
            val_list = unzip_and_iterate(zip_path, [float(v) for v in args.point])
            model_to_variant_data[(variable, scenario, model)].append(
                (variant, val_list))
            print(variable, scenario, model, variant)

        # create a new figure
        fig, ax = plt.subplots(2, figsize=(10, 10))
        labeled_models = set()
        for (variable, scenario, model), variant_val_list in model_to_variant_data.items():
            # plot the data
            label = None
            if model not in labeled_models:
                label = model
                labeled_models.add(model)

            for variant, val_list in variant_val_list:
                dates, values = zip(*val_list)
                cum_sum = numpy.cumsum(values)
                if cum_sum[-1] > 1000:
                    print(variable, scenario, model, variant, val_list)
                    raise RuntimeError()
                ax[0].plot(
                    dates,
                    cum_sum,
                    label=label,
                    linewidth=1,
                    color=model_to_color[model],
                    alpha=0.5,
                    )

            # format the ticks
            ax[0].xaxis.set_major_locator(mdates.DayLocator(interval=30))  # adjust interval for your needs
            ax[0].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

            # plot mean and standard deviation
            variant_series = [list(zip(*v[1]))[1] for v in variant_val_list]
            mean = numpy.mean(variant_series, axis=0)
            std = numpy.std(variant_series, axis=0)
            ax[1].plot(dates, mean, color=model_to_color[model])
            ax[1].fill_between(dates, mean - std, mean + std, color=model_to_color[model], alpha=0.2)

        # rotate dates for better display
        plt.gcf().autofmt_xdate()

        # Add labels and title
        ax[0].set_xlabel('Date')
        ax[0].set_xlabel(f'{variable}')
        ax[1].set_xlabel('Date')
        ax[1].set_xlabel(f'{variable}')
        plt.title(f'{variable} from in {year} at {args.point}')

        # Add a legend
        handles = [mlines.Line2D([], [], color=color, label=model) for model, color in model_to_color.items()]
        ax[0].legend(handles=handles)


        plt.tight_layout()
        plt.savefig(f'{year}_{variable}.tif')
        plt.clf()

if __name__ == '__main__':
    main()
