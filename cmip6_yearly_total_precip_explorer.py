from concurrent.futures import ProcessPoolExecutor
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


def iterate_files(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if not file.endswith('.zip'):
                continue
            yield(os.path.join(root, file))


def unzip_and_sum_yearly_pr(zip_filepath, point):
    running_val = 0.0
    with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
        for filename in zip_ref.namelist():
            # Check if it's a file (not a directory)
            if not filename.endswith('/'):
                # Extract each file to dest_dir
                zip_ref.extract(filename, LOCAL_WORKSPACE)

                # Get the full path of the extracted file
                full_path = os.path.join(LOCAL_WORKSPACE, filename)
                running_val += get_val(full_path, point)*86400  # convert to mm
                os.remove(full_path)
                print(f'done with {full_path}')
    return running_val


def get_val(file_path, point):
    raster = gdal.Open(file_path)
    gt = raster.GetGeoTransform()
    x_pixel = int((point[0] - gt[0]) / gt[1])
    y_pixel = int((point[1] - gt[3]) / gt[5])
    array = raster.GetRasterBand(1).ReadAsArray()
    pixel_value = array[y_pixel, x_pixel]
    return pixel_value


def process_file(file_path, zip_path, point):
    print(f'processing {zip_path}')
    if not os.path.exists(zip_path):
        shutil.copy(file_path, zip_path)
    try:
        yearly_val = unzip_and_sum_yearly_pr(zip_path, [float(v) for v in point])
        print(f'done with {zip_path}')
        return yearly_val
    except Exception:
        print(f'error processing {zip_path}, continuing')


def main():
    parser = argparse.ArgumentParser(description=(
        'Process CMIP6 raw urls to geotiff.'))
    parser.add_argument('point', nargs=2, help='lat/lng point to analyze')
    args = parser.parse_args()

    variable_to_process = 'pr'
    for scenario_to_process in ['historical', 'ssp245', 'ssp370']:
        with ProcessPoolExecutor(24*2) as executor:
            model_index = {}
            #models_to_run = []
            variant_count = collections.defaultdict(set)
            future_workers = {}
            for file_path in iterate_files(CACHE_DIR):
                _, _, variable, scenario, model, variant, _ = file_path.split(
                    os.path.sep)
                if variable != variable_to_process:
                    continue
                if scenario_to_process != scenario:
                    continue
                if model not in model_index:
                    model_index[model] = len(model_index)
                # #for debugging, do one model
                # if len(models_to_run) < 2 and model not in models_to_run:
                #     models_to_run.append(model)
                # elif model not in models_to_run:
                #     continue
                zip_path = os.path.join(
                    LOCAL_WORKSPACE, os.path.basename(file_path))
                year = zip_path[-8:-4]
                # if year < '2000' or year > '2010':
                #     continue
                # if variant not in variant_count[model] and len(variant_count[model]) > 2:
                #     continue
                variant_count[model].add(variant)
                future_workers[(year, scenario, model, variant)] = executor.submit(
                    process_file, file_path, zip_path, args.point)

        # at this point, all tasks are complete
        model_to_variant_data = collections.defaultdict(list)
        for index, ((year, scenario, model, variant), future) in enumerate(future_workers.items()):
            yearly_val = future.result()
            print(f'done with {index} of {len(future_workers)}')
            model_to_variant_data[(scenario, model, variant)].append(
                (year, yearly_val))
        if len(model_to_variant_data) == 0:
            continue

        cmap = plt.cm.get_cmap('tab20')

        # Generate colors and line styles
        color = [cmap(i % cmap.N) for i in range(len(model_index))]
        linestyles = ['-', '--', ':', '-.'] * (len(model_index) // 4 + 1)

        # create a new figure
        fig, ax = plt.subplots(2, figsize=(15, 15))
        labeled_models = set()
        variant_series = collections.defaultdict(list)
        for (scenario, model, variant), yearly_val_list in model_to_variant_data.items():
            # plot the data
            label = None
            if model not in labeled_models:
                label = model
                labeled_models.add(model)
            dates, values = zip(*sorted(yearly_val_list))
            ax[0].plot(
                dates,
                values,
                label=label,
                linewidth=1,
                color=color[model_index[model]],
                linestyle=linestyles[model_index[model]],
                alpha=0.5,
                )

            # format the ticks
            ax[0].xaxis.set_major_locator(mdates.DayLocator(interval=30))  # adjust interval for your needs
            ax[0].xaxis.set_major_formatter(mdates.DateFormatter('%Y'))

            variant_series[model].append((dates, values))
        for model, date_value_list in variant_series.items():
            # plot mean and standard deviation
            date_series, variant_series = zip(*date_value_list)
            mean = numpy.mean(variant_series, axis=0)
            std = numpy.std(variant_series, axis=0)
            ax[1].plot(
                date_series[0], mean,
                color=color[model_index[model]],
                linestyle=linestyles[model_index[model]])
            ax[1].fill_between(
                date_series[0], mean - std, mean + std,
                color=color[model_index[model]],
                alpha=0.2)

        # rotate dates for better display
        plt.gcf().autofmt_xdate()

        # Add labels and title
        ax[0].set_title('Group by model')
        ax[0].set_xlabel('Year')
        ax[0].set_ylabel(f'{variable} (mm)')
        ax[1].set_xlabel('Year')
        ax[1].set_ylabel(f'{variable} (mm)')
        plt.title(f'{variable_to_process} from {scenario_to_process} at {args.point}')

        # Add a legend
        handles = [mlines.Line2D(
            [], [],
            color=color[model_index[model]],
            linestyle=linestyles[model_index[model]],
            label=f'{model} ({len(variant_count[model])})')
            for model in model_index]
        ax[0].legend(handles=handles)

        plt.tight_layout()
        plt.savefig(f'yearly_{scenario_to_process}_{variable_to_process}.png')
        plt.clf()


if __name__ == '__main__':
    main()
