"""See `python scriptname.py --help"""
from concurrent.futures import ThreadPoolExecutor
import argparse
import collections
import os
import shutil
import zipfile

from osgeo import gdal
import matplotlib.colors
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.lines as mlines
import numpy


CACHE_DIR = '_cmip6_local_cache'
LOCAL_WORKSPACE = r'D:\cmip6_workspace'
os.makedirs(LOCAL_WORKSPACE, exist_ok=True)


def line_seg(n, max_n):
    dashes = [max_n]
    for index, digit in enumerate(bin(n)[2:]):
        dashes.append(1+2**index*int(digit))
    return dashes


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
    tries = 0
    while True:
        if not os.path.exists(zip_path):
            shutil.copy(file_path, zip_path)
        try:
            yearly_val = unzip_and_sum_yearly_pr(zip_path, [float(v) for v in point])
            os.remove(zip_path)
            print(f'done with {zip_path}')
            return yearly_val
        except Exception:
            if tries == 0:
                print(f'error processing {zip_path}, continuing')
                os.remove(zip_path)
                tries += 1
            return 0.0


def main():
    parser = argparse.ArgumentParser(description=(
        'A script used to generate box plots to interpret CMIP6 raw data.'))
    parser.add_argument('point', nargs=2, help='lat/lng point to analyze')
    args = parser.parse_args()

    variable_to_process = 'pr'
    variant_set = set()
    for scenario_to_process, start_year, end_year in [
            ('historical', 1950, 2020),
            ('ssp245', 2021, 2100),
            ('ssp370', 2021, 2010)
            ]:
        with ThreadPoolExecutor(4) as executor:
            model_index = {}
            variant_index = {}
            variant_count = collections.defaultdict(set)
            future_workers = {}
            # TODO for debugging:
            # models_to_run = []
            for file_path in iterate_files(CACHE_DIR):
                _, _, variable, scenario, model, variant, _ = file_path.split(
                    os.path.sep)
                if variable != variable_to_process:
                    continue
                if scenario_to_process != scenario:
                    continue
                # # TODO: for debugging, do one model
                # if len(models_to_run) < 2 and model not in models_to_run:
                #     models_to_run.append(model)
                # elif model not in models_to_run:
                #     continue
                zip_path = os.path.join(
                    LOCAL_WORKSPACE, os.path.basename(file_path))
                year = int(zip_path[-8:-4])
                if year < start_year or year > end_year:
                    continue
                if model not in model_index:
                    model_index[model] = len(model_index)
                if variant not in variant_index:
                    variant_index[variant] = len(variant_index)
                variant_set.add(variant)
                variant_count[model].add(variant)
                future_workers[(year, scenario, model, variant)] = executor.submit(
                    process_file, file_path, zip_path, args.point)

        # at this point, all tasks are complete
        model_to_variant_data = collections.defaultdict(
            lambda: [0]*(end_year-start_year+1))

        for index, ((year, scenario, model, variant), future) in enumerate(future_workers.items()):
            yearly_val = future.result()
            print(f'done with {index} of {len(future_workers)}')
            # this is a part i need to fix -- we might not get ALL the yearly
            # values in it so we'll need to pre-allocate
            model_to_variant_data[(model, variant)][
                year-start_year] = yearly_val
        if len(model_to_variant_data) == 0:
            continue

        #cmap = plt.cm.get_cmap('tab20')
        HSV_tuples = [
            (x*1.0/len(model_index), 1.0, 1.0)
            for x in range(len(model_index))]
        color = [matplotlib.colors.hsv_to_rgb(x) for x in HSV_tuples]

        # Generate colors and line styles
        #color = [cmap(i % cmap.N) for i in range(len(model_index))]
        linestyles = [
            line_seg(i, len(variant_index)) for i in range(len(variant_index))]
        # create a new figure
        fig, ax = plt.subplots(2, figsize=(15, 15))
        labeled_models = set()
        variant_series = collections.defaultdict(list)
        year_list = list(range(start_year, end_year+1, 1))
        for (model, variant), yearly_val_list in model_to_variant_data.items():
            # plot the data
            label = None
            if model not in labeled_models:
                label = model
                labeled_models.add(model)
            line, = ax[0].plot(
                year_list,
                yearly_val_list,
                label=label,
                linewidth=1,
                color=color[model_index[model]],
                #linestyle=linestyles[variant_index[variant]],
                alpha=0.5,
                )
            line.set_dashes(linestyles[variant_index[variant]])

            # format the ticks
            ax[0].xaxis.set_major_locator(mdates.DayLocator(interval=30))  # adjust interval for your needs
            ax[0].xaxis.set_major_formatter(mdates.DateFormatter('%Y'))

            variant_series[model].append(yearly_val_list)
        model_list = []
        for model, variant_series in variant_series.items():
            # plot mean and standard deviation
            mean = numpy.mean(variant_series, axis=0)
            std = numpy.std(variant_series, axis=0)
            line, ax[1].plot(
                year_list, mean,
                color=color[model_index[model]])
            ax[1].fill_between(
                year_list, mean - std, mean + std,
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
            label=f'{model} ({len(variant_count[model])})')
            for model in model_index]

        # for handle, (_, variant) in zip(handles, model_to_variant_data):
        #     handle.set_dashes(linestyles[variant_index[variant]])

        # for handle, variant in zip(handles, variant_index.values()):
        #     handle.set_dashes(linestyles[variant_index[variant]])
        ax[0].legend(handles=handles)

        plt.tight_layout()
        plt.savefig(f'yearly_{scenario_to_process}_{variable_to_process}.png')
        plt.clf()


if __name__ == '__main__':
    main()
