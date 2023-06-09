"""See `python scriptname.py --help"""
import glob
import os

import argparse
import numpy
import itertools
import rasterio
from rasterio.transform import Affine
import xarray


def main():
    """Entrypoint."""
    parser = argparse.ArgumentParser(description=(
        'Convert netcat files to geotiff'))
    parser.add_argument(
        'netcat_path', type=str,
        help='Path or pattern to netcat files to convert')
    parser.add_argument(
        'x_y_fields', nargs=2,
        help='the names of the x and y coordinates in the netcat file')
    parser.add_argument('--band_field', help=(
        'if defined, will use this coordinate as the band field'))
    parser.add_argument(
        '--target_nodata', type=float,
        help='Set this as target nodata value if desired')
    parser.add_argument('out_dir', help='path to output directory')
    args = parser.parse_args()
    _ = parser.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)
    for nc_path in glob.glob(args.netcat_path):
        print(f'processing {nc_path}')
        decode_times = True
        while True:
            try:
                dataset = xarray.open_dataset(
                    nc_path, decode_times=decode_times)
                break
            except ValueError:
                if decode_times is False:
                    raise
                decode_times = False
        res_list = []
        coord_list = []
        for coord_id, field_id in zip(['x', 'y'], args.x_y_fields):
            if field_id not in dataset.coords:
                raise ValueError(
                    f'Expected {field_id} in coordinates but only found '
                    f'{list(dataset.coords.keys())}')
            coord_array = dataset.coords[field_id]
            res_list.append(float(
                (coord_array[-1] - coord_array[0]) / len(coord_array)))
            coord_list.append(coord_array)

        n_bands = 1
        if args.band_field is not None:
            n_bands = len(dataset.coords[args.band_field])

        remaining_coords = (
            set(dataset.coords) - set(args.x_y_fields+[args.band_field]))

        # Reduce the dimensionality of the dataset using the coordinates
        coord_values = {
            coord_name: dataset[coord_name].values
            for coord_name in remaining_coords
        }

        # get all combinations
        combinations = list(itertools.product(*coord_values.values()))
        if not combinations:
            combinations = [None]

        transform = Affine.translation(
            *[a[0] for a in coord_list]) * Affine.scale(*res_list)

        basename = os.path.basename(os.path.splitext(nc_path)[0])

        for variable_name in dataset.keys():
            combination_suffix = ''
            local_dataset = dataset

            for combination in combinations:
                local_selector = {
                    key: value
                    for key, value in zip(coord_values.keys(), combination)}
                local_dataset = dataset.sel(**local_selector)
                combination_suffix = '_'+'_'.join([
                    f'{key}{value}' for key, value in local_selector.items()])
                if combination_suffix == '_':
                    combination_suffix = ''
                target_dir = os.path.join(args.out_dir, variable_name)
                os.makedirs(target_dir, exist_ok=True)
                filename = f"{basename}_{variable_name}{combination_suffix}.tif"
                print(filename)
                target_path = os.path.join(target_dir, filename)
                with rasterio.open(
                    target_path,
                    mode="w",
                    driver="GTiff",
                    height=len(coord_list[1]),
                    width=len(coord_list[0]),
                    count=n_bands,
                    dtype=numpy.float32,
                    nodata=None,
                    crs="+proj=latlong",
                    transform=transform,
                    **{
                        'tiled': 'YES',
                        'COMPRESS': 'LZW',
                        'PREDICTOR': 2}) as new_dataset:
                    print(f'writing {target_path}')
                    local_variable_ds = local_dataset[variable_name]
                    if len(local_variable_ds.dims) == 2:
                        new_dataset.write(
                            local_variable_ds.expand_dims('band'))
                    else:
                        new_dataset.write(local_variable_ds)

                    #local_dataset = local_dataset[variable_name].expand_dims('band')

                # gdm_dataset = gdm_dataset.sel(
                #     time=pandas.date_range(start='2012-01-01', end='2022-03-01', freq='MS'))
                # for date_index in range(len(gdm_dataset.time)):
                #     # there's only one so just get the first one
                #   data_key = next(iter(gdm_dataset.isel(time=date_index).data_vars))
                #   #print(next(iter(gdm_dataset.isel(time=date_index).data_vars)))
                #   print(f'writing band {basename} {date_index} of {len(gdm_dataset.time)}')
                #   #print(gdm_dataset.isel(time=date_index)[data_key])
                #   #new_dataset.write(gdm_dataset.isel(time=date_index)[data_key], 1+date_index)
                #   new_dataset.write(gdm_dataset.isel(time=date_index)[data_key], 1+date_index)


if __name__ == '__main__':
    main()