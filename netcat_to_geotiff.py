"""See `python scriptname.py --help"""
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
        'Convert netcat files to geotiff'))
    parser.add_argument(
        'netcat_path', type=str,
        help='Path or pattern to netcat files to convert')
    parser.add_argument(
        'x_y_fields', nargs=2,
        help='the names of the x and y coordinates in the netcat file')
    parser.add_argument(
        '--target_nodata', type=float,
        help='Set this as target nodata value if desired')
    parser.add_argument('out_dir', help='path to output directory')
    args = parser.parse_args()
    _ = parser.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)
    for nc_path in glob.glob(args.netcat_path):
        print(f'processing {nc_path}')
        dataset = xarray.open_dataset(nc_path)

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

        if len(dataset.coords) == 3:
            band_coord = set(dataset.coords).difference(args.x_y_fields).pop()
            n_bands = len(dataset.coords[band_coord])
        else:
            n_bands = 1

        transform = Affine.translation(
            *[a[0] for a in coord_list]) * Affine.scale(*res_list)

        basename = os.path.basename(os.path.splitext(nc_path)[0])

        for variable_name in dataset.keys():
            target_dir = os.path.join(args.out_dir, variable_name)
            os.makedirs(target_dir, exist_ok=True)
            with rasterio.open(
                os.path.join(target_dir, f"{basename}_{variable_name}.tif"),
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

                new_dataset.write(dataset[variable_name])

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