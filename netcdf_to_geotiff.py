"""See `python scriptname.py --help"""
import glob
import os
import tempfile
import shutil
import uuid

from ecoshard import geoprocessing
from osgeo import gdal
from osgeo import osr
from pathvalidate import sanitize_filename
from rasterio.transform import Affine
import argparse
import itertools
import numpy
import rasterio
import xarray


def main():
    """Entrypoint."""
    parser = argparse.ArgumentParser(description=(
        'Convert netcdf files to geotiff'))
    parser.add_argument(
        'netcdf_path', type=str,
        help='Path or pattern to netcdf files to convert')
    parser.add_argument(
        'x_y_fields', nargs=2,
        help='the names of the x and y coordinates in the netcdf file')
    parser.add_argument('--band_field', help=(
        'if defined, will use this coordinate as the band field'))
    parser.add_argument(
        '--target_nodata', type=float,
        help='Set this as target nodata value if desired')
    parser.add_argument('out_dir', help='path to output directory')
    args = parser.parse_args()
    _ = parser.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)
    print(f'processing {args.netcdf_path}')
    for nc_path in glob.glob(args.netcdf_path):
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

        # iterate through all the variables in the dataset
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
                target_path = os.path.join(target_dir, sanitize_filename(
                    filename, replacement_text="_"))
                # assume latlng
                crs_string = "+proj=latlong"
                src_array = local_dataset[variable_name]
                if len(src_array.dims) == 2:
                    src_array = src_array.expand_dims('band')

                if args.target_nodata is not None:
                    src_array = src_array.fillna(args.target_nodata)

                with rasterio.open(
                    target_path,
                    mode="w",
                    driver="GTiff",
                    height=len(coord_list[1]),
                    width=len(coord_list[0]),
                    count=n_bands,
                    dtype=numpy.float32,
                    nodata=args.target_nodata,
                    crs=crs_string,
                    transform=transform,
                    **{
                        'tiled': 'YES',
                        'COMPRESS': 'LZW',
                        'PREDICTOR': 2}) as new_dataset:
                    print(f'writing {target_path}')
                    new_dataset.write(src_array)

                warp_to_180(target_path)


def warp_to_180(local_raster_path):
    # if the netcdf file extends beyond 180 longitude, wrap it back to -180
    local_raster_info = geoprocessing.get_raster_info(local_raster_path)
    srs = osr.SpatialReference()
    srs.ImportFromWkt(local_raster_info['projection_wkt'])
    proj4_str = srs.ExportToProj4()
    vrt_dir = None
    if ('+proj=longlat' in proj4_str and
            local_raster_info['bounding_box'][2] > 180):
        vrt_dir = tempfile.mkdtemp(dir=os.path.dirname(local_raster_path))
        base_raster_path = copy_to_unique_file(local_raster_path, vrt_dir)
        vrt_local = os.path.join(vrt_dir, 'buffered.vrt')
        proj4_str += '+lon_wrap=180'
        bb = local_raster_info['bounding_box']
        vrt_pixel_size = local_raster_info['pixel_size']
        buffered_bounds = [
            _op(bb[i], bb[j])+offset*2 for _op, i, j, offset in [
                (min, 0, 2, -abs(vrt_pixel_size[0])),
                (max, 1, 3, abs(vrt_pixel_size[1])),
                (max, 0, 2, abs(vrt_pixel_size[0])),
                (min, 1, 3, -abs(vrt_pixel_size[1]))]]
        local_raster = gdal.OpenEx(base_raster_path, gdal.OF_RASTER)
        gdal.Translate(
            vrt_local, local_raster, format='VRT',
            outputBounds=buffered_bounds)
        local_raster = None
        base_raster_path = vrt_local

        target_bb = local_raster_info['bounding_box'].copy()
        if target_bb[2] > 180:
            target_bb[2] -= 180
            target_bb[0] -= 180

        geoprocessing.warp_raster(
            vrt_local, local_raster_info['pixel_size'], local_raster_path,
            'near',
            base_projection_wkt=proj4_str,
            target_projection_wkt='+proj=longlat',
            target_bb=target_bb)
        shutil.rmtree(vrt_dir)


def copy_to_unique_file(src_path, target_dir):
    ext = os.path.splitext(src_path)[1]
    unique_filename = str(uuid.uuid4()) + ext
    dst_path = os.path.join(target_dir, unique_filename)
    shutil.copy(src_path, dst_path)
    return dst_path


if __name__ == '__main__':
    main()
