import logging
import sys
import argparse
import csv
import glob
import os

from ecoshard import geoprocessing
from matplotlib.colors import LinearSegmentedColormap
from osgeo import gdal
from ecoshard import taskgraph
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.colors as mcolors

CUSTOM_STYLE_DIR = 'custom_styles'

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
logging.getLogger('fetch_data').setLevel(logging.INFO)


def read_raster_csv(file_path):
    raster_dict = {}
    with open(file_path, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        current_raster_name = ""
        for i, row in enumerate(reader):
            if i % 4 == 0:
                current_raster_name = row[0]
                raster_dict[current_raster_name] = {}
            elif i % 4 == 1:
                raster_dict[current_raster_name]['position'] = [float(x) for x in row[1:] if x != '']
            elif i % 4 == 2:
                raster_dict[current_raster_name]['color'] = [x for x in row[1:] if x != '']
            elif i % 4 == 3:
                raster_dict[current_raster_name]['transparency'] = [int(x) for x in row[1:]  if x != '']
    return raster_dict


def hex_to_rgba(hex_code, transparency):
    hex_code = hex_code.lstrip('#')
    rgb = tuple(int(hex_code[i:i+2], 16)/255.0 for i in (0, 2, 4))
    alpha = transparency / 100  # Convert 0-100 scale to 0-1 scale
    return rgb + (alpha,)


CUSTOM_STYLES = {}
for style_file_path in glob.glob(os.path.join(CUSTOM_STYLE_DIR, '*.csv')):
    CUSTOM_STYLES.update(read_raster_csv(style_file_path))


def compute_hillshade(dem_path, hillshade_output_path):
    dem_ds = gdal.Open(dem_path)
    gdal.DEMProcessing(hillshade_output_path, dem_ds, 'hillshade')


def interpolated_colormap(cmap_name, N=100):
    try:
        # Get the original colormap from matplotlib
        original_cmap = plt.colormaps[cmap_name]
    except:
        # Handle custom colormap dictionary
        custom_cmap_info = CUSTOM_STYLES[cmap_name]
        positions = custom_cmap_info['position']
        hex_colors = custom_cmap_info['color']
        transparency = custom_cmap_info['transparency']

        # Convert hex to RGBA format and include transparency
        rgba_colors = [
            hex_to_rgba(hex_colors[i], transparency[i])
            for i in range(len(hex_colors))]

        # positions are from 0..100 in the csv
        original_cmap = LinearSegmentedColormap.from_list(
            'custom_cmap', list(zip(np.array(positions)/100, rgba_colors)))

    # Use linspace to get N interpolated colors from the original colormap
    colors = original_cmap(np.linspace(0, 1, N))
    # Create a new colormap from these colors
    new_cmap = LinearSegmentedColormap.from_list(
        cmap_name + "_interp", colors, N=N)
    return new_cmap


def degrees_per_pixel(dpi):
    earth_circumference_km = 40075  # Earth's average circumference in km
    km_per_degree = earth_circumference_km / 360.0
    inches_per_degree = km_per_degree * 1000 * 100 / 2.54  # Convert km to inches
    deg_per_pixel = 1 / (dpi * inches_per_degree)
    return deg_per_pixel


def warp_and_set_valid_nodata(
        base_raster_path, target_pixel_size, bounding_box,
        boundary_vector_path, where_filter, working_dir,
        resample_method,
        target_raster_path):
    if where_filter is not None:
        field_id, field_value = where_filter.split('=')
    raster_info = geoprocessing.get_raster_info(base_raster_path)
    target_nodata = raster_info['nodata'][0]
    if target_nodata is None:
        target_nodata = -9999
    geoprocessing.warp_raster(
        base_raster_path, target_pixel_size, target_raster_path,
        resample_method, target_bb=bounding_box,
        vector_mask_options={
            'mask_vector_path': boundary_vector_path,
            'mask_vector_where_filter': (
                None if where_filter is None else
                f'"{field_id}"="{field_value}"'),
            'target_mask_value': target_nodata,
            'all_touched': True,
            },
        working_dir=working_dir)
    r = gdal.OpenEx(target_raster_path, gdal.OF_RASTER | gdal.GA_Update)
    b = r.GetRasterBand(1)
    b.SetNoDataValue(target_nodata)
    b = None
    r = None


def main():
    parser = argparse.ArgumentParser(description=('Style a raster.'))
    parser.add_argument('raster_path', help=('Path to raw geotiff to style'))
    parser.add_argument(
        'boundary_vector_path', help='Path to boundary vector')
    parser.add_argument(
        '--zoom_level', default=1, type=float,
        help='Zoom level to decrease the base raster_path pixel size by.')
    parser.add_argument(
        '--resample_method', default='bilinear', help=(
            'one of near|bilinear|cubic|cubicspline|lanczos|average|mode|max'
            '|min|med|q1|q3'))
    parser.add_argument(
        '--where_filter', help=(
            'A string of the form "field=val" where field is the field in '
            'the boundary_vector_path and val is the value to filter by. '
            'ex: sov_a3=IND'))
    parser.add_argument(
        '--dem_path', help='Path to DEM used for hillshade styling.')
    parser.add_argument(
        '--cmap', default='turbo',
        help='Colomap, must be one of: ' + ', '.join(plt.colormaps()))

    args = parser.parse_args()

    # clip the raster path and DEM path to the boundary subset
    gdf = gpd.read_file(args.boundary_vector_path)
    field_id, field_value = args.where_filter.split('=')
    filtered_gdf = gdf[gdf[field_id].isin([field_value])]
    bounding_box = filtered_gdf.unary_union.envelope.bounds
    target_pixel_size = [
        v/args.zoom_level
        for v in geoprocessing.get_raster_info(args.raster_path)['pixel_size']]

    # warp `raster_path` and DEM_PATH to the bounding box and target pixel size
    working_dir = f'working_dir/{args.zoom_level}'
    os.makedirs(working_dir, exist_ok=True)
    task_graph = taskgraph.TaskGraph(working_dir, -1)
    warped_raster_path = os.path.join(working_dir, 'warped_raster.tif')
    warped_dem_path = os.path.join(working_dir, 'warped_dem.tif')
    for base_raster_path, target_raster_path, where_filter in [
            (args.raster_path, warped_raster_path, args.where_filter),
            (args.dem_path, warped_dem_path, None)]:
        if base_raster_path is not None:
            print(f'warping and clipping {base_raster_path}')
            task_graph.add_task(
                func=warp_and_set_valid_nodata,
                args=(
                    base_raster_path, target_pixel_size, bounding_box,
                    args.boundary_vector_path, where_filter, working_dir,
                    args.resample_method,
                    target_raster_path),
                target_path_list=[target_raster_path],
                task_name=f'warp {base_raster_path}')

    if args.dem_path is not None:
        hillshade_output_path = os.path.join(working_dir, 'hillshade.tif')
        print('calculating hillshade')
        task_graph.add_task(
            func=compute_hillshade,
            args=(warped_dem_path, hillshade_output_path),
            target_path_list=[hillshade_output_path],
            task_name='calculating hillshade')

    task_graph.join()
    task_graph.close()

    base_array = gdal.OpenEx(warped_raster_path, gdal.OF_RASTER).ReadAsArray()

    # Create a color gradient
    cm = interpolated_colormap(args.cmap)
    no_data_color = [0, 0, 0, 0]  # Assuming a black NoData color with full transparency

    nodata = geoprocessing.get_raster_info(warped_raster_path)['nodata'][0]
    nodata_mask = (base_array == nodata) | np.isnan(base_array)
    styled_array = np.empty(base_array.shape + (4,), dtype=float)
    valid_base_array = base_array[~nodata_mask]
    base_min, base_max = np.min(valid_base_array), np.max(valid_base_array)

    normalized_array = valid_base_array/(base_max-base_min)

    lower_quantile = np.percentile(valid_base_array, 0)
    upper_quantile = np.percentile(valid_base_array, 100)
    normalized_array = (valid_base_array - lower_quantile) / (
        upper_quantile - lower_quantile)

    styled_array[~nodata_mask] = cm(normalized_array)
    styled_array[nodata_mask] = no_data_color
    fig, ax = plt.subplots(figsize=(10, 10))

    extend_bb = [bounding_box[i] for i in (0, 2, 1, 3)]

    if args.dem_path is not None:
        hillshade_array = gdal.OpenEx(hillshade_output_path, gdal.OF_RASTER).ReadAsArray()
        hillshade_array = (
            hillshade_array-np.min(hillshade_array))/(
            np.max(hillshade_array)-np.min(hillshade_array))
        # adjust brightness, not hue
        styled_hsv = mcolors.rgb_to_hsv(styled_array[..., :3])
        min_brightness = 0.3
        scaled_hillshade = min_brightness + (1.0 - min_brightness) * hillshade_array
        styled_hsv[..., 2] *= scaled_hillshade
        styled_rgb = mcolors.hsv_to_rgb(styled_hsv)
        if styled_array.shape[2] == 4:
            alpha_channel = styled_array[..., 3]
            # Perform alpha blending
            blended_rgb = alpha_channel[..., np.newaxis] * styled_rgb + (1 - alpha_channel[..., np.newaxis]) * scaled_hillshade[..., np.newaxis]
            # Update the RGB channels with the blended colors
            styled_array = np.dstack((blended_rgb, np.ones(alpha_channel.shape)))
            styled_array[nodata_mask] = no_data_color
        else:
            styled_array = styled_rgb
        styled_array = np.clip(styled_array, 0, 1)

    ax.imshow(styled_array,  extent=extend_bb, origin='upper')
    filtered_gdf.boundary.plot(ax=ax, color='black', linewidth=1)
    ax.axis('off')
    plt.tight_layout()
    basename = os.path.basename(os.path.splitext(args.raster_path)[0])
    if args.dem_path is not None:
        basename += '_hillshade'
    if args.where_filter is not None:
        basename += f'_{args.where_filter}'
    plt.savefig(f'{basename}_{args.zoom_level}.png')
    plt.show()


if __name__ == '__main__':
    main()
