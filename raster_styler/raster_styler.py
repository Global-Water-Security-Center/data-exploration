import rasterio
from affine import Affine
from matplotlib.colors import LinearSegmentedColormap
from osgeo import gdal
from rasterio import features
from scipy.ndimage import zoom
from shapely.geometry import box
import argparse
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np

import richdem as rd


def compute_hillshade(dem_array, resolution):
    dem = rd.rdarray(dem_array, no_data=-9999)
    hillshade_val = rd.TerrainAttribute(dem, attrib='hillshade')
    return hillshade_val

# Your DEM resampling code...



DEM_PATH = r'D:\local_global_swy_data\global_dem_3s_md5_22d0c3809af491fa09d03002bdf09748\dem.vrt'
ZOOM_FACTOR = 4  # Replace with the desired zoom factor

def main():
    parser = argparse.ArgumentParser(description=('Style a raster.'))
    parser.add_argument('raster_path', help=('Path to raw geotiff to style'))
    parser.add_argument('boundary_vector_path', help=('Path to boundary vector'))
    args = parser.parse_args()

    # Load the raster file
    ds = gdal.Open(args.raster_path)
    band = ds.GetRasterBand(1)
    array = band.ReadAsArray()
    nodata_value = band.GetNoDataValue()

    # Mask the nodata values
    nodata_mask = np.isnan(array)
    if nodata_value is not None:
        nodata_mask &= (array == nodata_value)
    print(nodata_mask)

    # Normalize the raster values to the range [0, 1]
    print(array.dtype)
    masked_array = np.ma.masked_where(nodata_mask, array)
    array_min = masked_array.min()
    array_max = masked_array.max()
    print(array_min, array_max)
    norm_array = (masked_array - array_min) / (array_max - array_min)

    # Increase the resolution of the raster with bilinear filtering
    resampled_array = zoom(norm_array, ZOOM_FACTOR, order=1)
    resampled_nodata_mask = zoom(nodata_mask, ZOOM_FACTOR, order=0)
    print(norm_array.shape)
    print(resampled_array.shape)

    # Apply the nodata mask after the zoom
    if nodata_value is not None:
        resampled_array = np.where(
            resampled_nodata_mask, nodata_value, resampled_array)
    #resampled_array = norm_array
    # Create a color gradient
    colors = [(1, 0, 0), (1, 1, 0), (0, 1, 0)]  # R -> Y -> G
    cmap_name = 'custom1'
    cm = LinearSegmentedColormap.from_list(cmap_name, colors, N=100)
    gdf = gpd.read_file(args.boundary_vector_path)
    filtered_gdf = gdf[gdf['sov_a3'].isin(['IND'])]

    gt = ds.GetGeoTransform()
    gt = list(gt)
    gt[1] /= ZOOM_FACTOR
    gt[5] /= ZOOM_FACTOR
    print(gt)
    raster_extent = [
        gt[0],
        gt[0] + gt[1] * ds.RasterXSize * ZOOM_FACTOR,
        gt[3] + gt[5] * ds.RasterYSize * ZOOM_FACTOR,
        gt[3]]
    raster_bbox = box(*[raster_extent[i] for i in (0, 2, 1, 3)])
    union_bbox = raster_bbox.intersection(filtered_gdf.unary_union.envelope)
    print(union_bbox)

    min_x, min_y, max_x, max_y = union_bbox.bounds
    ul_x, ul_y = (min_x - gt[0]) / gt[1], (max_y - gt[3]) / gt[5]
    lr_x, lr_y = (max_x - gt[0]) / gt[1], (min_y - gt[3]) / gt[5]
    print(ul_x, ul_y)
    print(lr_x, lr_y)
    print(resampled_array.shape)

    # Clip the styled_array using the bounding box in raster coordinates
    # Apply the color gradient to the resampled raster values
    clipped_resampled_array = resampled_array[int(ul_y):int(lr_y), int(ul_x):int(lr_x)]
    no_data_color = [0, 0, 0, 0]  # Assuming a black NoData color with full transparency

    # Define the transformation between pixel coordinates and geospatial coordinates.
    print(gt)
    transform = Affine(
        gt[1], gt[2], gt[0]+gt[1]/2/ZOOM_FACTOR,
        gt[4], gt[5], gt[3]+gt[5]/2/ZOOM_FACTOR)
    print(transform)
    mask = features.rasterize(
        shapes=((geom, 1) for geom in filtered_gdf.geometry),
        out_shape=resampled_array.shape, transform=transform, dtype=np.uint8,
        all_touched=True)

    clipped_mask = mask[int(ul_y):int(lr_y), int(ul_x):int(lr_x)]
    styled_array = cm(clipped_resampled_array)
    styled_array[clipped_mask == 0] = no_data_color


    with rasterio.open(DEM_PATH) as src:
        # Resample DEM to match resampled_array resolution
        dem_data = src.read(
            out_shape=(
                src.count,
                int(src.height * ZOOM_FACTOR),
                int(src.width * ZOOM_FACTOR)
            ),
            resampling=rasterio.enums.Resampling.bilinear
        )
        # Update the transform
        transform_dem = src.transform * src.transform.scale(
            (src.width / dem_data.shape[-1]),
            (src.height / dem_data.shape[-2])
        )
        dem = dem_data[0]

    hs_array = compute_hillshade(dem, resolution=gt[1])
    ul_x_dem, ul_y_dem = (min_x - transform_dem.c) / transform_dem.a, (max_y - transform_dem.f) / transform_dem.e
    lr_x_dem, lr_y_dem = (max_x - transform_dem.c) / transform_dem.a, (min_y - transform_dem.f) / transform_dem.e
    clipped_hs_array = hs_array[int(ul_y_dem):int(lr_y_dem), int(ul_x_dem):int(lr_x_dem)]





    # Plot the colored raster
    fig, ax = plt.subplots(figsize=(10, 10))
    ax.imshow(
        styled_array, extent=[min_x, max_x, min_y, max_y], origin='upper',
        cmap=cm)
    # Plot the bounding box of the union
    #gpd.GeoSeries(union_bbox).boundary.plot(ax=ax, color='blue', linewidth=2)

    # Plot the polygon boundaries of the filtered shapefile on top of the raster
    filtered_gdf.boundary.plot(ax=ax, color='black', linewidth=1)

    # Save the figure as a PNG image
    ax.axis('off')
    plt.tight_layout()
    plt.savefig('styled_raster_with_filtered_boundaries.png')


if __name__ == '__main__':
    main()
