"""Demo of how to slice the GDM dataset by date range and save to geotiff"""
import collections
import argparse

import geopandas
import numpy
import rioxarray

COUNTRY_NAME = 'Kenya'
START_DATE = '2012-01-01'
END_DATE = '2022-03-01'


def main():
    parser = argparse.ArgumentParser(description=(
        'In development -- modification of extract hard coded '
        'Kenya drought data from CMIP5.'))
    parser.add_argument(
        'aoi_vector_path', help='Path to vector/shapefile of area of interest')
    parser.add_argument('--aggregate_by_field', help=(
        'If provided, this aggregates results by the unique values found in '
        'the field in `aoi_vector_path`'))
    parser.add_argument('start_date', type=str, help='start date YYYY-MM-DD')
    parser.add_argument('end_date', type=str, help='end date YYYY-MM-DD')
    parser.add_argument(
        '--authenticate', action='store_true',
        help='Pass this flag if you need to reauthenticate with GEE')
    # gdm_dataset = xarray.open_dataset('nclimdiv.nc')
    # print(gdm_dataset)
    # return

    # countries.gpkg can be downloaded from https://github.com/tsamsonov/r-geo-course/blob/master/data/ne/countries.gpkg
    countries_vector = geopandas.read_file('countries.gpkg')
    country_geom = countries_vector[
        countries_vector.name == COUNTRY_NAME].geometry

    # this was hardcoded in the sample code i got from AER
    # gdm_dataset = xarray.open_dataset(
    #     'http://h2o-dev.aer-aws-nonprod.net/thredds/dodsC/gwsc/gdm')

    gdm_dataset = rioxarray.open_rasterio('Kenya_drought_2012-01-01_2022-03-01_v2.nc')
    gdm_dataset = gdm_dataset.rio.write_crs(4326)
    gdm_dataset = gdm_dataset.rio.clip(country_geom, drop=True)
    unique_values = numpy.unique(gdm_dataset.drought)

    gdm_mask = rioxarray.open_rasterio("kenya_debug.tif")
    gdm_mask[:] = 1
    gdm_mask = gdm_mask.rio.clip(country_geom)
    valid_mask = (gdm_mask == 1)[0, :, :]
    print(valid_mask)
    # print(gdm_mask)

    # #print(basename, gdm_dataset)
    # # get exact coords for correct geotransform
    # xres = float((gdm_dataset.x[-1] - gdm_dataset.x[0]) / len(gdm_dataset.x))
    # yres = float((gdm_dataset.y[-1] - gdm_dataset.y[0]) / len(gdm_dataset.y))
    # transform = Affine.translation(
    #     gdm_dataset.x[0], gdm_dataset.y[0]) * Affine.scale(xres, yres)

    # with rasterio.open(
    #     f"kenya_debug.tif",
    #     mode="w",
    #     driver="GTiff",
    #     height=len(gdm_dataset.y),
    #     width=len(gdm_dataset.x),
    #     count=1,
    #     dtype=numpy.float32,
    #     nodata=0,
    #     crs="+proj=latlong",
    #     transform=transform,
    #     kwargs={
    #         'tiled': 'YES',
    #         'COMPRESS': 'LZW',
    #         'PREDICTOR': 2}) as new_dataset:
    #         new_dataset.write(gdm_mask, 1)
    #         return
    #         # gdm_dataset = gdm_dataset.sel(
    #         #     time=pandas.date_range(start='2012-01-01', end='2022-03-01', freq='MS'))
    #         # for date_index in range(len(gdm_dataset.time)):
    #         #     # there's only one so just get the first one
    #         #     data_key = next(iter(gdm_dataset.isel(time=date_index).data_vars))
    #         #     #print(next(iter(gdm_dataset.isel(time=date_index).data_vars)))
    #         #     print(f'writing band {date_index} of {len(gdm_dataset.time)}')
    #         #     #print(gdm_dataset.isel(time=date_index)[data_key])
    #         #     #new_dataset.write(gdm_dataset.isel(time=date_index)[data_key], 1+date_index)
    #         #     new_dataset.write(gdm_dataset.isel(time=date_index)[data_key], 1+date_index)
    #         #     break
    # return

    with open(f'{COUNTRY_NAME}_{START_DATE}_{END_DATE}_drought_v3.csv', 'w') as out_table_file:
        out_table_file.write('date,'+','.join([str(x) for x in unique_values])+'\n')
        for time_index in gdm_dataset.time:
            val_to_count_map = collections.defaultdict(int)
            print(gdm_dataset.sel(time=time_index).drought)
            unique_vals_list, unique_count_list = numpy.unique(gdm_dataset.sel(time=time_index).drought.values[valid_mask], return_counts=True)
            val_to_count_map.update({
                val: count for val, count in zip(unique_vals_list, unique_count_list)
            })
            print(time_index.values.item())
            out_table_file.write(f'{(time_index.values.item()).strftime("%Y-%m-%d")},' + ','.join([str(val_to_count_map[val]) for val in unique_values])+'\n')


if __name__ == '__main__':
    main()
