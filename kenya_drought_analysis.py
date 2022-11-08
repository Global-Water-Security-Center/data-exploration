"""Demo of how to slice the GDM dataset by date range and save to geotiff"""
import collections

import geopandas
import numpy
import rioxarray

COUNTRY_NAME = 'Kenya'
START_DATE = '2012-01-01'
END_DATE = '2022-03-01'


def main():
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
    gdm_dataset = gdm_dataset.rio.clip(country_geom)
    unique_values = numpy.unique(gdm_dataset.drought)
    val_to_count_map = collections.defaultdict(int)
    with open(f'{COUNTRY_NAME}_{START_DATE}_{END_DATE}_drought.csv', 'w') as out_table_file:
        out_table_file.write('date,'+','.join([str(x) for x in unique_values])+'\n')
        for time_index in gdm_dataset.time:
            unique_vals_list, unique_count_list = numpy.unique(gdm_dataset.sel(time=time_index).drought, return_counts=True)
            val_to_count_map.update({
                val: count for val, count in zip(unique_vals_list, unique_count_list)
            })
            print(time_index.values.item())
            out_table_file.write(f'{(time_index.values.item()).strftime("%Y-%m-%d")},' + ','.join([str(val_to_count_map[val]) for val in unique_values])+'\n')


if __name__ == '__main__':
    main()
