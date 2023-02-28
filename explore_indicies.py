"""Explore the results of `climate-indicies`.

From here: https://climate-indices.readthedocs.io/en/latest/
"""
import argparse
import os

import geopandas
import matplotlib.pyplot as plt
import numpy
import pandas
import plotly.express as px
import rioxarray
import xarray

COUNTRY_NAME = 'Kenya'
START_DATE = '2012-01-01'
END_DATE = '2022-03-01'


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(description=(
        'Not a command line script. Used to explore how to extract rain '
        'events by watershed in a time range.'))
    _ = parser.parse_args()
    # countries.gpkg can be downloaded from https://github.com/tsamsonov/r-geo-course/blob/master/data/ne/countries.gpkg

    # x1 = [1, 3, 5, 7, 9]
    # y1 = np.random.random(5)

    # x2 = [2, 4, 6, 8, 10]
    # y2 = np.random.random(5)

    # df1 = pd.DataFrame({'name': ['1']*len(x1),
    #                     'x': x1,
    #                     'y': y1})

    # df2 = pd.DataFrame({'name': ['2']*len(x2),
    #                     'x': x2,
    #                     'y': y2})

    # df = pd.concat([df1, df2])

    # fig = px.line(df, x = 'x', y = 'y', color = 'name', markers = True)
    # fig.show()

    # countries_vector = geopandas.read_file('countries.gpkg')
    # country_geom = countries_vector[
    #     countries_vector.name == COUNTRY_NAME].geometry

    # print(rioxarray.open_rasterio('nclimgrid-prcp.nc'))
    # return

    dataframe_list = []
    n_month_avg = 1
    lat_index = 200
    lng_index = 500
    with open('index_exploration.csv', 'w') as index_table:
        index_table.write('dataset_name')
        for path in [
                "spi_spi_gamma_60.nc",
                "spi_spi_pearson_60.nc",
                "pnp_pnp_60.nc",
                "spei_spei_gamma_60.nc",
                "spei_spei_pearson_60.nc",
                "spei_pet_thornthwaite.nc",
                ]:
            label = '-'.join(os.path.splitext(path)[0].split('_')[-3:])
            climate_indicies = rioxarray.open_rasterio(path)
            print(climate_indicies)
            continue
            index_slice = climate_indicies[lat_index, lng_index, :]

            index_slice = numpy.convolve(index_slice, [1/n_month_avg]*n_month_avg, 'same')

            dataframe = pandas.DataFrame({
                'name': [label]*index_slice.size,
                'date': pandas.date_range(start='1895-01-01', periods=index_slice.size, freq='MS'),
                'y': index_slice})
            dataframe_list.append(dataframe)
            #print(climate_indicies.lat.shape)
            #print(climate_indicies.y.shape)
            #print(climate_indicies.long_name)
            #print(climate_indicies.shape)
            #print(climate_indicies.valid_max)
    return

        #print(numpy.count_nonzero(numpy.isnan(climate_indicies[:, :, 1000])))
    valid_min = climate_indicies.valid_min
    valid_max = climate_indicies.valid_min

    lat = (climate_indicies.geospatial_lat_max-climate_indicies.geospatial_lat_min)/climate_indicies.shape[0]*lat_index
    lng = (climate_indicies.geospatial_lon_max-climate_indicies.geospatial_lon_min)/climate_indicies.shape[1]*lng_index

    climate_indicies = rioxarray.open_rasterio('nclimgrid-tavg.nc')
    print(climate_indicies)
    index_slice = climate_indicies.tavg[:, lat_index, lng_index]
    index_slice = numpy.convolve(index_slice, [1/n_month_avg]*n_month_avg, 'same')

    dataframe = pandas.DataFrame({
        'name': ['nclimgrid-tavg']*index_slice.size,
        'date': pandas.date_range(start='1895-01-01', periods=index_slice.size, freq='MS'),
        'y': index_slice})
    dataframe_list.append(dataframe)

    climate_indicies = rioxarray.open_rasterio('nclimgrid-prcp.nc')
    print(climate_indicies)
    index_slice = climate_indicies.prcp[:, lat_index, lng_index]
    index_slice = numpy.convolve(index_slice, [1/n_month_avg]*n_month_avg, 'same')

    dataframe = pandas.DataFrame({
        'name': ['nclimgrid-prcp']*index_slice.size,
        'date': pandas.date_range(start='1895-01-01', periods=index_slice.size, freq='MS'),
        'y': index_slice})
    dataframe_list.append(dataframe)

    df = pandas.concat(dataframe_list)
    fig = px.line(df, title=f'Climate Indexes at lat={lat:.2f}, lng={lng:.2f} with 60 month smoothing', x='date', y='y', color='name', markers=True)
    fig.update_yaxes(range=[-4, numpy.max(index_slice)])
    fig.show()
    print(f"{lat:.2f}_lat_{lng:.2f}_lon_{n_month_avg}-smooth".replace('.', '_'))
    # lines = regional_means.plot.line(hue='region', add_legend=False)
    # labels = range(6)
    # plt.legend(lines, labels, ncol=2, loc='lower right')

    # print(climate_indicies.sel(
    #     lat=slice([24.56]),
    #     x=[0.5],
    #     y=[0.5]))
    return
    climate_indicies = climate_indicies.rio.write_crs(4326)
    climate_indicies = climate_indicies.rio.clip(country_geom)


if __name__ == '__main__':
    main()
