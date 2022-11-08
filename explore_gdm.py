"""Demo of how to slice the GDM dataset by date range and save to geotiff"""
from rasterio.transform import Affine
import geopandas
import numpy
import pandas
import rasterio
import xarray

COUNTRY_NAME = 'Kenya'
START_DATE = '2012-01-01'
END_DATE = '2022-03-01'


def main():
    # gdm_dataset = xarray.open_dataset('nclimdiv.nc')
    # print(gdm_dataset)
    # return

    # countries.gpkg can be downloaded from https://github.com/tsamsonov/r-geo-course/blob/master/data/ne/countries.gpkg
    countries_vector = geopandas.read_file('countries.gpkg')
    country_bounds = countries_vector[
        countries_vector.name == COUNTRY_NAME].bounds

    # this was hardcoded in the sample code i got from AER
    gdm_dataset = xarray.open_dataset(
        'http://h2o-dev.aer-aws-nonprod.net/thredds/dodsC/gwsc/gdm')
    print(gdm_dataset)
    print('*')
    # GDM has dates in the byte format of '2012-01-01T00:00:00Z', so this
    # converts it to that
    # fix the S64 coding to a real datetime
    gdm_dataset['time'] = pandas.DatetimeIndex(gdm_dataset.time.str.decode('utf-8').astype('datetime64'))
    print(gdm_dataset)
    print('*')
    # # this one from nick that he could open in ArcGIS
    # gdm_dataset = xarray.open_dataset("H2O_GlobalDroughtIndex_ECMWF-ERA5_2022-07_2022-09.nc")
    # print(gdm_dataset)
    # print('*')

    date_range = pandas.date_range(start=START_DATE, end=END_DATE, freq='MS')
    print(date_range)

    # lat slice goes from pos to neg because it's 90 to -90 degrees so max first
    lat_slice = slice(float(country_bounds.maxy), float(country_bounds.miny))
    # lng slice is from -180 to 180 so min first
    lon_slice = slice(float(country_bounds.minx), float(country_bounds.maxx))

    local_slice = gdm_dataset.sel(
        lat=lat_slice,
        lon=lon_slice,
        time=date_range)

    print(numpy.unique(local_slice.drought))

    netcdf_path = f'{COUNTRY_NAME}_drought_{START_DATE}_{END_DATE}.nc'
    print(f'saving to {netcdf_path}')
    local_slice.to_netcdf(netcdf_path)
    print(f'verifying {netcdf_path} saved correctly')
    local_dataset = xarray.open_dataset(netcdf_path)
    print(local_dataset)
    return

    # get exact coords for correct geotransform
    lat_slice = gdm_dataset.sel(lat=lat_slice).lat
    lon_slice = gdm_dataset.sel(lon=lon_slice).lon
    xres = float((lon_slice[-1] - lon_slice[0]) / len(lon_slice))
    yres = float((lat_slice[-1] - lat_slice[0]) / len(lat_slice))
    transform = Affine.translation(
        lon_slice[0], lat_slice[0]) * Affine.scale(xres, yres)

    with rasterio.open(
        f"{COUNTRY_NAME}_drought_{START_DATE}_{END_DATE}.tif",
        mode="w",
        driver="GTiff",
        height=len(lat_slice),
        width=len(lon_slice),
        count=len(date_range),
        dtype=numpy.uint8,
        nodata=0,
        crs="+proj=latlong",
        transform=transform,
        kwargs={
            'tiled': 'YES',
            'COMPRESS': 'LZW',
            'PREDICTOR': 2}) as new_dataset:
        for year_index in range(len(date_range)):
            print(f'writing band {year_index} of {len(date_range)}')
            new_dataset.write(
                local_slice.drought[year_index, :], 1+year_index)


if __name__ == '__main__':
    main()
