"""Sum precipitation in a watershed(s) over a given time period."""
from rasterio.transform import Affine
import geopandas
import numpy
import pandas
import rasterio
import xarray

START_DATE = '2022-05-01'
END_DATE = '2022-08-01'


def main():
    gdm_dataset = xarray.open_dataset('http://h2o-sandbox1.aer-aws-nonprod.net/thredds/dodsC/era5/daily-summary.nc')
    print(gdm_dataset)


    # TODO:
    # - get time range
    #   - clip by time range
    # - get watershed(s)
    #   - clip by watersheds
    # - print per day and total sum


    return
    # GDM has dates in the byte format of '2012-01-01T00:00:00Z', so this
    # converts it to that
    # fix the S64 coding to a real datetime
    gdm_dataset['time'] = pandas.DatetimeIndex(gdm_dataset.time.str.decode('utf-8').astype('datetime64'))
    date_range = pandas.date_range(start=START_DATE, end=END_DATE, freq='MS')

    vector = geopandas.read_file('europe_basins')
    vector = vector.to_crs({'init': 'epsg:4326'})
    print(vector.bounds.maxy)
    # lat slice goes from pos to neg because it's 90 to -90 degrees so max first
    lat_slice = slice(float(numpy.max(vector.bounds.maxy)), float(numpy.min(vector.bounds.miny)))
    # lng slice is from -180 to 180 so min first
    lon_slice = slice(float(numpy.min(vector.bounds.minx)), float(numpy.max(vector.bounds.maxx)))

    local_slice = gdm_dataset.sel(
        latitude=lat_slice,
        longitude=lon_slice,
        time=date_range)

    # countries.gpkg can be downloaded from https://github.com/tsamsonov/r-geo-course/blob/master/data/ne/countries.gpkg

    # this was hardcoded in the sample code i got from AER
    gdm_dataset = xarray.open_dataset(
        'http://h2o-dev.aer-aws-nonprod.net/thredds/dodsC/gwsc/gdm')
    print(gdm_dataset)
    print('*')
    print(gdm_dataset)
    print('*')
    # # this one from nick that he could open in ArcGIS
    # gdm_dataset = xarray.open_dataset("H2O_GlobalDroughtIndex_ECMWF-ERA5_2022-07_2022-09.nc")
    # print(gdm_dataset)
    # print('*')

    print(date_range)


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
