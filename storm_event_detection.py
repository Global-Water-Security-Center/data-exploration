"""Script to analyze a time series to determine when a storm event happens.

The storm of April 11 dumped nearly 300mm of rain
in just over 24 hours and became the deadliest to hit
South Africa in recent history eclipsing one-day
rainfall totals from previous disasters. Including the
1987 Durban floods that brought on average
225mm rainfall in 24 hours and killed as many as
500 people and the 2019 floods with 165mm rainfall
in 24 hours left at least 80 dead


"""
import glob

import xarray
import pandas
import numpy
import rioxarray

import matplotlib.pyplot as plt
import numpy as np

#threadd_url = 'http://h2o-sandbox1.aer-aws-nonprod.net/thredds/dodsC/era5/daily-summary.nc'
threadd_url = 'http://h2o-sandbox1.aer-aws-nonprod.net/thredds/dodsC/era5/normal-prcp-temp.nc'

def main():
    """Entry point."""
    nc_path_list = list(glob.glob(r'D:\repositories\wwf-sipa\rain_events/*.nc'))
    precip_array = numpy.zeros(len(nc_path_list), dtype=float)
    date_array = numpy.empty(len(nc_path_list), dtype='datetime64[ns]')
    # print(date_array)
    for index, nc_path in enumerate(nc_path_list):
        print(index, len(nc_path_list))
        era5_daily_summary = rioxarray.open_rasterio(nc_path)
        print(era5_daily_summary[0].attrs)
        return
        current_date = era5_daily_summary.time.values[0]
        date_array[index] = current_date
        precip_array[index] = numpy.max(era5_daily_summary.precip)

    numpy.save('precip.np', precip_array)
    print('done')
    # plt.plot_date(date_array, precip_array)
    # plt.show()

    #era5_daily_summary['time'] = pandas.DatetimeIndex(era5_daily_summary.time.str.decode('utf-8').astype('datetime64'))
    #date_range = pandas.date_range(start=START_DATE, end=END_DATE, freq='MS')
    #era5_daily_summary = era5_daily_summary.sel(time=date_range)


    # vector = geopandas.read_file('europe_basins')
    # vector = vector.to_crs({'init': 'epsg:4326'})
    # print(vector.bounds.maxy)
    # # lat slice goes from pos to neg because it's 90 to -90 degrees so max first
    # lat_slice = slice(float(numpy.max(vector.bounds.maxy)), float(numpy.min(vector.bounds.miny)))
    # # lng slice is from -180 to 180 so min first
    # lon_slice = slice(float(numpy.min(vector.bounds.minx)), float(numpy.max(vector.bounds.maxx)))

    # local_slice = era5_daily_summary.sel(
    #     latitude=lat_slice,
    #     longitude=lon_slice,
    #     time=date_range)



if __name__ == '__main__':
    main()
