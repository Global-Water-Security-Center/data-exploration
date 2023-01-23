import cdsapi

c = cdsapi.Client()


for year in range(1950, 2022):
    print(f'fetching {year} data')
    c = cdsapi.Client()

    c.retrieve(
        'reanalysis-era5-land-monthly-means',
        {
            'product_type': 'monthly_averaged_reanalysis',
            'variable': 'total_precipitation',
            'year': str(year),
            'month': [
                '01', '02', '03',
                '04', '05', '06',
                '07', '08', '09',
                '10', '11', '12',
            ],
            'time': '00:00',
            'format': 'grib',
        },
        f'monthly_averaged_reanalysis_precip_{year}.grib')
