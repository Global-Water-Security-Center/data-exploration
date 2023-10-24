import argparse
import ee
import logging
import os
import geemap
import geopandas
import sys


logging.basicConfig(
    level=logging.WARNING,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


VALID_MODEL_LIST = [
    'ACCESS-ESM1-5',
    'BCC-CSM2-MR',
    'CanESM5',
    'CESM2',
    'CMCC-ESM2',
    'CNRM-ESM2-1',
    'EC-Earth3-Veg-LR',
    'FGOALS-g3',
    'GFDL-ESM4',
    'GISS-E2-1-G',
    'HadGEM3-GC31-MM',
    'IITM-ESM',
    'INM-CM5-0',
    'IPSL-CM6A-LR',
    'KACE-1-0-G',
    'KIOST-ESM',
    'MIROC-ES2L',
    'MPI-ESM1-2-HR',
    'MRI-ESM2-0',
    'NESM3',
    'NorESM2-MM',
    'TaiESM1',
    'UKESM1-0-LL',
]

DATASET_ID = 'NASA/GDDP-CMIP6'
DATASET_CRS = 'EPSG:4326'
DATASET_SCALE = 27830

# We would like to evaluate the risk of crop failure from high temperatures
# during the growing season, specifically by quantifying the historic and future
# frequency of temperature exceeding 35 degrees during the wheat growing season
# (November-March) To do this, we would count the number of years the maximum
# temperature during the November-March period exceeded 35 degrees at least once
# during the historic period (2000-2020). For the future, for each model and
# iteration, we would do the same calculation for (2040-2060) and (2090-2110 OR
#     2080-2100 if the models only go out to 2100) and then compare the average
# and range of exceedance frequency in the future to that in the past
# For the short time period, that’s been defined for us: one calendar day (24 hours).
# What we need the script to allow us define:
# an area (state, country, a place with a boundary we can find or create a shapefile for)
# a temperature threshold (e.g. 35•C)
# a medium time frame like a month or a season (e.g. wheat growing season Nov-Mar)
# a long time frame like a number of years or historical/future period (e.g. 2001-2021, 2040-2060, etc.)
# What we need the script to calculate:
# a list of years in the long time period where the temperature threshold was reached or breached in the medium period
# the number of days within the medium period that reach or breach the temperature threshold, by year
# whether those days are consecutive and could constitute a heat wave, again by year
# if there are consecutive heat days, the number of heat blocks in the medium period, by year


def main():
    parser = argparse.ArgumentParser(
        description='Experiments on NEX GDDP CMIP6 data.')
    parser.add_argument(
        '--authenticate', action='store_true',
        help='Pass this flag if you need to reauthenticate with GEE')
    parser.add_argument(
        'aoi_vector_path', help='Path to vector/shapefile of area of interest')
    parser.add_argument('--where_statement', help=(
        'If provided, allows filtering by a field id and value of the form '
        'field_id=field_value'))
    parser.add_argument(
        '--temperature_threshold', type=float, help='Temp threshold in C.')
    parser.add_argument('--season_range', type=str, help=(
        'Two numbers separated by a hyphen representing the start and end day '
        'of a season in Julian calendar days. Negative numbers refer to the '
        'previous year and >365 indicates the next year. '
        'i.e. 201-320 or -10-65'))
    parser.add_argument('--year_range', help=(
        'A start and end year date as a hypenated string to run the analysis '
        'on.'))
    args = parser.parse_args()

    gee_key_path = os.environ['GEE_KEY_PATH']
    credentials = ee.ServiceAccountCredentials(None, gee_key_path)
    ee.Initialize(credentials)

    aoi_vector = geopandas.read_file(args.aoi_vector_path)
    if args.where_statement:
        field_id, field_val = args.where_statement.split('=')
        print(aoi_vector.size)
        aoi_vector = aoi_vector[aoi_vector[field_id] == field_val]
        print(aoi_vector)
        print(aoi_vector.size)

    local_shapefile_path = '_local_cmip6_aoi_ok_to_delete.json'
    aoi_vector = aoi_vector.to_crs('EPSG:4326')
    aoi_vector.to_file(local_shapefile_path)
    aoi_vector = None
    ee_poly = geemap.geojson_to_ee(local_shapefile_path)
    os.remove(local_shapefile_path)

    # def reduce_region_function(image_set):
    #     available_bands = image_set.bandNames()
    #     if available_bands.contains('tasmax'):
    #         image = image_set.select('tasmax')
    #         reduced_value = image.reduceRegion(
    #             reducer=ee.Reducer.max(),
    #             geometry=ee_poly,
    #             crs=DATASET_CRS,
    #             scale=DATASET_SCALE)
    #     # Return the reduced value as a feature, setting some properties that help in identifying the image
    #         return ee.Feature(None, reduced_value).set(
    #             {'ImageID': image.id(), 'Date': image.date().format()})
    #     else:
    #         return ee.Feature(None, {'tasmax': None})

    # Only pass on images with 'tasmax' band to the map function
    def robust_reduce_region_function(image):
        available_bands = image.bandNames()
        is_tasmax_present = available_bands.contains('tasmax')

        # Conditional computation using ee.Algorithms.If
        reduced_value = ee.Algorithms.If(
            is_tasmax_present,
            image.select('tasmax').reduceRegion(
                reducer=ee.Reducer.max(),
                geometry=ee_poly,
                crs=DATASET_CRS,
                scale=DATASET_SCALE
            ),
            ee.Dictionary({'tasmax': -9999}),
        )
        return ee.Feature(None, reduced_value)

    cmip6_dataset = ee.ImageCollection(DATASET_ID).filter(
        ee.Filter.inList('model', VALID_MODEL_LIST))
    LOGGER.debug(cmip6_dataset.first().getInfo())

    start_year, end_year = [int(v) for v in args.year_range.split('-')]
    season_day_start, season_day_end = [
        int(v) for v in args.season_range.split('-')]
    collection_by_year = ee.Dictionary()
    for year_id in range(start_year, end_year+1):
        #TODO: a list of years in the long time period where the temperature threshold was reached or breached in the medium period
        # Convert Julian days to actual dates
        start_date = ee.Date.fromYMD(year_id, 1, 1).advance(season_day_start, 'day')
        end_date = ee.Date.fromYMD(year_id, 1, 1).advance(season_day_end, 'day')
        local_cmip6_dataset = cmip6_dataset.filterDate(start_date, end_date)
        # Map the function over the ImageCollection
        reduced_collection = local_cmip6_dataset.map(robust_reduce_region_function)
        collection_by_year.add(year_id, reduced_collection)
    #reduced_collection_list = reduced_collection.getInfo()['features']
    print(collection_by_year.getInfo())


if __name__ == '__main__':
    main()
