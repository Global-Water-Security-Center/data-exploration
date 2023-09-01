import argparse
import logging
import os
import sys

import ee
import geemap
import geopandas
import requests


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


def check_collection(model, dataset_id, start_year, end_year):
    collection = (ee.ImageCollection(dataset_id)
                  .filter(ee.Filter.eq('model', model))
                  .filter(ee.Filter.calendarRange(
                        start_year, end_year, 'year'))
                  .select('tasmax'))
    size = collection.size().getInfo()
    return size > 0


def download_geotiff(image, description, scale, region):
    print(image.bandNames().getInfo())
    url = image.getDownloadURL({
        'scale': scale,
        'crs': 'EPSG:4326',
        'region': region,
        'fileFormat': 'GeoTIFF',
        'description': description,
    })
    print(f'downloading {url}')
    response = requests.get(url)
    if response.status_code == 200:
        with open(f"{description}.tif", 'wb') as f:
            f.write(response.content)
    else:
        print(f"Failed to download {description} from {url}")


def main():
    parser = argparse.ArgumentParser(description='Fetch CMIP6 monthly normals.')
    parser.add_argument(
        '--authenticate', action='store_true',
        help='Pass this flag if you need to reauthenticate with GEE')
    parser.add_argument(
        'aoi_vector_path', help='Path to vector/shapefile of area of interest')
    parser.add_argument('--where_statement', help=(
        'If provided, allows filtering by a field id and value of the form '
        'field_id=field_value'))
    parser.add_argument('--band_ids', nargs='+', help="band ids to fetch")
    parser.add_argument('--date_range', nargs=2, type=str, help=(
        'Two date ranges in YYYY format to download between.'))
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

    # Filter models dynamically based on data availability
    start_year = int(args.date_range[0])
    end_year = int(args.date_range[1])
    filtered_models = [
        model for model in VALID_MODEL_LIST
        if check_collection(model, DATASET_ID, start_year, end_year)]

    cmip6_dataset = ee.ImageCollection(DATASET_ID).filter(
        ee.Filter.inList('model', filtered_models)).select(args.band_ids)

    for month in range(1, 13):
        monthly_collection = cmip6_dataset.filter(
            ee.Filter.calendarRange(month, month, 'month')).filter(
            ee.Filter.calendarRange(start_year, end_year, 'year'))
        monthly_mean = monthly_collection.reduce(ee.Reducer.mean())
        monthly_mean_clipped = monthly_mean.clip(ee_poly)
        LOGGER.debug(monthly_mean_clipped.getInfo())
        description = f"MonthlyMean_{month}_{start_year}_{end_year}"
        download_geotiff(monthly_mean_clipped.select(
            [f'{band_id}_mean' for band_id in args.band_ids]), description,
            DATASET_SCALE, ee_poly.geometry())

if __name__ == '__main__':
    main()
