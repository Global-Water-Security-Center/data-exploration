"""See `python scriptname.py --help"""
from concurrent.futures import ThreadPoolExecutor
from itertools import product
import argparse
import collections
import io
import logging
import os
import shutil
import sys
import zipfile

from ecoshard import geoprocessing
from osgeo import gdal
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
    # 'HadGEM3-GC31-MM',
    # 'IITM-ESM',
    # 'INM-CM5-0',
    # 'IPSL-CM6A-LR',
    # 'KACE-1-0-G',
    # 'KIOST-ESM',
    # 'MIROC-ES2L',
    # 'MPI-ESM1-2-HR',
    # 'MRI-ESM2-0',
    # 'NESM3',
    # 'NorESM2-MM',
    # 'TaiESM1',
    # 'UKESM1-0-LL',
]

HISTORICAL_YEAR_CUTOFF = 2015  # anything less than 2015 is historical
SSP_LIST = [285, 585]
DATASET_ID = 'NASA/GDDP-CMIP6'
DATASET_CRS = 'EPSG:4326'
WORKSPACE_DIR = 'cmip6_raster_fetch_workspace'
os.makedirs(WORKSPACE_DIR, exist_ok=True)
VALID_VARIABLES = ['pr', 'tasmax', 'tasmin', 'tas']
PRECIP_VARIABLES = {'pr'}
TEMPERATURE_VARIABLES = {'tasmax', 'tasmin', 'tas'}

def check_dataset_collection(model, dataset_id, band_id, start_year, end_year):
    def band_checker(image):
        available_bands = image.bandNames()
        return ee.Image(ee.Algorithms.If(
            available_bands.contains(band_id), image,
            ee.Image.constant(0).rename('dummy_band')))

    try:
        collection = (
            ee.ImageCollection(dataset_id)
            .filter(ee.Filter.eq('model', model))
            .filter(ee.Filter.calendarRange(start_year, end_year, 'year'))
            .map(band_checker))

        # Filter out images with the 'dummy_band'
        collection = collection.filterMetadata(
            'system:band_names', 'not_equals', ['dummy_band'])

        size = collection.size().getInfo()
        print(f'{model} has {size} elements with the band(s) {band_id}')
        return size > 0

    except Exception as e:
        print(f"An error occurred: {e}")
        return False


def download_geotiff(
        image, description, scale, ee_poly, clip_poly_path,
        target_raster_path):
    url = image.resample('bilinear').getDownloadURL({
        'scale': scale,
        'crs': 'EPSG:4326',
        'region': ee_poly.geometry(),
        'fileFormat': 'GeoTIFF',
        'description': description,
    })

    response = requests.get(url)
    if response.status_code == 200:
        content_type = response.headers.get('content-type')
        if 'application/zip' in content_type or 'application/octet-stream' in content_type:
            zip_buffer = io.BytesIO(response.content)
            target_preclip = os.path.join(WORKSPACE_DIR, f"pre_clip_{description}.tif")

            with zipfile.ZipFile(zip_buffer) as zip_ref:
                zip_ref.extractall(f"{description}")
                # Assuming there is only one tif file in the zip
                for filename in zip_ref.namelist():
                    if filename.endswith('.tif'):
                        # Optionally rename the file
                        if os.path.exists(target_preclip):
                            os.remove(target_preclip)
                        os.rename(f"{description}/{filename}", target_preclip)
                        shutil.rmtree(description)
                        print(f"Successfully downloaded and unzipped {filename}")
            r = gdal.OpenEx(target_preclip, gdal.OF_RASTER)
            b = r.GetRasterBand(1)
            b.SetNoDataValue(-9999)
            b = None
            r = None
            raster_info = geoprocessing.get_raster_info(target_preclip)

            geoprocessing.warp_raster(
                target_preclip, raster_info['pixel_size'],
                target_raster_path,
                'near',
                vector_mask_options={
                    'mask_vector_path': clip_poly_path,
                    'all_touched': True,
                    })
            os.remove(target_preclip)
            os.remove(f'{target_preclip}.aux.xml')
            LOGGER.info(f'saved {target_raster_path}')
        else:
            print(f"Unexpected content type: {content_type}")
            print(response.content.decode('utf-8'))
    else:
        print(f"Failed to download {description} from {url}")


def filter_by_julian_day(collection, start_year, end_year, julian_day, n_day_window):
    years = ee.List.sequence(start_year, end_year)
    initial = ee.ImageCollection([])

    def year_filter(year, collection):
        year = ee.Number(year)
        start_date = ee.Date.fromYMD(year, 1, 1).advance(julian_day - 1, 'day')
        start_window = start_date.advance(-n_day_window//2, 'day')
        end_window = start_date.advance(n_day_window//2, 'day')

        filtered = collection.filterDate(start_window, end_window)
        return ee.ImageCollection(collection).merge(filtered)

    return ee.ImageCollection(years.iterate(year_filter, initial))


def authenticate():
    try:
        gee_key_path = os.environ['GEE_KEY_PATH']
        credentials = ee.ServiceAccountCredentials(None, gee_key_path)
        ee.Initialize(credentials)
        return
    except Exception:
        pass

    try:
        ee.Initialize()
        return
    except Exception:
        pass

    ee.Authetnicate()
    ee.Initialize()


def main():
    parser = argparse.ArgumentParser(description=(
        'Fetch CMIP6 temperature and precipitation monthly normals given a '
        'year date range.'))
    parser.add_argument(
        'aoi_vector_path', help='Path to vector/shapefile of area of interest')
    parser.add_argument(
        '--field_id_for_aggregate', help='Field ID in aoi for aggregating.')
    parser.add_argument('--where_statement', help=(
        'If provided, allows filtering by a field id and value of the form '
        'field_id=field_value'))
    parser.add_argument('--date_range', nargs='+', type=str, help=(
        'Date ranges in YYYY-YYYY format to download between.'))
    parser.add_argument(
        '--dataset_scale', type=float, help='Dataset scale', default=27830)
    parser.add_argument(
        '--table_path', help='Desired output table path.')
    parser.add_argument(
        '--n_day_window', default=10, type=int, help='Number of days in which to average around')
    args = parser.parse_args()

    authenticate()

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

    raster_path_map = {}
    executor = ThreadPoolExecutor(2 * os.cpu_count())
    for date_range, variable_id in product(args.date_range, VALID_VARIABLES):
        # Filter models dynamically based on data availability
        start_year, end_year = [int(y) for y in date_range.split('-')]

        if end_year < HISTORICAL_YEAR_CUTOFF:
            scenario_list = ['historical']
        elif start_year >= HISTORICAL_YEAR_CUTOFF:
            scenario_list = ['ssp245', 'ssp585']
        else:
            raise ValueError(
                'start year and end year must be < 2015 for historical or '
                f'>=2015 for scenarios, got this instead: {date_range}')

        def filter_model(model):
            return model if check_dataset_collection(
                model, DATASET_ID, variable_id, start_year, end_year) else None

        valid_models = list(
            filter(None, executor.map(filter_model, VALID_MODEL_LIST)))
        model_list_to_run = [('all_models', valid_models)]

        for (model_id, model_list), scenario_id in product(model_list_to_run, scenario_list):
            cmip6_dataset = ee.ImageCollection(DATASET_ID).filter(
                ee.Filter.And(
                    ee.Filter.inList('model', model_list),
                    ee.Filter.eq('scenario', scenario_id))).select(
                variable_id)

            for julian_day in [0]+list(range(args.n_day_window//2, 366, args.n_day_window)):
                description = (
                    f'{variable_id}_{scenario_id}_{model_id}_'
                    f'{start_year}_{end_year}')
                if julian_day > 0:
                    # Convert Julian day to a Date object
                    filtered_collection = filter_by_julian_day(
                        cmip6_dataset, start_year, end_year, julian_day, args.n_day_window)
                    description += f'_{julian_day:02d}'
                else:
                    description += '_year'
                    filtered_collection = cmip6_dataset.filter(
                        ee.Filter.calendarRange(start_year, end_year, 'year'))

                target_raster_path = os.path.join(WORKSPACE_DIR, f'{description}.tif')
                if os.path.exists(target_raster_path):
                    LOGGER.info(f'{target_raster_path} already exists, so skipping')
                    raster_path_map[
                        (variable_id, scenario_id, model_id,
                         f'{start_year}-{end_year}', julian_day)] = target_raster_path
                    continue

                if variable_id in TEMPERATURE_VARIABLES:
                    # convert to C
                    aggregate = filtered_collection.reduce(
                        ee.Reducer.mean()).subtract(273.15)
                elif variable_id in PRECIP_VARIABLES:
                    # multiply by 86400 to convert to mm
                    aggregate = filtered_collection.reduce(
                        ee.Reducer.sum()).multiply(
                            86400/((end_year-start_year+1)*len(model_list)))
                else:
                    raise ValueError(
                        f'unknown aggregate type for variable {variable_id}')
                aggregate_clipped = aggregate.clip(ee_poly)
                try:
                    executor.submit(
                        download_geotiff, aggregate_clipped, description,
                        args.dataset_scale, ee_poly, local_shapefile_path,
                        target_raster_path)
                    raster_path_map[
                        (variable_id, scenario_id, model_id,
                         f'{start_year}-{end_year}', julian_day)] = target_raster_path
                except Exception as e:
                    LOGGER.error(f'***** ERROR on downloading geotiff, skipping: {e}')

    executor.shutdown()
    aoi_vector = gdal.OpenEx(args.aoi_vector_path)
    aoi_layer = aoi_vector.GetLayer()
    result_dict = collections.defaultdict(
        lambda:collections.defaultdict(lambda: None))
    scenario_key_set = set()
    for (variable_id, scenario_id, model_id, start_end_year, julian_day), raster_path in \
            raster_path_map.items():
        if julian_day == 0:
            julian_day = 'year'
        zonal_working_dir = os.path.join(
            WORKSPACE_DIR, os.path.basename(os.path.splitext(raster_path)[0]))
        os.makedirs(zonal_working_dir, exist_ok=True)
        zonal_stats_map = geoprocessing.zonal_statistics(
            (raster_path, 1), args.aoi_vector_path,
            polygons_might_overlap=False,
            working_dir=zonal_working_dir)
        shutil.rmtree(zonal_working_dir)
        for fid, stats_map in zonal_stats_map.items():
            feature = aoi_layer.GetFeature(fid)
            aggregate_id = feature.GetField(args.field_id_for_aggregate)
            val = (stats_map['max']+stats_map['min'])/2
            scenario_key = f'{start_end_year} - {scenario_id}'
            scenario_key_set.add(scenario_key)
            result_dict[(aggregate_id, variable_id, model_id, julian_day)][scenario_key] = val

    scenario_key_list = list(sorted(scenario_key_set))
    os.remove(local_shapefile_path)
    table_file = open(args.table_path, 'w')
    table_file.write(f'{args.field_id_for_aggregate},variable,model ID,julian day,')
    table_file.write(','.join(scenario_key_list) + '\n')
    for (aggregate_id, variable_id, model_id, julian_day), val_dict in result_dict.items():
        table_file.write(
            f'{aggregate_id},{variable_id},{model_id},{julian_day},')
        table_file.write(','.join(str(val_dict[key]) for key in scenario_key_list) + '\n')
    table_file.close()


if __name__ == '__main__':
    main()
