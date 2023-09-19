"""See `python scriptname.py --help"""
import argparse
import io
import logging
import os
import sys
import zipfile
import threading
from concurrent.futures import ThreadPoolExecutor

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
SSP_LIST = [285, 585]
DATASET_ID = 'NASA/GDDP-CMIP6'
DATASET_CRS = 'EPSG:4326'
WORKSPACE_DIR = 'cmip6_raster_fetch_workspace'
os.makedirs(WORKSPACE_DIR, exist_ok=True)

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


def main():
    parser = argparse.ArgumentParser(description=(
        'Fetch CMIP6 temperature and precipitation monthly normals given a '
        'year date range.'))
    parser.add_argument(
        '--authenticate', action='store_true',
        help='Pass this flag if you need to reauthenticate with GEE')
    parser.add_argument(
        'aoi_vector_path', help='Path to vector/shapefile of area of interest')
    parser.add_argument(
        '--field_id_for_aggregate', help='Field ID in aoi for aggregating.')
    parser.add_argument('--where_statement', help=(
        'If provided, allows filtering by a field id and value of the form '
        'field_id=field_value'))
    parser.add_argument('--aggregate_type', help='Either "sum" or "mean"')
    parser.add_argument('--band_id', help="band id to fetch")
    parser.add_argument(
        '--scenario_id', help="Scenario ID ssp245, ssp585, historical")
    parser.add_argument('--date_range', nargs=2, type=str, help=(
        'Two date ranges in YYYY format to download between.'))
    parser.add_argument(
        '--dataset_scale', type=float, help='Dataset scale', default=27830)
    parser.add_argument(
        '--table_path', help='Desired output table path.')
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

    # Filter models dynamically based on data availability
    start_year = int(args.date_range[0])
    end_year = int(args.date_range[1])

    def filter_model(model):
        return model if check_dataset_collection(
            model, DATASET_ID, args.band_id, start_year, end_year) else None

    with ThreadPoolExecutor(len(VALID_MODEL_LIST)) as executor:
        filtered_models = list(
            filter(None, executor.map(filter_model, VALID_MODEL_LIST)))

    make_header = not os.path.exists(args.table_path)
    make_header = True
    table_file = open(args.table_path, 'a')
    if make_header:
        table_file.write('f{args.field_id_for_aggregate},band_id,scenario_id,model_id,aggregate_type,start-end-year,month,avg val in feature\n')

    models_to_analyze = [
        (model_id, [model_id]) for model_id in filtered_models]
    models_to_analyze += [('all_models', filtered_models)]

    for model_id, model_list in models_to_analyze:
        model_list = [model_id]
        cmip6_dataset = ee.ImageCollection(DATASET_ID).filter(
            ee.Filter.And(
                ee.Filter.inList('model', model_list),
                ee.Filter.eq('scenario', args.scenario_id))).select(
            args.band_id)

        thread_list = []
        raster_path_map = {}
        for month in range(1, 13):
            description = (
                f'{args.band_id}_{args.scenario_id}_{model_id}_monthly_{args.aggregate_type}'
                f'{start_year}_{end_year}_{month}')
            target_raster_path = os.path.join(WORKSPACE_DIR, f'{description}.tif')
            raster_path_map[
                (args.band_id, args.scenario_id, args.aggregate_type,
                 f'{start_year}-{end_year}', month)] = target_raster_path
            if os.path.exists(target_raster_path):
                LOGGER.info(f'{target_raster_path} already exists, so skipping')
                continue

            monthly_collection = cmip6_dataset.filter(
                ee.Filter.calendarRange(month, month, 'month')).filter(
                ee.Filter.calendarRange(start_year, end_year, 'year'))
            if args.aggregate_type == 'min':
                # convert to C
                monthly_aggregate = monthly_collection.reduce(
                    ee.Reducer.min()).subtract(273.15)
            elif args.aggregate_type.startswith('percentile'):
                # convert to C
                percentile = float(args.aggregate_type.split('_')[1])
                monthly_aggregate = monthly_collection.reduce(
                    ee.Reducer.percentile([percentile])).subtract(273.15)
            elif args.aggregate_type == 'sum':
                # multiply by 86400 to convert to mm
                monthly_aggregate = monthly_collection.reduce(
                    ee.Reducer.sum()).multiply(
                        86400/((end_year-start_year+1)*len(model_list)))
            monthly_aggregate_clipped = monthly_aggregate.clip(ee_poly)
            LOGGER.debug(monthly_aggregate_clipped.getInfo())
            worker = threading.Thread(
                target=download_geotiff,
                args=(
                    monthly_aggregate_clipped,
                    description, args.dataset_scale, ee_poly,
                    local_shapefile_path, target_raster_path))
            worker.start()
            thread_list.append(worker)

        for worker in thread_list:
            worker.join()

        aoi_vector = gdal.OpenEx(args.aoi_vector_path)
        aoi_layer = aoi_vector.GetLayer()
        for (band_id, scenario_id, aggregate_type, start_end_year, month), raster_path in raster_path_map.items():
            zonal_stats_map = geoprocessing.zonal_statistics(
                (raster_path, 1), args.aoi_vector_path,
                polygons_might_overlap=False)
            for fid, stats_map in zonal_stats_map.items():
                feature = aoi_layer.GetFeature(fid)
                aggregate_id = feature.GetField(args.field_id_for_aggregate)
                val = (stats_map['max']+stats_map['min'])/2
                table_file.write(f'{aggregate_id},{band_id},{scenario_id},{model_id},{aggregate_type},{start_end_year},{month},{val}\n')

    os.remove(local_shapefile_path)
    table_file.close()


if __name__ == '__main__':
    main()
