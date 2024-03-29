"""See `python scriptname.py --help"""
import argparse
import functools
import logging
import os
import pickle
import sys

import ee
import geemap
import geopandas
import numpy

logging.basicConfig(
    level=logging.WARNING,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


VALID_MODEL_LIST = (
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
)

DATASET_ID = 'NASA/GDDP-CMIP6'
DATASET_CRS = 'EPSG:4326'
DATASET_SCALE = 27830

PICKLE_FILE = 'result.pkl'


def auto_memoize(func):
    cache_file = f"{func.__name__}_cache.pkl"

    if os.path.exists(cache_file):
        with open(cache_file, 'rb') as f:
            cache = pickle.load(f)
    else:
        cache = {}

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        key = (args, frozenset(kwargs.items()))
        if key in cache:
            return cache[key]
        else:
            result = func(*args, **kwargs)
            cache[key] = result
            with open(cache_file, 'wb') as f:
                pickle.dump(cache, f)
            return result

    return wrapper


@auto_memoize
def get_valid_model_list(model_list, start_year, end_year, scenario_id):
    # Initialize the ImageCollection with your filters
    cmip6_dataset = ee.ImageCollection(DATASET_ID).select('pr').filter(
        ee.Filter.And(
            ee.Filter.inList('model', model_list),
            ee.Filter.eq('scenario', scenario_id),
            ee.Filter.calendarRange(start_year, end_year, 'year'))
    )

    # Aggregate model IDs
    unique_models = cmip6_dataset.aggregate_array('model').distinct()

    # Bring the list to Python
    unique_models_list = unique_models.getInfo()

    # Print or otherwise use the list
    print("Unique model IDs:", unique_models_list)
    return tuple(unique_models_list)


def authenticate():
    try:
        ee.Initialize()
        return
    except Exception:
        pass

    try:
        ee.Authenticate()
        ee.Initialize()
        return
    except Exception:
        pass

    try:
        gee_key_path = os.environ['GEE_KEY_PATH']
        credentials = ee.ServiceAccountCredentials(None, gee_key_path)
        ee.Initialize(credentials)
        return
    except Exception:
        pass

    ee.Initialize()


def main():
    parser = argparse.ArgumentParser(description=(
        'Fetch CMIP6 data cut by model and by year.'))
    parser.add_argument(
        '--variable_id', required=True, help='variable to process')
    parser.add_argument(
        '--aggregate_function', required=True, help='either "sum", or "mean"')
    parser.add_argument(
        '--aoi_vector_path', required=True,
        help='Path to vector/shapefile of area of interest')
    parser.add_argument('--where_statement', help=(
        'If provided, allows filtering by a field id and value of the form '
        'field_id=field_value'))
    parser.add_argument('--year_range', nargs=2, type=int, help=(
        'Two year ranges in YYYY format to download between.'))
    parser.add_argument(
        '--scenario_id', required=True,
        help="Scenario ID ssp245, ssp585, historical")
    parser.add_argument(
        '--season_range', nargs='+', required=True,
        help='Pairs of start-end Julian days to do the analysis, successive ones will be appended to the table')
    parser.add_argument(
        '--dataset_scale', type=float, default=DATASET_SCALE, help=(
            f'Override the base scale of {DATASET_SCALE}m to '
            f'whatever you desire.'))
    parser.add_argument(
        '--target_table_path', help="Name of target table", required=True)
    parser.add_argument(
        '--file_prefix', default='', help='Prefix the output file with this.')
    parser.add_argument(
        '--eval_cmd',
        help='an arbitrary command using "var" as the variable to do any final conversion')

    args = parser.parse_args()
    working_dir = args.target_table_path
    target_table_base = os.path.join(
        working_dir,
        f'{args.file_prefix}_'
        f'{args.variable_id}_' +
        f'{args.aggregate_function}_'
        f'{os.path.basename(os.path.splitext(args.aoi_vector_path)[0])}_' +
        f'{args.where_statement}_' +
        f'{args.scenario_id}_' +
        '_'.join([str(x) for x in args.year_range]) + '_' +
        '_'.join([str(x) for x in args.season_range]))
    target_table_path = f'{target_table_base}.csv'
    if os.path.exists(target_table_path):
        print(f'{target_table_path} exists, skipping....')
        return

    # Filter models dynamically based on data availability
    start_year, end_year = args.year_range
    model_list = get_valid_model_list(
        VALID_MODEL_LIST, start_year, end_year, args.scenario_id)

    pickle_file = '%s.pkl ' % target_table_base
    if os.path.exists(pickle_file):
        with open(pickle_file, 'rb') as f:
            result_by_year_and_season = pickle.load(f)
    else:
        authenticate()

        aoi_vector = geopandas.read_file(args.aoi_vector_path)
        if args.where_statement:
            field_id, field_val = args.where_statement.split('=')
            # make sure we cast the input type to the type in the field
            print(numpy.unique(aoi_vector[field_id]))
            field_type = type(aoi_vector[field_id].iloc[0])
            aoi_vector = aoi_vector[aoi_vector[field_id] == field_type(field_val)]
            print(aoi_vector)

        local_shapefile_path = '_local_cmip6_aoi_ok_to_delete.json'
        aoi_vector = aoi_vector.to_crs('EPSG:4326')
        aoi_vector.to_file(local_shapefile_path)
        aoi_vector = None
        ee_poly = geemap.geojson_to_ee(local_shapefile_path)

        result_by_year_and_season = {}

        for start_end_str in args.season_range:
            start_day, end_day = [int(x) for x in start_end_str.split('-')]
            for target_year in range(start_year, end_year+1):
                # Create a function to get the ImageCollection filtered by days and year
                def filter_by_days_and_year(start_day, end_day, target_year):
                    return ee.ImageCollection(DATASET_ID).select(args.variable_id).filter(
                        ee.Filter.And(
                            ee.Filter.inList('model', model_list),
                            ee.Filter.eq('scenario', args.scenario_id),
                            ee.Filter.calendarRange(target_year, target_year, 'year'),
                            ee.Filter.calendarRange(start_day, end_day, 'day_of_year')))

                # Initialize the ImageCollection based on whether the day range crosses a new year
                if start_day <= end_day:
                    cmip6_dataset = filter_by_days_and_year(start_day, end_day, target_year)
                else:
                    first_part = filter_by_days_and_year(start_day, 365, target_year)
                    second_part = filter_by_days_and_year(1, end_day, target_year + 1)
                    cmip6_dataset = first_part.merge(second_part)

                def aggregate_op(model_name):
                    model_data = cmip6_dataset.filter(
                        ee.Filter.eq('model', model_name))
                    sum_result = False
                    if args.aggregate_function.startswith('gt'):
                        sum_result = True
                        threshold_val = float(args.aggregate_function.split('_')[1])
                        print(threshold_val)

                        def mark_above_threshold(image):
                            return image.gt(threshold_val)
                        # Apply the count_days_above_threshold function to each image in the collection
                        model_data = model_data.map(mark_above_threshold)

                        def has_one_or_more(image):
                            result = ee.Image.constant(image.reduceRegion(
                                reducer=ee.Reducer.anyNonZero(),
                                geometry=ee_poly,
                                scale=args.dataset_scale
                            ).values().get(0)).toInt()
                            return result
                        # Map the function over the ImageCollection
                        model_data = model_data.map(has_one_or_more)

                    if args.aggregate_function == 'sum' or sum_result:
                        print('doing sum')
                        reducer_op = ee.Reducer.sum()
                    elif args.aggregate_function == 'mean':
                        print('doing mean')
                        reducer_op = ee.Reducer.mean()
                    return model_data.reduce(
                        reducer_op).reduceRegion(
                        reducer=ee.Reducer.mean(), geometry=ee_poly,
                        scale=args.dataset_scale).values().get(0)

                aggregate_by_model_dict = ee.Dictionary.fromLists(
                    model_list,
                    ee.List(model_list).map(
                        lambda model_name: aggregate_op(
                            ee.String(model_name))))
                print(f'processing year {target_year} of {args.variable_id} of {args.where_statement}')
                result_by_year_and_season[(target_year, start_end_str)] = aggregate_by_model_dict.getInfo()

        if args.eval_cmd is not None:
            result_by_year_and_season = {
                year_season: {
                    model: None if var is None else eval(args.eval_cmd)
                    for model, var in inner_dict.items()
                } for year_season, inner_dict in result_by_year_and_season.items()
            }

        os.makedirs(working_dir, exist_ok=True)
        pickle_file = '%s.pkl ' % target_table_base
        with open(pickle_file, 'wb') as f:
            pickle.dump(result_by_year_and_season, f)

    with open(target_table_path, 'w') as table_file:
        table_file.write(',' + ','.join(sorted(model_list)) + '\n')
        for year_season in sorted(result_by_year_and_season, key=lambda x: f'{x[0]}{int(x[1].split("-")[0]):03d}'):
            year_data = result_by_year_and_season[year_season]
            table_file.write(
                f'{year_season[0]} {year_season[1]},' + ','.join(
                    [str(year_data[model_id])
                     for model_id in sorted(model_list)]) + '\n')


if __name__ == '__main__':
    main()
