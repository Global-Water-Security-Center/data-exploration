from concurrent.futures import ThreadPoolExecutor
import argparse
import collections
import logging
import os
import requests
import shutil
import sys
from threading import Lock

from tenacity import retry, stop_after_attempt, wait_random_exponential
from rasterio.transform import Affine
import numpy
import rasterio
import xarray


BASE_URL = 'https://esgf-node.llnl.gov/esg-search/search'
BASE_SEARCH_URL = 'https://esgf-node.llnl.gov/search_files'
VARIANT_SUFFIX = 'i1p1f1'
LOCAL_CACHE_DIR = '_cmip6_local_cache'
os.makedirs(LOCAL_CACHE_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'))
LOGGER = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
LOGGER.setLevel(logging.DEBUG)
logging.getLogger('fetch_data').setLevel(logging.INFO)

ERROR_LOCK = Lock()


@retry(wait=wait_random_exponential(multiplier=1, min=1, max=10), retry_error_callback=handle_retry_error)
def _do_search(offest, search_params):
    return (search_params['offset'],
            requests.get(BASE_URL, params=search_params).json())


def handle_retry_error(retry_state):
    # retry_state.outcome is a built-in tenacity method that contains the result or exception information from the last call
    last_exception = retry_state.outcome.exception()
    LOGGER.error(last_exception)
    with ERROR_LOCK:
        with open('cmip6_error_log.txt', 'a') as error_log:
            error_log.write(
                f"{retry_state.args[0]}," +
                str(last_exception).replace('\n', '<enter>')+"\n")
            error_log.write(
                f"\t{retry_state.args[0]}," +
                str(last_exception.statistics).replace('\n', '<enter>')+"\n")


@retry(wait=wait_random_exponential(multiplier=1, min=1, max=10), retry_error_callback=handle_retry_error)
def _download_file(target_dir, url):

    stream_path = os.path.join(
        LOCAL_CACHE_DIR, 'streaming', os.path.basename(url))
    target_path = os.path.join(
        LOCAL_CACHE_DIR, target_dir, os.path.basename(stream_path))
    if os.path.exists(target_path):
        print(f'{target_path} exists, skipping')
        return target_path
    os.makedirs(os.path.dirname(stream_path), exist_ok=True)
    LOGGER.info(f'downloading {url} to {target_path}')

    # Get the total file size
    file_stream_response = requests.get(url, stream=True)
    file_size = int(file_stream_response.headers.get(
        'Content-Length', 0))

    # Download the file with progress
    progress = 0

    with open(stream_path, 'wb') as file:
        for chunk in file_stream_response.iter_content(
                chunk_size=2**20):
            if chunk:  # filter out keep-alive new chunks
                file.write(chunk)
            # Update the progress
            progress += len(chunk)
            print(
                f'Downloaded {progress:{len(str(file_size))}d} of {file_size} bytes '
                f'({100. * progress / file_size:5.1f}%) of '
                f'{stream_path} {target_path}')
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    shutil.move(stream_path, target_path)
    return target_path


def print_dict(d, indent=0):
    for key, value in d.items():
        print('  ' * indent + str(key))
        if isinstance(value, dict):
            print_dict(value, indent+1)


def process_era5_netcat_to_geotiff(netcat_path, date_str, target_path_pattern):
    """Convert era5 netcat files to geotiff

    Args:
        netcat_path (str): path to netcat file
        date_str (str): formatted version of the date to use in the target
            file
        target_path_pattern (str): pattern that will allow the replacement
            of `variable` and `date` strings with the appropriate variable
            date strings in the netcat variables.

    Returns:
        list of (file, variable_id) tuples created by this process
    """
    LOGGER.info(f'processing {netcat_path}')
    dataset = xarray.open_dataset(netcat_path)

    res_list = []
    coord_list = []
    for coord_id, field_id in zip(['x', 'y'], ['longitude', 'latitude']):
        coord_array = dataset.coords[field_id]
        res_list.append(float(
            (coord_array[-1] - coord_array[0]) / len(coord_array)))
        coord_list.append(coord_array)

    transform = Affine.translation(
        *[a[0] for a in coord_list]) * Affine.scale(*res_list)

    target_path_variable_id_list = []
    for variable_id, data_array in dataset.items():
        target_path = target_path_pattern.format(**{
            'date': date_str,
            'variable': variable_id
            })
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        with rasterio.open(
            target_path,
            mode="w",
            driver="GTiff",
            height=len(coord_list[1]),
            width=len(coord_list[0]),
            count=1,
            dtype=numpy.float32,
            nodata=None,
            crs="+proj=latlong",
            transform=transform,
            **{
                'tiled': 'YES',
                'COMPRESS': 'LZW',
                'PREDICTOR': 2}) as new_dataset:
            new_dataset.write(data_array)
        target_path_variable_id_list.append((target_path, variable_id))
    return target_path_variable_id_list


def main():
    parser = argparse.ArgumentParser(description=(
        'Fetch CMIP6 variables into wasabi hot storage.'))
    parser.add_argument(
        '--variables', nargs='+',
        default=['pr', 'tas'],
        help='Could be "pr", "tas" etc.')
    parser.add_argument(
        '--experiments', nargs='+',
        default=['historical', 'ssp245', 'ssp370'],
        help="One of ")

    # CMIP6 is cool bec ause  it has the variants
    # r variable is start each model with a different
    # surface temperature, so having the 10 ensamble
    # members provides some sense of reality of that
    # particular model run

    # a year is defined differently between models
    # 365 and some include leap years

    # at 2050 more variation in the ensamble

    # pull 10 ensambles for each of the models we want
    # take ensamble mean for each model

    # total annual precip
    # within a year a count of 20 days with no rain
    #   koppenheimer climate zones

    # grids that the different models run on are not the same
    #

    # only use models with 10 variants
    # varients we want 'r*i1p1f1'

    # do you want a list of all the possibilities given your restrictions?

    parser.add_argument(
        '--local_workspace', type=str, default='cmip6_process_workspace',
        help='Directory to downloand and work in.')
    args = parser.parse_args()

    # Define the search parameters as a dictionary
    search_params = {
        'experiment_id': [args.experiments],
        'frequency': ['day'],
        'variable': [args.variables],
        'limit': 1000,
        'product': 'model-output',
        'format': 'application/solr+json'
    }

    # Make the initial request to get the number of results and pages
    response = requests.get(BASE_URL, params=search_params)
    response_data = response.json()
    num_results = response_data['response']['numFound']
    # #print(list(response_data['response'].keys()))
    # for docs in response_data['response']['docs']:
    #     print_dict(docs)
    #     for key, value in docs.items():
    #         print(f'{key}: {value}')

    #     break
    # return

    # Loop through all pages and append the results to a list
    #result_by_variable = defaultdict(dict)
    result_set = set()

    search_param_list = [
        {**search_params, 'offset': offset}
        for offset in range(0, num_results, 1000)]

    print(search_param_list)
    with ThreadPoolExecutor(10) as executor:
        print('executing')
        response_data_list = list(executor.map(
            _do_search, search_param_list))
        print('done')

        download_data_list = []
        for offset, response_data in response_data_list:
            print(f'processing {offset} of {num_results}')
            for response in response_data['response']['docs']:
                variant_label = response['variant_label'][0]
                if not variant_label.endswith(VARIANT_SUFFIX):
                    continue
                experiment_id = response['experiment_id'][0]
                variable_id = response['variable_id'][0]
                source_id = response['source_id'][0]
                print(f'{experiment_id}, {variable_id}, {source_id}')
                file_search_url = (
                    f"{BASE_SEARCH_URL}/{response['id']}/{response['index_node']}")
                limit = requests.get(file_search_url).json()["response"]["numFound"]
                file_search_url += f'?limit={limit}'
                print(file_search_url)
                data = requests.get(file_search_url).json()['response']['docs']
                download_param_list = []
                for doc_info in data:
                    url = [url.split('|')[0]
                           for url in doc_info['url']
                           if url.endswith('HTTPServer')][0]
                    arg_tuple = (
                        (variable_id, experiment_id, source_id, variant_label,
                         url), (os.path.join(variable_id, experiment_id, source_id, variant_label), url))
                    download_param_list.append(arg_tuple)
                download_data_list += list(executor.map(
                    lambda param_set:
                        (param_set[0], _download_file(*param_set[1])),
                        download_param_list))

    # Print the results
    with open('available_models.csv', 'w') as model_table:
        model_table.write('variable,experiment,model,variant,url,path\n')
        n_downloads = len(download_data_list)
        for index, (model_key, path) in enumerate(download_data_list):
            LOGGER.info(f'downloading {index/(n_downloads-1)*100:5.2f}% complete')
            model_table.write(f'{",".join(model_key)},{path}\n')


if __name__ == '__main__':
    main()
