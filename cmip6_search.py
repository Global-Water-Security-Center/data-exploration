from concurrent.futures import ThreadPoolExecutor
import json
import argparse
import collections
import requests

BASE_URL = 'https://esgf-node.llnl.gov/esg-search/search'
BASE_SEARCH_URL = 'https://esgf-node.llnl.gov/search_files'
VARIANT_SUFFIX = 'i1p1f1'


def print_dict(d, indent=0):
    for key, value in d.items():
        print('  ' * indent + str(key))
        if isinstance(value, dict):
            print_dict(value, indent+1)


def main():
    parser = argparse.ArgumentParser(description=(
        'Synchronize the files in AER era5 to GWSC wasabi hot storage.'))
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
    with ThreadPoolExecutor() as executor:
        print('exeucting')
        response_data_list = list(executor.map(
            lambda search_params:
                (search_params['offset'],
                 requests.get(BASE_URL, params=search_params).json()),
            search_param_list))
        print('done')

    variant_model_set = collections.defaultdict(list)
    for offset, response_data in response_data_list:
        print(f'processing {offset} of {num_results}')
        for response in response_data['response']['docs']:
            variant_label = response['variant_label'][0]
            if not variant_label.endswith(VARIANT_SUFFIX):
                continue
            experiment_id = response['experiment_id'][0]
            variable_id = response['variable_id'][0]
            source_id = response['source_id'][0]
            print(response)

            file_search_url = (
                f"{BASE_SEARCH_URL}/{response['id']}/{response['index_node']}")
            print(file_search_url)
            limit = requests.get(file_search_url).json()["response"]["numFound"]
            file_search_url += f'?limit={limit}'
            data = requests.get(file_search_url).json()['response']['docs']
            for doc_info in data:
                url = [url.split('|')[0]
                       for url in doc_info['url']
                       if url.endswith('HTTPServer')][0]
                print(url)
            return
            variant_model_set[(variable_id, experiment_id, source_id)].append(
                (variant_label, response['url']))

            # print(list(response.keys()))

            # ['id', 'version', 'access', 'activity_drs', 'activity_id',
            #  'cf_standard_name', 'citation_url', 'data_node',
            #  'data_specs_version', 'dataset_id_template_', 'datetime_start',
            #  'datetime_stop', 'directory_format_template_', 'east_degrees',
            #  'experiment_id', 'experiment_title', 'frequency',
            #  'further_info_url', 'geo', 'geo_units', 'grid', 'grid_label',
            #  'index_node', 'instance_id', 'institution_id', 'latest',
            #  'master_id', 'member_id', 'mip_era', 'model_cohort',
            #  'nominal_resolution', 'north_degrees', 'number_of_aggregations',
            #  'number_of_files', 'pid', 'product', 'project', 'realm',
            #  'replica', 'size', 'source_id', 'source_type', 'south_degrees',
            #  'sub_experiment_id', 'table_id', 'title', 'type', 'url',
            #  'variable', 'variable_id', 'variable_long_name', 'variable_units',
            #  'variant_label', 'west_degrees', 'xlink', '_version_', 'retracted', '_timestamp', 'score']
            # try:
            #     datetime_start = response['datetime_start']
            #     datetime_stop = response['datetime_stop']
            # except KeyError:
            #     datetime_start = '?'
            #     datetime_stop = datetime_start

            # %7C
            # dpesgf03.nccs.nasa.gov/esgf-node.llnl.gov/?limit=10&rnd=1686688804082
            # https://esgf-node.llnl.gov/search_files/

            # https://esgf-node.llnl.gov/search_files/CMIP6.ScenarioMIP.NASA-GISS.GISS-E2-1-G.ssp245.r101i1p1f1.Amon.pr.gn.v20220115%7C
            # https://esgf-node.llnl.gov/search_files/CMIP6.ScenarioMIP.NUIST.NESM3.ssp245.r2i1p1f1.day.pr.gn.v20190801%7Cesg.lasg.ac.cn/esgf-node.llnl.gov/?limit=10&rnd=1686688804082
            # https://esgf-node.llnl.gov/search_files/CMIP6.ScenarioMIP.NUIST.NESM3.ssp245.r2i1p1f1.day.pr.gn.v20190801%7Cesg.lasg.ac.cn/esgf-node.llnl.go/
            # {BASE_SEARCH_URL}/{response['id']}/{response['data_node']}

    # Print the results
    with open('available_models.csv', 'w') as model_table:
        model_table.write(f'variable,experiment,model,variant matching r*{VARIANT_SUFFIX}\n')
        for model_key in sorted(variant_model_set):
            variant_list = variant_model_set[model_key]
            if len(variant_list) < 10:
                print(variant_list)
                continue
            model_table.write(f'{",".join(model_key)},{",".join(sorted(variant_list))}\n')

    # print date range


if __name__ == '__main__':
    main()
