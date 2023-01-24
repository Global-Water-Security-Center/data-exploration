"""Calculate average monthly events over a given time period."""
import argparse
import os
import concurrent
import shutil
import tempfile

import ee
import geemap
import geopandas
import requests

ERA5_RESOLUTION_M = 27830

month_list = [
    '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
days_in_month_list = [
    '31', '28', '31', '30', '31', '30', '31', '31', '30', '31', '30', '31']

# (ID, DATASET, BAND ID, RESOLUTION IN M, SCALE FACTOR)
DATASETS = [
    ('ERA5', 'ECMWF/ERA5/DAILY', 'total_precipitation', 27830, 1000.0),
    ('CHIRPS', 'UCSB-CHG/CHIRPS/DAILY', 'precipitation', 5566, 1),
    ]

TARGET_RESOLUTION = max([x[3] for x in DATASETS])


def main():
    parser = argparse.ArgumentParser(
        description='Monthly rain events by watershed in an average yearly range.')
    parser.add_argument(
        'path_to_watersheds', help='Path to vector/shapefile of watersheds')
    parser.add_argument('start_year', type=int, help='start year YYYY')
    parser.add_argument('end_year', type=int, help='end year YYYY')
    parser.add_argument(
        '--authenticate', action='store_true',
        help='Pass this flag if you need to reauthenticate with GEE')
    parser.add_argument(
        '--rain_event_threshold', default=0.1, type=float,
        help='amount of rain (mm) in a day to count as a rain event')
    args = parser.parse_args()

    if args.authenticate:
        ee.Authenticate()
        return
    ee.Initialize()

    # convert to GEE polygon
    gp_poly = geopandas.read_file(args.path_to_watersheds).to_crs('EPSG:4326')
    workspace_dir = tempfile.mkdtemp(prefix='ok_to_delete_')
    local_shapefile_path = os.path.join(
        workspace_dir, '_local_ok_to_delete.shp')
    gp_poly.to_file(local_shapefile_path)
    gp_poly = None
    ee_poly = geemap.shp_to_ee(local_shapefile_path)

    # landcover code 200 is open ocean, so use that as a mask
    land_mask = ee.ImageCollection(
        "COPERNICUS/Landcover/100m/Proba-V-C3/Global").filter(
        ee.Filter.date('2018-01-01', '2018-01-02')).select(
        'discrete_classification').toBands().eq(200).Not()

    poly_mask = land_mask.clip(ee_poly)
    land_pixel_count_reducer = poly_mask.reduceRegion(**{
        'reducer': 'sum',
        'geometry': ee_poly,
        'maxPixels': 1e15,
        })
    land_pixel_count = land_pixel_count_reducer.getInfo()[
        '2018_discrete_classification']
    print(land_pixel_count)

    url_fetch_worker_list = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for dataset_id, dataset, band_id, resolution_in_m, scale_factor in DATASETS:
            precip_count_list = ee.List([])
            base_collection = ee.ImageCollection(dataset)
            for month_val, day_in_month in zip(month_list, days_in_month_list):
                monthly_rain_event_image = ee.Image.constant(0).mask(poly_mask)
                for year in range(args.start_year, args.end_year+1):
                    start_date = f'{year}-{month_val}-01'
                    end_date = f'{year}-{month_val}-{day_in_month}'
                    month_collection = base_collection.filterDate(
                        start_date, end_date)
                    daily_precip = month_collection.select(
                        band_id).toBands().multiply(scale_factor)  # convert to mm
                    daily_precip_events = daily_precip.where(
                        daily_precip.lt(args.rain_event_threshold), 0).where(
                        daily_precip.gte(args.rain_event_threshold), 1)

                    precip_event_sum = daily_precip_events.reduce('sum').clip(
                        ee_poly).mask(poly_mask)

                    monthly_rain_event_image = monthly_rain_event_image.add(
                        precip_event_sum)

                    # calcualte the average number of precip events in the area
                    precip_count_redcucer = precip_event_sum.reduceRegion(**{
                        'reducer': 'sum',
                        'geometry': ee_poly,
                        'maxPixels': 1e15,
                        })
                    precip_count_list = precip_count_list.add((f'{year}-{month_val}', precip_count_redcucer))

                monthly_rain_event_image = monthly_rain_event_image.divide(
                    args.end_year+1-args.start_year)

                vector_basename = os.path.basename(os.path.splitext(args.path_to_watersheds)[0])
                precip_path = f"{vector_basename}_{dataset_id}_avg_precip_events_{args.start_year}_{args.end_year}_{month_val}_{args.rain_event_threshold}.tif"

                url_fetch_worker_list.append(
                    executor.submit(
                        save_raster, monthly_rain_event_image, land_mask,
                        ee_poly, resolution_in_m, precip_path))

            table_path = f"{vector_basename}_{dataset_id}_avg_precip_events_{args.start_year}_{args.end_year}_{args.rain_event_threshold}.csv"
            with open(table_path, 'w') as table_file:
                table_file.write('date,avg rain events over area\n')
                for index, (date, precip_count_sum) in enumerate(sorted(precip_count_list.getInfo())):
                    table_file.write(f'''{date},{
                        precip_count_sum["sum"]/land_pixel_count}\n''')

        for future in url_fetch_worker_list:
            try:
                _ = future.result()
            except Exception as exc:
                print('generated an exception: %s' % (exc))
    shutil.rmtree(workspace_dir)


def save_raster(image, mask, region, scale, target_path):
    """Write `url` to `target_path`."""
    print(scale)
    url = image.reduceResolution(
        **{'reducer': ee.Reducer.max()}).mask(mask).getDownloadUrl(
            {
                'region': region.geometry().bounds(),
                'scale': scale,
                'format': 'GEO_TIFF'
            })
    response = requests.get(url)
    print(f'write {target_path}')
    with open(target_path, 'wb') as fd:
        fd.write(response.content)
    return True


if __name__ == '__main__':
    main()
