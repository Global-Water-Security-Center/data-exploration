"""Calculate average monthly events over a given time period."""
import argparse
import os
import concurrent

import ee
import geemap
import geopandas

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
    parser = argparse.ArgumentParser(description=(
        'Monthly rain events by watershed in an average yearly range. Produces'
        'a histogram CSV useful for seeing the daily distribution of '
        'precipitation in the area of interest.'))
    parser.add_argument(
        'path_to_watersheds', help='Path to vector/shapefile of watersheds')
    parser.add_argument('start_date', help='YYYY-MM-DD')
    parser.add_argument('end_date', help='YYYY-MM-DD')
    parser.add_argument(
        '--authenticate', action='store_true',
        help='Pass this flag if you need to reauthenticate with GEE')
    args = parser.parse_args()

    if args.authenticate:
        ee.Authenticate()
        return
    ee.Initialize()

    # convert to GEE polygon
    gp_poly = geopandas.read_file(args.path_to_watersheds).to_crs('EPSG:4326')
    local_shapefile_path = '_local_ok_to_delete.shp'
    gp_poly.to_file(local_shapefile_path)
    gp_poly = None
    ee_poly = geemap.shp_to_ee(local_shapefile_path)

    # landcover code 200 is open ocean, so use that as a mask
    land_mask = ee.ImageCollection(
        "COPERNICUS/Landcover/100m/Proba-V-C3/Global").filter(
        ee.Filter.date('2018-01-01', '2018-01-02')).select(
        'discrete_classification').toBands().eq(200).Not()

    poly_mask = land_mask.clip(ee_poly)
    #save_raster(poly_mask, ee_poly, 1000, 'mask.tif')

    url_fetch_worker_list = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        for visual_id, dataset, band_id, resolution_in_m, scale_factor in DATASETS:
            base_collection = ee.ImageCollection(dataset)
            #for month_val, day_in_month in zip(month_list, days_in_month_list):
            #rain_event_image = ee.Image.constant(0).mask(poly_mask)
            #       for year in range(args.start_year, args.end_year+1):
            # start_date = f'{year}-{month_val}-01'
            # end_date = f'{year}-{month_val}-{day_in_month}'
            precip_collection = base_collection.filterDate(
                args.start_date, args.end_date)
            daily_precip = precip_collection.\
                select(band_id).toBands().\
                clip(ee_poly).\
                mask(poly_mask).\
                multiply(scale_factor)

            # daily_precip = daily_precip.where(
            #     daily_precip.lt(args.rain_event_threshold), 0).where(
            #     daily_precip.gte(args.rain_event_threshold), 1)

            # precip_event_sum = daily_precip.reduce('sum').clip(
            #     ee_poly).mask(poly_mask)
            # rain_event_image = rain_event_image.add(
            #     precip_event_sum)

            # rain_event_image = rain_event_image.divide(
            #     args.end_year+1-args.start_year)

            vector_basename = os.path.basename(os.path.splitext(args.path_to_watersheds)[0])
            precip_path = f"{vector_basename}_{visual_id}_avg_precip_events_{args.start_date}_{args.end_date}.tif"

            url_fetch_worker_list.append(
                (executor.submit(
                    calculate_pdf, daily_precip, land_mask,
                    ee_poly, resolution_in_m, precip_path), visual_id))

        for (future, dataset_id) in url_fetch_worker_list:
            try:
                # result is a dictionary with band and 2d array
                histogram = next(iter(future.result().values()))

                with open(f'histogram_{vector_basename}_{args.start_date}_{args.end_date}_{dataset_id}.csv', 'w') as histogram_table:
                    histogram_table.write('rainfall (mm),pdf\n')
                    total_count = sum([x[1] for x in histogram])
                    for val, count in histogram:
                        pdf = count/total_count
                        histogram_table.write(f'{val},{pdf}\n')

            except Exception as exc:
                print('generated an exception: %s' % (exc))


def calculate_pdf(image, mask, region, scale, target_path):
    """Write `url` to `target_path`."""
    print(scale)

    #ee.Reducer.autoHistogram(maxBuckets, minBucketWidth, maxRaw, cumulative)
    precip_pdf = image.reduceRegion(**{
        'reducer': ee.Reducer.fixedHistogram(**{
            'min': 0.05,
            'max': 50,
            'steps': 100,
            }),
        'geometry': region})

    histogram = precip_pdf.getInfo()
    print(type(histogram))
    return histogram
    # response = requests.get(url)
    # print(f'write {target_path}')
    # with open(target_path, 'wb') as fd:
    #     fd.write(response.content)
    # return True


if __name__ == '__main__':
    main()
