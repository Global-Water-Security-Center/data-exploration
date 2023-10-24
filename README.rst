README
======

This repository is an unstructured collection of prototypes, data processing pipelines, and helper scripts to support the work by the Global Water Security Center at the University of Alabama https://ua-gwsc.org/

Scripts
=======
average_rasters.py
------------------
usage: average_rasters.py [-h] [--target_path TARGET_PATH] raster_path_pattern

Average the rasters in the argument list.

positional arguments:
  raster_path_pattern   Path to rasters

optional arguments:
  -h, --help            show this help message and exit
  --target_path TARGET_PATH
                        Path to target raster.

box_plot_cmips5_experiment.py
-----------------------------
usage: box_plot_cmips5_experiment.py [-h]

Not a command line script. Used to manually extract pre-fetched CMIP5 processed CSVs and make box plots of those data.

optional arguments:
  -h, --help  show this help message and exit
calculate_average_monthly_rain_events.py
----------------------------------------
usage: calculate_average_monthly_rain_events.py [-h] [--authenticate] [--rain_event_threshold RAIN_EVENT_THRESHOLD] path_to_watersheds start_year end_year

Monthly rain events by watershed in an average yearly range. Output includes average yearly rasters by month indicating average number of rainfall events >
than the given rain event threshold per pixel. Two additional tables include average number of precip events over the area of interest per month, and a
daily table indicating a "1" if that day had a rain event greater than the provided threshold, and 0 if not.

positional arguments:
  path_to_watersheds    Path to vector/shapefile of watersheds
  start_year            start year YYYY
  end_year              end year YYYY

optional arguments:
  -h, --help            show this help message and exit
  --authenticate        Pass this flag if you need to reauthenticate with GEE
  --rain_event_threshold RAIN_EVENT_THRESHOLD
                        amount of rain (mm) in a day to count as a rain event

calculate_precip_pdf.py
-----------------------
usage: calculate_precip_pdf.py [-h] [--authenticate] path_to_watersheds start_date end_date

Monthly rain events by watershed in an average yearly range. Producesa histogram CSV useful for seeing the daily distribution of precipitation in the area
of interest.

positional arguments:
  path_to_watersheds  Path to vector/shapefile of watersheds
  start_date          YYYY-MM-DD
  end_date            YYYY-MM-DD

optional arguments:
  -h, --help          show this help message and exit
  --authenticate      Pass this flag if you need to reauthenticate with GEE

change_in_historical_temp.py
----------------------------
usage: change_in_historical_temp.py [-h] [--authenticate] aoi_vector_path

Examine historical change of temp in gregion. Produces files with the pattern ``historic_mean_temp_difference_{scenarioid}.tif`` in the working directory.

positional arguments:
  aoi_vector_path  Path to vector/shapefile of area of interest

optional arguments:
  -h, --help       show this help message and exit
  --authenticate   Pass this flag if you need to reauthenticate with GEE

cimp5_rain_event_extractor.py
-----------------------------
usage: cimp5_rain_event_extractor.py [-h] [--authenticate] country_name start_date end_date

Calculate rain events from CIMP5 data from GEE. Generates a CSV table that whose rows are days and columns are daily mean precip over the given country.

positional arguments:
  country_name    Path to vector/shapefile of watersheds
  start_date      start date YYYY-MM-DD
  end_date        end date YYYY-MM-DD

optional arguments:
  -h, --help      show this help message and exit
  --authenticate  Pass this flag if you need to reauthenticate with GEE

cmip6_download.py
-----------------
usage: cmip6_download.py [-h] url_list_path

Process CMIP6 raw urls to geotiff.

positional arguments:
  url_list_path  url list

optional arguments:
  -h, --help     show this help message and exit
cmip6_explorer.py
-----------------
usage: cmip6_explorer.py [-h] date_range [date_range ...] point point

Process CMIP6 raw urls to geotiff.

positional arguments:
  date_range  list of years to analyze
  point       lat/lng point to analyze

optional arguments:
  -h, --help  show this help message and exit
cmip6_fetch_tool.py
-------------------
usage: cmip6_fetch_tool.py [-h] --variable_id VARIABLE_ID --aggregate_function AGGREGATE_FUNCTION --aoi_vector_path AOI_VECTOR_PATH
                           [--where_statement WHERE_STATEMENT] [--year_range YEAR_RANGE YEAR_RANGE] --scenario_id SCENARIO_ID --season_range SEASON_RANGE
                           SEASON_RANGE [--dataset_scale DATASET_SCALE] --target_table_path TARGET_TABLE_PATH [--file_prefix FILE_PREFIX]
                           [--eval_cmd EVAL_CMD]

Fetch CMIP6 data cut by model and by year.

optional arguments:
  -h, --help            show this help message and exit
  --variable_id VARIABLE_ID
                        variable to process
  --aggregate_function AGGREGATE_FUNCTION
                        either "sum", or "mean"
  --aoi_vector_path AOI_VECTOR_PATH
                        Path to vector/shapefile of area of interest
  --where_statement WHERE_STATEMENT
                        If provided, allows filtering by a field id and value of the form field_id=field_value
  --year_range YEAR_RANGE YEAR_RANGE
                        Two year ranges in YYYY format to download between.
  --scenario_id SCENARIO_ID
                        Scenario ID ssp245, ssp585, historical
  --season_range SEASON_RANGE SEASON_RANGE
                        Julian start/end day of analysis
  --dataset_scale DATASET_SCALE
                        Override the base scale of 27830m to whatever you desire.
  --target_table_path TARGET_TABLE_PATH
                        Name of target table
  --file_prefix FILE_PREFIX
                        Prefix the output file with this.
  --eval_cmd EVAL_CMD   an arbitrary command using "var" as the variable to do any final conversion

cmip6_raster_fetch.py
---------------------
usage: cmip6_raster_fetch.py [-h] [--field_id_for_aggregate FIELD_ID_FOR_AGGREGATE] [--where_statement WHERE_STATEMENT]
                             [--date_range DATE_RANGE [DATE_RANGE ...]] [--dataset_scale DATASET_SCALE] [--table_path TABLE_PATH]
                             [--n_day_window N_DAY_WINDOW]
                             aoi_vector_path

Fetch CMIP6 temperature and precipitation monthly normals given a year date range.

positional arguments:
  aoi_vector_path       Path to vector/shapefile of area of interest

optional arguments:
  -h, --help            show this help message and exit
  --field_id_for_aggregate FIELD_ID_FOR_AGGREGATE
                        Field ID in aoi for aggregating.
  --where_statement WHERE_STATEMENT
                        If provided, allows filtering by a field id and value of the form field_id=field_value
  --date_range DATE_RANGE [DATE_RANGE ...]
                        Date ranges in YYYY-YYYY format to download between.
  --dataset_scale DATASET_SCALE
                        Dataset scale
  --table_path TABLE_PATH
                        Desired output table path.
  --n_day_window N_DAY_WINDOW
                        Number of days in which to average around

cmip6_search.py
---------------
usage: cmip6_search.py [-h] [--variables VARIABLES [VARIABLES ...]] [--experiments EXPERIMENTS [EXPERIMENTS ...]] [--missed_url_file MISSED_URL_FILE]
                       [--local_workspace LOCAL_WORKSPACE]

Fetch CMIP6 variables into wasabi hot storage.

optional arguments:
  -h, --help            show this help message and exit
  --variables VARIABLES [VARIABLES ...]
                        Could be "pr", "tas" etc.
  --experiments EXPERIMENTS [EXPERIMENTS ...]
                        Experiments to search for
  --missed_url_file MISSED_URL_FILE
                        overrides a general search and instead fetches urls missing from a previous run
  --local_workspace LOCAL_WORKSPACE
                        Directory to downloand and work in.

cmip6_yearly_total_precip_explorer.py
-------------------------------------
usage: cmip6_yearly_total_precip_explorer.py [-h] point point

A script used to generate box plots to interpret CMIP6 raw data.

positional arguments:
  point       lat/lng point to analyze

optional arguments:
  -h, --help  show this help message and exit
cmips5_95th_rain_events.py
--------------------------
usage: cmips5_95th_rain_events.py [-h]

Not a command line script. Experiment to: Get the historical 95th percentile of rain in one day as a threshold to establish what a heavy rain event looks
like and report the number of heavy rain days per year in the historical time period (can be averaged over the time period if that's useful) Using that same
heavy rain event threshold, report the number of heavy rain days per year for the three future time steps (could also be averaged over each time step if
that's useful) user analysis: Are there many more heavy rain days in future time steps than there were in the historical time period?

optional arguments:
  -h, --help  show this help message and exit
explore_cmip5.py
----------------
usage: explore_cmip5.py [-h] [--aggregate_by_field AGGREGATE_BY_FIELD] [--authenticate] aoi_vector_path start_date end_date

Extract CIMP5 data from GEE given an AOI and date range. Produces a CSV table with the pattern `CIMP5_{unique_id}.csv` with monthly means for precipitation
and temperature broken down by model.

positional arguments:
  aoi_vector_path       Path to vector/shapefile of area of interest
  start_date            start date YYYY-MM-DD
  end_date              end date YYYY-MM-DD

optional arguments:
  -h, --help            show this help message and exit
  --aggregate_by_field AGGREGATE_BY_FIELD
                        If provided, this aggregates results by the unique values found in the field in `aoi_vector_path`
  --authenticate        Pass this flag if you need to reauthenticate with GEE

explore_indicies.py
-------------------
usage: explore_indicies.py [-h]

Not a command line script. Was used to explore how to extract rain events by watershed in a time range. API docs at: https://climate-
indices.readthedocs.io/en/latest/

optional arguments:
  -h, --help  show this help message and exit
extract_drought_thresholds_from_aer_gdm.py
------------------------------------------
usage: extract_drought_thresholds_from_aer_gdm.py [-h] [--filter_aoi_by_field FILTER_AOI_BY_FIELD] aoi_vector_path start_date end_date

Extract SPEI12 thresholds from https://h2o.aer.com/thredds/dodsC/gwsc/gdm and produce a CSV that breaks down analysis by year to highlight how many months
experience drought in 1/3, 1/2, and 2/3 of region. Results are in three files: (1) spei12_drought_info_raw_{aoi}.csv contains month by month aggregates, (2)
spei12_drought_events_by_pixel_{aoi}.tif contains pixels whose values are the number of months drought during the query time range and (3)
spei12_drought_info_by_year_{aoi}.csv, summaries of total number of drought events per year in the AOI.

positional arguments:
  aoi_vector_path       Path to vector/shapefile of area of interest
  start_date            start date YYYY-MM
  end_date              end date YYYY-MM

optional arguments:
  -h, --help            show this help message and exit
  --filter_aoi_by_field FILTER_AOI_BY_FIELD
                        an argument of the form FIELDNAME=VALUE such as `sov_a3=AFG`

fetch_aer_anomalies.py
----------------------
usage: fetch_aer_anomalies.py [-h] [--local_workspace LOCAL_WORKSPACE] --path_to_aoi PATH_TO_AOI [--filter_aoi_by_field FILTER_AOI_BY_FIELD]
                              start_date end_date

Fetch and clip AER ERA anomaly data.

positional arguments:
  start_date            Pick a date to start downloading YYYY-MM.
  end_date              Pick a date to start downloading YYYY_MM.

optional arguments:
  -h, --help            show this help message and exit
  --local_workspace LOCAL_WORKSPACE
                        Directory to downloand and work in.
  --path_to_aoi PATH_TO_AOI
                        Path to clip AOI from
  --filter_aoi_by_field FILTER_AOI_BY_FIELD
                        an argument of the form FIELDNAME=VALUE such as `sov_a3=AFG`

kenya_drought_analysis.py
-------------------------
usage: kenya_drought_analysis.py [-h] [--aggregate_by_field AGGREGATE_BY_FIELD] [--authenticate] aoi_vector_path start_date end_date

In development -- modification of extract hard coded Kenya drought data from CMIP5.

positional arguments:
  aoi_vector_path       Path to vector/shapefile of area of interest
  start_date            start date YYYY-MM-DD
  end_date              end date YYYY-MM-DD

optional arguments:
  -h, --help            show this help message and exit
  --aggregate_by_field AGGREGATE_BY_FIELD
                        If provided, this aggregates results by the unique values found in the field in `aoi_vector_path`
  --authenticate        Pass this flag if you need to reauthenticate with GEE

monthly_and_annual_precip_temp_in_watershed.py
----------------------------------------------
usage: monthly_and_annual_precip_temp_in_watershed.py [-h] --date_range DATE_RANGE DATE_RANGE [--filter_aoi_by_field FILTER_AOI_BY_FIELD] path_to_aoi

Given a region and a time period create four tables (1) monthly precip and mean temperature and (2) annual rainfall, (3) monthly normal temp, and (4)
monthly normal precip over the query time period as well as two rasters: (5) total precip sum over AOI and (6) overall monthly temperture mean in the AOI.

positional arguments:
  path_to_aoi           Path to vector/shapefile of watersheds

optional arguments:
  -h, --help            show this help message and exit
  --date_range DATE_RANGE DATE_RANGE
                        Pass a pair of start/end dates in the (YYYY-MM-DD) format
  --filter_aoi_by_field FILTER_AOI_BY_FIELD
                        an argument of the form FIELDNAME=VALUE such as `sov_a3=AFG`

ncinfo.py
---------
usage: ncinfo.py [-h] raster_path

Dump netcdf info on a file to command line.

positional arguments:
  raster_path  path to netcdf file

optional arguments:
  -h, --help   show this help message and exit
netcat_to_geotiff_kenya_drought.py
----------------------------------
usage: netcat_to_geotiff_kenya_drought.py [-h]

not a command line script -- used to process local `Kenya_drought_2012-01-01_2022-03-01_v2.nc`

optional arguments:
  -h, --help  show this help message and exit
netcdf_to_geotiff.py
--------------------
usage: netcdf_to_geotiff.py [-h] [--band_field BAND_FIELD] [--target_nodata TARGET_NODATA] netcdf_path x_y_fields x_y_fields out_dir

Convert netcdf files to geotiff

positional arguments:
  netcdf_path           Path or pattern to netcdf files to convert
  x_y_fields            the names of the x and y coordinates in the netcdf file
  out_dir               path to output directory

optional arguments:
  -h, --help            show this help message and exit
  --band_field BAND_FIELD
                        if defined, will use this coordinate as the band field
  --target_nodata TARGET_NODATA
                        Set this as target nodata value if desired

nex_gddp_cmip6_explorer.py
--------------------------
usage: nex_gddp_cmip6_explorer.py [-h] [--authenticate] [--where_statement WHERE_STATEMENT] [--temperature_threshold TEMPERATURE_THRESHOLD]
                                  [--season_range SEASON_RANGE] [--year_range YEAR_RANGE]
                                  aoi_vector_path

Experiments on NEX GDDP CMIP6 data.

positional arguments:
  aoi_vector_path       Path to vector/shapefile of area of interest

optional arguments:
  -h, --help            show this help message and exit
  --authenticate        Pass this flag if you need to reauthenticate with GEE
  --where_statement WHERE_STATEMENT
                        If provided, allows filtering by a field id and value of the form field_id=field_value
  --temperature_threshold TEMPERATURE_THRESHOLD
                        Temp threshold in C.
  --season_range SEASON_RANGE
                        Two numbers separated by a hyphen representing the start and end day of a season in Julian calendar days. Negative numbers refer to
                        the previous year and >365 indicates the next year. i.e. 201-320 or -10-65
  --year_range YEAR_RANGE
                        A start and end year date as a hypenated string to run the analysis on.

rename_date_prefixed_files.py
-----------------------------
usage: rename_date_prefixed_files.py [-h] [--new_date NEW_DATE] [--rename RENAME] directories_to_search [directories_to_search ...]

Script to rename files with the pattern (.*QL)\d{8}(-.*) to \g<1>{NEW_DATE}\g<2>.

positional arguments:
  directories_to_search
                        Path/pattern to directories to search

optional arguments:
  -h, --help            show this help message and exit
  --new_date NEW_DATE   Date pattern to replace the matching pattern with, default is current date as 20231010.
  --rename RENAME       Pass with an argument of True to do the rename, otherwise it lists what the renames will be.

storm_event_detection.py
------------------------
usage: storm_event_detection.py [-h] --date_range DATE_RANGE DATE_RANGE [--rain_event_threshold RAIN_EVENT_THRESHOLD] path_to_watersheds

Detect storm events in a 48 hour window using a threshold for precip. Result is located in a directory called `workspace_{vector name}` and contains rasters
for each month over the time period showing nubmer of precip events per pixel, a raster prefixed with "overall_" showing the overall storm event per pixel,
and a CSV table prefixed with the vector basename and time range showing number of events in the region per month.

positional arguments:
  path_to_watersheds    Path to vector/shapefile of watersheds

optional arguments:
  -h, --help            show this help message and exit
  --date_range DATE_RANGE DATE_RANGE
                        Pass a pair of start/end dates in the (YYYY-MM-DD) format
  --rain_event_threshold RAIN_EVENT_THRESHOLD
                        amount of rain (mm) in a day to count as a rain event

sub_rasters.py
--------------
usage: sub_rasters.py [-h] [--target_path TARGET_PATH] raster_a raster_b

Calculate raster_a - raster_b.

positional arguments:
  raster_a              Path to raster A
  raster_b              Path to raster B

optional arguments:
  -h, --help            show this help message and exit
  --target_path TARGET_PATH
                        Path to target raster.

update_era5.py
--------------
usage: update_era5.py [-h] [--local_workspace LOCAL_WORKSPACE] start_date end_date

Synchronize the files in AER era5 to GWSC wasabi hot storage.

positional arguments:
  start_date            Pick a date to start downloading.
  end_date              Pick a date to start downloading.

optional arguments:
  -h, --help            show this help message and exit
  --local_workspace LOCAL_WORKSPACE
                        Directory to downloand and work in.

