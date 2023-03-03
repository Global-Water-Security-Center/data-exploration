README
======

This repository is an unstructured collection of prototypes, data processing pipelines, and helper scripts to support the work by the Global Water Security Center at the University of Alabama https://ua-gwsc.org/

Scripts
=======
box_plot_cmips5_experiment.py
-----------------------------
usage: box_plot_cmips5_experiment.py [-h]

Not a command line script. Used to manually extract pre-fetched CMIP5 processed CSVs and make box plots of those data.


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

options:
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

options:
  -h, --help          show this help message and exit
  --authenticate      Pass this flag if you need to reauthenticate with GEE

change_in_historical_temp.py
----------------------------
usage: change_in_historical_temp.py [-h] [--authenticate] aoi_vector_path

Examine historical change of temp in gregion. Produces files with the pattern ``historic_mean_temp_difference_{scenarioid}.tif`` in the working directory.

positional arguments:
  aoi_vector_path  Path to vector/shapefile of area of interest

options:
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

options:
  -h, --help      show this help message and exit
  --authenticate  Pass this flag if you need to reauthenticate with GEE

cmips5_95th_rain_events.py
--------------------------
usage: cmips5_95th_rain_events.py [-h]

Not a command line script. Exploration of 95th percentile of historical rain events to predict future rain events.


download_cmip5_day.py
---------------------
usage: download_cmip5_day.py [-h]

Not a command line script. Used to download and inspect the size of a GEE CMIP5 daily raster to estimate total offline storage.


explore_cimp5.py
----------------
usage: explore_cimp5.py [-h] [--aggregate_by_field AGGREGATE_BY_FIELD] [--authenticate] aoi_vector_path start_date end_date

Extract CIMP5 data from GEE given an AOI and date range. Produces a CSV table with the pattern `CIMP5_{unique_id}.csv` with monthly means for precipitation
and temperature broken down by model.

positional arguments:
  aoi_vector_path       Path to vector/shapefile of area of interest
  start_date            start date YYYY-MM-DD
  end_date              end date YYYY-MM-DD

options:
  -h, --help            show this help message and exit
  --aggregate_by_field AGGREGATE_BY_FIELD
                        If provided, this aggregates results by the unique values found in the field in `aoi_vector_path`
  --authenticate        Pass this flag if you need to reauthenticate with GEE

explore_cimp6.py
----------------
usage: explore_cimp6.py [-h]

Not a command line script. Incomplete tracer code to fetch CMIP6 data.


explore_gdm.py
--------------
usage: explore_gdm.py [-h]

Not a command line script. Used to explore how to extract data from netcat files extracted from AER's THREADD DODSC GWSC server.


explore_indicies.py
-------------------
usage: explore_indicies.py [-h]

Not a command line script. Used to explore how to extract rain events by watershed in a time range.


extract_drought_thresholds_from_aer_gdm.py
------------------------------------------
usage: extract_drought_thresholds_from_aer_gdm.py [-h] aoi_vector_path start_date end_date

Extract drought thresholds from https://h2o.aer.com/thredds/dodsC/gwsc/gdm and produce a CSV that breaks down analysis by year to highlight how many months
experience drought in 1/3, 1/2, and 2/3 of region.

positional arguments:
  aoi_vector_path  Path to vector/shapefile of area of interest
  start_date       start date YYYY-MM-DD
  end_date         end date YYYY-MM-DD


kenya_drought_analysis.py
-------------------------
usage: kenya_drought_analysis.py [-h] [--aggregate_by_field AGGREGATE_BY_FIELD] [--authenticate] aoi_vector_path start_date end_date

In development -- modification of extract hard coded Kenya drought data from CMIP5.

positional arguments:
  aoi_vector_path       Path to vector/shapefile of area of interest
  start_date            start date YYYY-MM-DD
  end_date              end date YYYY-MM-DD

options:
  -h, --help            show this help message and exit
  --aggregate_by_field AGGREGATE_BY_FIELD
                        If provided, this aggregates results by the unique values found in the field in `aoi_vector_path`
  --authenticate        Pass this flag if you need to reauthenticate with GEE

monthly_and_annual_precip_temp_in_watershed.py
----------------------------------------------
usage: monthly_and_annual_precip_temp_in_watershed.py [-h] [--authenticate] path_to_watersheds start_date end_date

Given a region and a time period, create two tables (1) monthly precip and mean temporature and (2) showing annual rainfall, as well as two rasters (3)
total precip sum in AOI and (4) overall monthly temperture mean in the AOI.

positional arguments:
  path_to_watersheds  Path to vector/shapefile of watersheds
  start_date          start date for summation (YYYY-MM-DD) format
  end_date            start date for summation (YYYY-MM-DD) format

options:
  -h, --help          show this help message and exit
  --authenticate      Pass this flag if you need to reauthenticate with GEE

ncinfo.py
---------
usage: ncinfo.py [-h] raster_path

Dump netcat info on a file to command line.

positional arguments:
  raster_path  path to netcat file


netcat_to_geotiff.py
--------------------
usage: netcat_to_geotiff.py [-h]

not a command line script -- used to process local `Kenya_drought_2012-01-01_2022-03-01_v2.nc`


storm_event_detection.py
------------------------
usage: storm_event_detection.py [-h] [--authenticate] [--rain_event_threshold RAIN_EVENT_THRESHOLD] path_to_watersheds start_date end_date

Detect storm events in a 48 hour window using a threshold for precip. Result is a geotiff raster whose pixels show the count of detected rain events within
a 48 hour period with the suffix ``_48hr_avg_precip_events.tif``.

positional arguments:
  path_to_watersheds    Path to vector/shapefile of watersheds
  start_date            start date for summation (YYYY-MM-DD) format
  end_date              start date for summation (YYYY-MM-DD) format

options:
  -h, --help            show this help message and exit
  --authenticate        Pass this flag if you need to reauthenticate with GEE
  --rain_event_threshold RAIN_EVENT_THRESHOLD
                        amount of rain (mm) in a day to count as a rain event

