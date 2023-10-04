setlocal enabledelayedexpansion

set startyear=2035
set endyear=2065

for %%c in (120 221 321) do (
    call python cmip6_fetch_tool.py --aoi_vector_path base_data/moz_regions --where_statement CR=%%c --scenario_id ssp245 --year_range %startyear% %endyear% --season_range 1 365 --variable_id pr --aggregate_function sum --eval_cmd 86400*var --target_table_path cmip6_output/ --file_prefix annual_total_precip_mm_

    call python cmip6_fetch_tool.py --aoi_vector_path base_data/moz_regions --where_statement CR=%%c --scenario_id ssp245 --year_range %startyear% %endyear% --season_range 1 365 --variable_id tas --aggregate_function mean --eval_cmd var-272.15 --target_table_path cmip6_output/ --file_prefix annual_mean_temp_c_

    rem oct to feb is day 247 to feb 28 whch is day 59 so 365+59=424
    call python cmip6_fetch_tool.py --aoi_vector_path base_data/moz_regions --where_statement CR=%%c --scenario_id ssp245 --year_range %startyear% %endyear% --season_range 247 424 --variable_id tas --aggregate_function gt_309.15 --target_table_path cmip6_output/ --file_prefix days_above_36c_oct_through_feb_
)

for %%c in (1 2 3 4) do (
    call python cmip6_fetch_tool.py --aoi_vector_path base_data/HN_Koppen4Class.gpkg --where_statement KC=%%c --scenario_id ssp245 --year_range %startyear% %endyear% --season_range 1 365 --variable_id pr --aggregate_function sum --eval_cmd 86400*var --target_table_path cmip6_output/ --file_prefix annual_total_precip_mm_

    REM 121 id may 1 243 is aug 31
    call python cmip6_fetch_tool.py --aoi_vector_path base_data/HN_Koppen4Class.gpkg --where_statement KC=%%c --scenario_id ssp245 --year_range %startyear% %endyear% --season_range 121 243 --variable_id pr --aggregate_function sum --eval_cmd 86400*var --target_table_path cmip6_output/ --file_prefix total_precip_mm_may_through_aug

    call python cmip6_fetch_tool.py --aoi_vector_path base_data/HN_Koppen4Class.gpkg --where_statement KC=%%c --scenario_id ssp245 --year_range %startyear% %endyear% --season_range 1 365 --variable_id tas --aggregate_function mean --eval_cmd var-272.15 --target_table_path cmip6_output/ --file_prefix annual_mean_temp_c_

    REM 303.15 is 30 deg C
    call python cmip6_fetch_tool.py --aoi_vector_path base_data/HN_Koppen4Class.gpkg --where_statement KC=%%c --scenario_id ssp245 --year_range %startyear% %endyear% --season_range 1 365 --variable_id tas --aggregate_function gt_303.15 --target_table_path cmip6_output/ --file_prefix days_above_30c_

    call python cmip6_fetch_tool.py --aoi_vector_path base_data/HN_Koppen4Class.gpkg --where_statement KC=%%c --scenario_id ssp245 --year_range %startyear% %endyear% --season_range 121 243 --variable_id tas --aggregate_function gt_309.15 --target_table_path cmip6_output/ --file_prefix days_above_36c_may_aug_
)

REM For years: 2035-2065
REM x Total annual precipitation
REM Total May-August precipitation (Honduras, Maize)
REM
REM Mean annual temperature
REM Days per year above 30 C (Honduras, Coffee)
REM Days May-August above 36 C (Honduras, maize, grain filling)
REM Days Oct-Feb above 36ºC (Mozambique, maize)
REM I'm also attaching a vector of the climate regions for Mozambique.

