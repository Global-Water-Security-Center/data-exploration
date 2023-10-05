setlocal enabledelayedexpansion

call python cmip6_fetch_tool.py --aoi_vector_path base_data/HN_Koppen4Class.gpkg --where_statement KC=4 --scenario_id %scenario% --year_range 2035 2065 --season_range 1 365 --variable_id tas --aggregate_function mean --eval_cmd var-272.15 --target_table_path cmip6_output/ --file_prefix HN_annual_mean_temp_c_

set startyear=1985
set endyear=2014
set scenario=historical

for %%c in (120 221 321) do (
    REM x Total annual precipitation
    call python cmip6_fetch_tool.py --aoi_vector_path base_data/moz_regions --where_statement CR=%%c --scenario_id %scenario% --year_range %startyear% %endyear% --season_range 1 365 --variable_id pr --aggregate_function sum --eval_cmd 86400*var --target_table_path cmip6_output/ --file_prefix MOZ_annual_total_precip_mm_

    REM Mean annual temperature
    call python cmip6_fetch_tool.py --aoi_vector_path base_data/moz_regions --where_statement CR=%%c --scenario_id %scenario% --year_range %startyear% %endyear% --season_range 1 365 --variable_id tas --aggregate_function mean --eval_cmd var-272.15 --target_table_path cmip6_output/ --file_prefix MOZ_annual_mean_temp_c_

    REM Days Oct-Feb above 36ºC (Mozambique, maize)
    rem oct to feb is day 247 to feb 28 whch is day 59 so 365+59=424
    rem missing cr 120 for this one
    call python cmip6_fetch_tool.py --aoi_vector_path base_data/moz_regions --where_statement CR=%%c --scenario_id %scenario% --year_range %startyear% %endyear% --season_range 247 59 --variable_id tas --aggregate_function gt_309.15 --target_table_path cmip6_output/ --file_prefix MOZ_days_above_36c_oct_through_feb_ --dataset_scale 30000
)

REM for %%c in (1 2 3 4) do (
REM for %%c in (2 3 4) do (
    REM x Total annual precipitation
    call python cmip6_fetch_tool.py --aoi_vector_path base_data/HN_Koppen4Class.gpkg --where_statement KC=%%c --scenario_id %scenario% --year_range %startyear% %endyear% --season_range 1 365 --variable_id pr --aggregate_function sum --eval_cmd 86400*var --target_table_path cmip6_output/ --file_prefix HN_annual_total_precip_mm_

    REM Total May-August precipitation (Honduras, Maize)
    REM 121 id may 1 243 is aug 31
    call python cmip6_fetch_tool.py --aoi_vector_path base_data/HN_Koppen4Class.gpkg --where_statement KC=%%c --scenario_id %scenario% --year_range %startyear% %endyear% --season_range 121 243 --variable_id pr --aggregate_function sum --eval_cmd 86400*var --target_table_path cmip6_output/ --file_prefix HN_total_precip_mm_may_through_aug

    REM Mean annual temperature
    call python cmip6_fetch_tool.py --aoi_vector_path base_data/HN_Koppen4Class.gpkg --where_statement KC=%%c --scenario_id %scenario% --year_range %startyear% %endyear% --season_range 1 365 --variable_id tas --aggregate_function mean --eval_cmd var-272.15 --target_table_path cmip6_output/ --file_prefix HN_annual_mean_temp_c_

    REM Days per year above 30 C (Honduras, Coffee)
    REM 303.15 is 30 deg C
    call python cmip6_fetch_tool.py --aoi_vector_path base_data/HN_Koppen4Class.gpkg --where_statement KC=%%c --scenario_id %scenario% --year_range %startyear% %endyear% --season_range 1 182 --variable_id tas --aggregate_function gt_303.15 --target_table_path cmip6_output/ --File_prefix HN_days_above_30c_jan-jun

    call python cmip6_fetch_tool.py --aoi_vector_path base_data/HN_Koppen4Class.gpkg --where_statement KC=%%c --scenario_id %scenario% --year_range %startyear% %endyear% --season_range 183 365 --variable_id tas --aggregate_function gt_303.15 --target_table_path cmip6_output/ --file_prefix HN_days_above_30c_jul-dec

    call python cmip6_fetch_tool.py --aoi_vector_path base_data/HN_Koppen4Class.gpkg --where_statement KC=%%c --scenario_id %scenario% --year_range %startyear% %endyear% --season_range 121 243 --variable_id tas --aggregate_function gt_309.15 --target_table_path cmip6_output/ --file_prefix HN_days_above_36c_may_aug_
REM )

REM For years: 2035-2065
REM x Total annual precipitation
REM Total May-August precipitation (Honduras, Maize)
REM
REM Mean annual temperature
REM Days per year above 30 C (Honduras, Coffee)
REM Days May-August above 36 C (Honduras, maize, grain filling)
REM Days Oct-Feb above 36ºC (Mozambique, maize)
REM I'm also attaching a vector of the climate regions for Mozambique.

