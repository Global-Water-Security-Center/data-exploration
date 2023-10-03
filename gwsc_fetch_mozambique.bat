setlocal enabledelayedexpansion

for %%c in (120 221 321) do (

    call python cmip6_fetch_tool.py --aoi_vector_path base_data/moz_regions --where_statement CR=%%c --scenario_id ssp245 --year_range 2035 2065 --season_range 1 365 --variable_id pr --aggregate_function sum --eval_cmd 86400*var --target_table_path moz_cr_output/

    call python cmip6_fetch_tool.py --aoi_vector_path base_data/moz_regions --where_statement CR=%%c --scenario_id ssp245 --year_range 2035 2040 --season_range 1 365 --variable_id tas --aggregate_function mean --eval_cmd var-272.15 --target_table_path moz_cr_output/
)

REM For years: 2035-2065
REM Total annual precipitation
REM Total May-August precipitation (Honduras, Maize)
REM
REM Mean annual temperature
REM Days per year above 38 C (Honduras, Banana)
REM Days May-August above 36 C (Honduras, maize, grain filling)
REM Days Oct-Feb above 36ºC (Mozambique, maize)
REM I'm also attaching a vector of the climate regions for Mozambique.

