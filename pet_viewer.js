// These dates are hard-coded as known from running a script on Rich's machine that created the data in the first place
var start_date = '2018-01-01';
var end_date = '2018-01-31';
var year = '2018';
var day_of_year = 1;
// Solar constant [ MJ m-2 min-1]
var SOLAR_CONSTANT = 0.0820
var era5_month_collection = ee.ImageCollection("ECMWF/ERA5/MONTHLY").filterDate(
      start_date, end_date);

var era5_year_collection = ee.ImageCollection("ECMWF/ERA5/MONTHLY").filterDate(
      year+'01-01', year+'12-31');

# TODO: get the other climate index in here calculating

# so what

function


function hargreaves() {
    /*
    Estimate reference evapotranspiration over grass (ETo) using the Hargreaves
    equation.
    Generally, when solar radiation data, relative humidity data
    and/or wind speed data are missing, it is better to estimate them using
    the functions available in this module, and then calculate ETo
    the FAO Penman-Monteith equation. However, as an alternative, ETo can be
    estimated using the Hargreaves ETo equation.
    Based on equation 52 in Allen et al (1998).
    :param tmin: Minimum daily temperature [deg C]
    :param tmax: Maximum daily temperature [deg C]
    :param tmean: Mean daily temperature [deg C]. If emasurements not
        available it can be estimated as (*tmin* + *tmax*) / 2.
    :param et_rad: Extraterrestrial radiation (Ra) [MJ m-2 day-1]. Can be
        estimated using ``et_rad()``.
    :return: Reference evapotranspiration over grass (ETo) [mm day-1]
    :rtype: float*/


    // Note, multiplied by 0.408 to convert extraterrestrial radiation could
    // be given in MJ m-2 day-1 rather than as equivalent evaporation in
    // mm day-1


    var latitude = ee.Image.pixelLonLat().select('latitude').multiply(Math.PI/180);

    var sol_dec = ee.Image.constant(
      0.409 * Math.sin(((2.0 * Math.PI / 365.0) * day_of_year - 1.39)));

    //cos_sha = -np.tan(latitude) * np.tan(sol_dec)
    var cos_sha = latitude.tan().multiply(-1).multiply(sol_dec.tan());

    // sha = np.arccos(np.clip(cos_sha,-1,1))
    var sha = ee.Image(cos_sha.clamp(-1, 1).acos());
    //Map.addLayer(sha);
    //console.log(sol_dec);

    var tmp1 = ee.Image.constant((24.0 * 60.0) / Math.PI);
    //tmp2 = sha * np.sin(latitude) * np.sin(sol_dec)
    var tmp2 = sha.multiply(latitude.sin()).multiply(sol_dec.sin());
    //tmp3 = np.cos(latitude) * np.cos(sol_dec) * np.sin(sha)
    var tmp3 = latitude.cos().multiply(sol_dec.cos()).multiply(sha.sin());

    //ird = 1 + (0.033 * np.cos((2.0 * np.pi / 365.0) * day_of_year))
    var ird = ee.Image.constant(
      1 + (0.033 * Math.cos((2.0 * Math.PI / 365.0) * day_of_year)));

    //et_rad = tmp1 * SOLAR_CONSTANT * ird * (tmp2 + tmp3)
    var et_rad = tmp1.multiply(SOLAR_CONSTANT).multiply(ird).multiply(tmp2.add(tmp3));

    var tmean = era5_month_collection.select('mean_2m_air_temperature').toBands().subtract(273.15);
    var tmax = era5_month_collection.select('maximum_2m_air_temperature').toBands().subtract(273.15);
    var tmin = era5_month_collection.select('minimum_2m_air_temperature').toBands().subtract(273.15);

    var landcover = ee.Image("ESA/GLOBCOVER_L4_200901_200912_V2_3").select('landcover').rename('B0').eq(210).not();
    return ee.Image.constant(0.0023).multiply(
      tmean.add(17.8)).multiply(
      tmax.subtract(tmin)).pow(0.5).multiply(0.408).multiply(et_rad).rename('B0').mask(landcover);
}

function thornthwaite() {

    //*Estimate monthly potential evapotranspiration (PET) using the
    //Thornthwaite (1948) method.
    //Thornthwaite equation:
    //    *PET* = 1.6 (*L*/12) (*N*/30) (10*Ta* / *I*)***a*
    //where:
    //* *Ta* is the mean daily air temperature [deg C, if negative use 0] of the
    //  month being calculated
    //* *N* is the number of days in the month being calculated
    //* *L* is the mean day length [hours] of the month being calculated
    //* *a* = (6.75 x 10-7)*I***3 - (7.71 x 10-5)*I***2 + (1.792 x 10-2)*I* + 0.49239
    //* *I* is a heat index which depends on the 12 monthly mean temperatures and
    //  is calculated as the sum of (*Tai* / 5)**1.514 for each month, where
    //  Tai is the air temperature for each month in the year
    //:param monthly_t: Iterable containing mean daily air temperature for each
    //    month of the year [deg C].
    //:param monthly_mean_dlh: Iterable containing mean daily daylight
    //    hours for each month of the year (hours]. These can be calculated
    //    using ``monthly_mean_daylight_hours()``.
    //:param year: Year for which PET is required. The only effect of year is
    //    to change the number of days in February to 29 if it is a leap year.
    //    If it is left as the default (None), then the year is assumed not to
    //    be a leap year.
    //:return: Estimated monthly potential evaporation of each month of the year
    //    [mm/month]
    //:rtype: List of floats*/

    //# Negative temperatures should be set to zero
    //adj_monthly_t = [t * (t >= 0) for t in monthly_t]
    var adj_monthly_t = era5_year_collection.select('mean_2m_air_temperature').toBands().subtract(273.15).clamp(0, 100);

    //# Calculate the heat index (I)
    //for Tai in adj_monthly_t:
    //    if Tai / 5.0 > 0.0:
    //        I += (Tai / 5.0) ** 1.514
    var I = adj_monthly_t.divide(5.0).pow(1.514).where(adj_monthly_t.lte(0.0), 0.0).reduce('sum');


    //a = (6.75e-07 * I ** 3) - (7.71e-05 * I ** 2) + (1.792e-02 * I) + 0.49239
    var a = (ee.Image.constant(6.75e-07).multiply(I.pow(3))).subtract(ee.Image.constant(7.71e-05).multiply(I.pow(2))).add(ee.Image.constant(1.792e-02).multiply(I)).add(ee.Image.constant(0.49239));
    console.log(a);
    return a.rename('B0');

    //pet = []
    //for Ta, L, N in zip(adj_monthly_t, monthly_mean_dlh, month_days):
        //# Multiply by 10 to convert cm/month --> mm/month
        //pet.append(
            //1.6 * (L / 12.0) * (N / 30.0) * ((10.0 * Ta / I) ** a) * 10.0)

    var pet = ee.Image.constant(1.6)*

    //return pet
}
