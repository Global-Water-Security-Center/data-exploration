import intake
import xarray as xr
import proplot as plot
import matplotlib.pyplot as plt


def main():
    """Entry point."""
    # necessary url
    url = "https://raw.githubusercontent.com/NCAR/intake-esm-datastore/master/catalogs/pangeo-cmip6.json"
    # open the catalog
    dataframe = intake.open_esm_datastore(url)
    print(dataframe.df.columns)
    models = dataframe.search(
        experiment_id='historical',
        table_id='day',
        variable_id='pr')
    print(models)
    datasets = models.to_dataset_dict()
    print(datasets)
    print(datasets.keys())
    dset = datasets['CMIP.NCAR.CESM2.historical.day.gn']

    fig, ax = plot.subplots(
        axwidth=4.5, tight=True, proj='robin', proj_kw={'lon_0': 180},)
    # format options
    ax.format(land=False, coast=True, innerborders=True, borders=True,
              labels=True, geogridlinewidth=0,)
    map1 = ax.contourf(dset['lon'], dset['lat'], dset['pr'][0,0,:,:],
                       cmap='IceFire', extend='both')
    ax.colorbar(map1, loc='b', shrink=0.5, extendrect=True)
    plt.show()


if __name__ == '__main__':
    main()
