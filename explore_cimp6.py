"""Script to explore the API to extract CIMP6 data.

Here are good references for table and variable names:

table for listing all the "variables" in cmip 6 https://docs.google.com/spreadsheets/d/1UUtoz6Ofyjlpx5LdqhKcwHFz2SGoTQV2_yekHyMfL9Y/edit#gid=1221485271
tier1 experiments: https://docs.google.com/spreadsheets/d/1SktYsKYhRxQFjUsGYbKxu6G4egpQePbQ1Y5bgERRg3M/edit#gid=1894225558
tier 2 experiments: https://docs.google.com/spreadsheets/d/1RyOMbaCLjF4ffEQ3VB4E7DTOwiZwNuF1vxHm-fQ26pw/edit#gid=197844184
tier 3 experiments: https://docs.google.com/spreadsheets/d/1N7U6_Hi4yvojOUlXS3bGgTExoX6R1d9DpqGMbg5zMms/edit#gid=2049219682
"""
import intake
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
