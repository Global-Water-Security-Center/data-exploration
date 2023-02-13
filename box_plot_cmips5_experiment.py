"""Experiment to:

@Richard Sharp
 OK - this is going to be way to complicated to deliver, but for us ot have a chance to take a look at the data and figure out how much we want to simplify it, I want:
For each area of interest (5 countries + 5 watersheds)
Total annual precipitation
box plot of each of the 10 years in the time period
For each time period
5 years from now = 2029-2038
21 separate box plots, one for each model
25 years from now = 2043-2053
21 separate box plots, one for each model
50 years from now = 2069-2078)
21 separate box plots, one for each model
Same for annual mean temperature
Eventually we’ll do the same for number of “big” rain events (edited)
"""
import glob
import logging
import sys

import numpy
import pandas
import matplotlib.pyplot as plt

logging.basicConfig(
    level=logging.WARNING,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


def main():
    """Entrypoint."""
    for table_path in glob.glob("CIMP5_hyd_bas_L3_HYBAS_ID_*.csv"):
        watershed_id = table_path.split('CIMP5_hyd_bas_L3_HYBAS_ID_')[1][:-4]
        LOGGER.debug(watershed_id)
        df = pandas.read_csv(table_path)

        # get just the year to group by year later
        df['year'] = df['date'].map(lambda x: x[:4])
        df['date'] = pandas.to_datetime(df["date"], format="%Y-%m-%d")

        # dump historical values
        # df = df[df.columns.drop(df.filter(regex='.*_historical'))]
        # turn temps into mean in C
        for column in df.columns:
            if column.startswith('tasmin'):
                base_model = '_'.join(column.split('_')[1:])
                min_col = f'tasmin_{base_model}'
                max_col = f'tasmax_{base_model}'
                mean_temp = (df[min_col]+df[max_col])/2
                df = df[df.columns.drop((df.filter(regex=f'tas*_{base_model}')))]
                df[f'tasmean_{base_model}'] = mean_temp-272.15  # convert from K to C

        LOGGER.debug(df.filter(regex=f'tasmean_*').columns)
        min_temp = df.filter(regex=f'tasmean_*').min().min()
        max_temp = df.filter(regex=f'tasmean_*').max().max()
        min_temp = float(numpy.floor(min_temp*10)/10)
        max_temp = float(numpy.ceil(max_temp*10)/10)

        # sum up the years and convert to mm
        max_precip = (df.groupby('year').sum() * 86400).filter(regex=f'pr_*').max().max()
        max_precip = float(numpy.ceil(max_precip*10)/10)

        date_ranges = [
            #((1950, 1999), ('rcp45', 'rcp85')),
            #((2029, 2038), ('rcp45', 'rcp85')),
            #((2043, 2054), ('rcp45', 'rcp85')),
            #((2069, 2078), ('rcp45', 'rcp85')),
            ((1991, 2005), ('historical',)),

            ]
        for (start, end), experiment_list in date_ranges:
            date_index = (
                (df['date'] >= f'{start}-01-01') &
                (df['date'] <= f'{end}-12-31'))
            date_df = df[date_index]

            fig, axes = plt.subplots(nrows=len(experiment_list), ncols=1, figsize=(10, 10))
            for index, experiment in enumerate(experiment_list):
                # do pr_, convert to mm 86400, add per year, plot
                pr_columns = [
                    col for col in df.columns if
                    col != 'date' and
                    experiment in col and
                    col.startswith('pr_')]

                # sum up the years and convert to mm
                pr_df = date_df[pr_columns+['year']].groupby('year').sum() * 86400
                col_rename = dict({old_name: old_name[3:] for old_name in pr_columns})
                LOGGER.debug(col_rename)
                pr_df = pr_df.rename(columns=col_rename)
                subplot_ax = None
                if len(experiment_list) > 1:
                    subplot_ax = axes[index]
                boxplot = pr_df.boxplot(
                    column=list(col_rename.values()),
                    vert=False,
                    ax=subplot_ax)
                boxplot.set_title(f"{experiment}")
                if index == 1:
                    boxplot.set_xlabel('precip (mm/year)')
                plt.setp(boxplot, xlim=(0, max_precip))
            fig.subplots_adjust(left=0.2, right=.98, bottom=0.05, top=0.93)
            fig.suptitle(f"Watershed ({watershed_id}) Annual precipitation {start}-{end}")
            plt.savefig(f'{watershed_id}_precip_{start}-{end}.png')
            # plt.show()

            fig, axes = plt.subplots(nrows=len(experiment_list), ncols=1, figsize=(10, 10))
            for index, experiment in enumerate(experiment_list):
                # do tasmean
                temp_columns = [
                    col for col in df.columns if
                    col != 'date' and
                    experiment in col and
                    col.startswith('tasmean_')]
                temp_df = date_df[temp_columns]
                col_rename = dict({old_name: old_name[8:] for old_name in temp_columns})
                LOGGER.debug(col_rename)
                temp_df = temp_df.rename(columns=col_rename)
                subplot_ax = None
                if len(experiment_list) > 1:
                    subplot_ax = axes[index]
                boxplot = temp_df.boxplot(column=list(col_rename.values()), vert=False, ax=subplot_ax)
                boxplot.set_title(f"{experiment}")
                if index == 1:
                    boxplot.set_xlabel('Daily Temp (C)')
                plt.setp(boxplot, xlim=(min_temp, max_temp))
            fig.subplots_adjust(left=0.2, right=.98, bottom=0.05, top=0.93)
            fig.suptitle(f"Watershed ({watershed_id}) mean daily temperature {start}-{end}")
            plt.savefig(f'{watershed_id}_mean_temp_{start}-{end}.png')
            #plt.show()
            #return


if __name__ == '__main__':
    main()
