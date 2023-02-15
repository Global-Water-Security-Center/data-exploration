"""Experiment to:

Get the historical 95th percentile of rain in one day as a threshold to
    establish what a heavy rain event looks like and report the number of
    heavy rain days per year in the historical time period (can be averaged
    over the time period if that’s useful)
Using that same heavy rain event threshold, report the number of heavy rain
    days per year for the three future time steps (could also be averaged over
    each time step if that's useful)

user analysis:

Are there many more heavy rain days in future time steps than there were in
    the historical time period?
"""
import collections
import glob
import logging
import sys

import pandas
from pandas.plotting import table
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
    watershed_id_results = collections.defaultdict(dict)
    for table_path in glob.glob("CIMP5_hyd_bas_L3_HYBAS_ID_*.csv"):
        watershed_id = table_path.split('CIMP5_hyd_bas_L3_HYBAS_ID_')[1][:-4]
        LOGGER.debug(watershed_id)
        df = pandas.read_csv(table_path)

        # get just the year to group by year later
        df['year'] = df['date'].map(lambda x: x[:4])
        df['date'] = pandas.to_datetime(df["date"], format="%Y-%m-%d")

        # sum up the years and convert to mm
        # max_precip = (df.groupby('year').sum() * 86400).filter(regex=f'pr_*').max().max()
        # max_precip = float(numpy.ceil(max_precip*10)/10)

        his_start = 1951
        his_end = 2005
        date_index = (
            (df['date'] >= f'{his_start}-01-01') &
            (df['date'] <= f'{his_end}-12-31'))
        date_df = df[date_index]

        historical_pr_columns = [
            col for col in df.columns if
            col != 'date' and
            'historical' in col and
            col.startswith('pr_')]
        LOGGER.debug(historical_pr_columns)

        # convert to mm
        pr_df = date_df[historical_pr_columns] * 86400
        col_rename = dict({old_name: old_name[3:] for old_name in historical_pr_columns})
        pr_df = pr_df.rename(columns=col_rename)
        subplot_ax = None
        fig, axes = plt.subplots(nrows=len(pr_df.columns), ncols=1, figsize=(10, 100))
        rain_threshold_val = {}
        historical_count = {}

        for index, column_id in enumerate(pr_df.columns):
            subplot_ax = axes[index]
            model_df = pr_df[column_id]
            model_df = model_df[model_df > 0]
            histplot = model_df.plot.hist(
                bins=100,
                ax=subplot_ax)

            percentiles = [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
            quant = model_df.quantile(q=percentiles)
            rain_threshold_val[column_id.split('_historical')[0]] = quant[.95]

            table(subplot_ax, round(model_df.describe(percentiles=percentiles[:-1]), 3), loc="upper right", colWidths=[0.2, 0.2, 0.2])
            subplot_ax.set_xticks(quant[percentiles[:-1]])
            histplot.set_title(f"{column_id} historical precip daily")
            histplot.set_xlabel('precip (mm/day)')
            histplot.set_ylabel('number of days in bin')
            plt.setp(histplot, xlim=(0, quant[.99]))
            # count how many historical days >= .95 percentile
            historical_count[column_id] = (model_df >= quant[.95]).values.sum()
            fig.subplots_adjust(left=0.2, right=.98, bottom=0.01, top=0.97)
            fig.suptitle(f"Watershed ({watershed_id}) daily precipitation {his_start}-{his_end}")
        plt.savefig(f'{watershed_id}_precip_{his_start}-{his_end}.png')

        pr_columns = [
            col for col in df.columns if
            col != 'date' and
            'historical' not in col and
            col.startswith('pr_')]

        threshold_table = open(f'CMIP_95th_percentile_days_{watershed_id}.csv', 'w')
        threshold_table.write('date,'+','.join(pr_columns)+'\n')

        date_ranges = [
            ((2029, 2038), ('rcp45', 'rcp85')),
            ((2043, 2054), ('rcp45', 'rcp85')),
            ((2069, 2078), ('rcp45', 'rcp85')),
            ]
        for (start, end), experiment_list in date_ranges:
            date_index = (
                (df['date'] >= f'{start}-01-01') &
                (df['date'] <= f'{end}-12-31'))
            date_df = df[date_index]
            # convert to mm
            pr_df = date_df[pr_columns] * 86400

            threshold_table.write(f'{start}-{end}')

            for column_id in pr_columns:
                column_df = pr_df[column_id]
                LOGGER.debug(column_id)
                LOGGER.debug(rain_threshold_val)
                clean_col_id = column_id[:-6][3:]
                LOGGER.debug(clean_col_id)
                LOGGER.debug(column_df)
                threshold_df = column_df[column_df >= rain_threshold_val[clean_col_id]]
                LOGGER.debug(threshold_df.count())
                threshold_table.write(f',{threshold_df.count()}')
            threshold_table.write('\n')

        # also do historical
        threshold_table.write(
            f'\nhistorical date,' + ','.join(historical_count) + '\n' + f'{his_start}-{his_end}')
        for column_id in historical_count:
            threshold_table.write(f',{historical_count[column_id]}')
        threshold_table.write('\n')
        threshold_table.close()


if __name__ == '__main__':
    main()
