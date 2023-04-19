"""Shared functions across the data-exploration library."""
import calendar
import datetime
import os


def build_monthly_ranges(start_date, end_date):
    """Create a list of time range tuples that span months over the range.

    Args:
        start_date: (str) start date in the form YYYY-MM-DD
        end_date: (str) end date in the form YYYY-MM-DD

    Returns:
        list of start/end date tuples inclusive that span the time range
        defined by `start_date`-`end_date`
    """
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')

    current_day = start_date
    date_range_list = []
    while current_day <= end_date:
        current_year = current_day.year
        current_month = current_day.month
        last_day = datetime.datetime(
            year=current_year, month=current_month,
            day=calendar.monthrange(current_year, current_month)[1])
        last_day = min(last_day, end_date)
        date_range_list.append(
            (current_day.strftime('%Y-%m-%d'),
             last_day.strftime('%Y-%m-%d')))
        # this kicks it to next month
        current_day = last_day+datetime.timedelta(days=1)
    return date_range_list


def daterange(start_date, end_date):
    """Generator produces all ``datetimes`` between start and end."""
    if start_date == end_date:
        yield start_date
        return
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


def file_basename(path):
    """Return base file path, or last directory."""
    basename = os.path.basename(os.path.splitext(path)[0])
    if basename == '':
        # do last directory
        basename = os.path.normpath(path).split(os.sep)[-1]
    return basename