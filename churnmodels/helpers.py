import os
import sys
import pandas as pd

# alternative to the interval functionality in postgres is to create a tempory table that can be joined to
from datetime import timedelta
def make_day_interval(d_start_date, d_end_date, periods, freq_str):
    # we let pandas do find the starting date which is
    # new-start-date = start-date - (periods * frequency)
    seq=pd.date_range(d_start_date, periods=periods+1, freq=f"-{freq_str}")
    new_start_date=seq[-1]
    end_dates=pd.date_range(d_start_date, d_end_date, freq=freq_str)
    start_dates=pd.date_range(new_start_date, periods=len(end_dates), freq=freq_str)
    df=pd.DataFrame({"start_date":start_dates,"end_date":end_dates})
    df.index.rename("id")
    return df



def required_envvar(envvar, errtext):
    """
    return the environment variable envvar.
    If not given print error text and exit.
    :param envvar:
    :type envvar:
    :param errtext:
    :type errtext:
    :return:
    :rtype:
    """
    if envvar not in os.environ:
        print(errtext)
        exit()
    return os.getenv(envvar)


def progressBar_old(iterable, prefix='', suffix='', decimals=1, length=100, fill='█', printEnd="\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    total = len(iterable)

    # Progress Bar Printing Function
    def printProgressBar(iteration):
        percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + '-' * (length - filledLength)
        print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=printEnd)

    # Initial Call
    printProgressBar(0)
    # Update Progress Bar
    for i, item in enumerate(iterable):
        yield item
        printProgressBar(i + 1)
    # Print New Line on Complete
    print()


def progressBar(iteration, total, prefix='', suffix='', decimals=1, bar_length=100, fill='█', head=">", printEnd="\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        bar_length  - Optional  : character length of bar (Int)
    """
    str_format = "{0:." + str(decimals) + "f}"
    percents = str_format.format(100 * (iteration / float(total)))
    filled_length = int(round(bar_length * iteration / float(total)))
    if iteration == total:
        head = fill
    bar = fill * filled_length + head + '-' * (bar_length - filled_length)

    sys.stdout.write('%s%s |%s| %s%s %s' % (printEnd, prefix, bar, percents, '%', suffix)),

    if iteration == total:
        sys.stdout.write('\n')
    sys.stdout.flush()
