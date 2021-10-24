import os
import click
import shlex
import logging
import subprocess
import pandas as pd
from shutil import copyfile


def run_shell_command(cmd):
    subprocess.check_output(shlex.split(cmd))


def extract_date(date):
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    months = {m: str(n+1).zfill(2) for n, m in enumerate(months)}
    weekday, month, day, time, year, timezone = date.split(' ')
    return '-'.join([year, months[month], day.zfill(2)])


@click.command()
@click.option("--cases_by_dhb", default="cases_by_DHB.csv", help="Path to cases_by_DHB.csv")
@click.option("--output_folder", default="cases_by_DHB", help="Path to save cases_by_DHB for each date")
@click.option("--output_csv", default="cases_by_DHB_over_time.csv", help="Path to save cases_by_DHB_over_time.csv")
@click.option("--log_level", default="INFO", help="Log level (default: INFO)")
def main(cases_by_dhb, output_folder, output_csv, log_level):

    # Set logger config
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Extract git commit history
    ps = subprocess.Popen(
        shlex.split("git log --pretty=\"%H|%cd|%B\" cases_by_DHB.csv"),
        stdout=subprocess.PIPE
    )
    subprocess.call(
        shlex.split("grep -vE '^$'"),
        stdin=ps.stdout,
        stdout=open('cases_by_DHB.githistory', 'w')
    )

    cases_by_dhb_history = pd.read_table(
        'cases_by_DHB.githistory',
        sep='|',
        names=['commit', 'date', 'body']
    )

    cases_by_dhb_history['date'] = (cases_by_dhb_history
        .date
        .apply(extract_date)
    )

    cases_by_dhb_history = (cases_by_dhb_history
        .sort_values('date')
        .groupby('date')
        .last()
        .reset_index()
    )
    cases_by_dhb_history.to_csv('cases_by_DHB_history.csv', index=False)

    run_shell_command("mkdir -p cases_by_DHB")

    for i, row in cases_by_dhb_history.iterrows():

        # Check out the cases_by_DHB.csv file
        run_shell_command(f'git checkout {row.commit} cases_by_DHB.csv')

        # Copy the csv to the new location
        copyfile('cases_by_DHB.csv',
                 os.path.join('cases_by_DHB', row.date + '_cases_by_DHB.csv'))

    # Check out latest_cases_by_DHB.csv and clean up old files
    run_shell_command('git checkout cases_by_DHB.csv')
    run_shell_command('rm -rf cases_by_DHB.githistory cases_by_DHB_history.csv')


if __name__ == "__main__":
    main()
