#!/usr/bin/env python3
# coding: utf-8
import os
import random
import warnings
import argparse

import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client


# Sampling and generating single csv file (per event_time)
def sample_and_write_csv(event_time, df_name, df_sname, df_geo, out_dir, scale_fcator=10):
    ntarget = int(random.randint(4000000, 4614159) * scale_fcator / 10)
    out_csv = os.path.join(out_dir, event_time + '.csv')

    # Calculate frac ratio
    frac_geo = ntarget / df_geo.shape[0].compute()
    frac_name = ntarget / df_name.shape[0].compute()
    frac_sname = ntarget / df_sname.shape[0].compute()

    # Sampling from Dataframe
    sample_geo = df_geo.sample(frac=frac_geo, replace=True).reset_index(drop=True)
    sample_name = df_name.sample(frac=frac_name, replace=True).reset_index(drop=True)
    sample_sname = df_sname.sample(frac=frac_sname, replace=True).reset_index(drop=True)

    # Concat, assign event_dtm & reorder columns
    result = dd.concat([sample_name, sample_sname, sample_geo], axis='columns')
    result = result.assign(event_time=event_time).set_index('event_time')
    result = result[['name', 'sur_name', 'sex', 'city', 'latitude', 'longitude', 'country']]

    # Write into one file
    result_fn = result.to_csv(out_csv, compute=True, single_file=True)

    # Free result values & return
    del result
    return result_fn


# Generate csv files
def run_helper(partition, df_name, df_sname, df_geo, out_dir, scale_factor=10):
    results = []

    for _, item in partition.iterrows():
        event_time = item[0]
        result = sample_and_write_csv(event_time, df_name, df_sname, df_geo, out_dir, scale_factor)
        results.append(result)

    return results


# Parse arguments
def arg_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument('--start', help='starting datetime in \'YYYYMMDDHHMMDD\' format \n(default: 201903011230)',
                        default='201903011230', type=str)
    parser.add_argument('--count', help='how many files to generate (default: 24)', default=24, type=int)
    parser.add_argument('--scale', help='how large files to generate (default: 5)', default=5, type=int)
    parser.add_argument('--worker', help='how many processes to process (default: 8)', default=8, type=int)
    parser.add_argument('--outdir', help='dir path to save generated files (default: ./loadData)',
                        default='./loadData', type=str)

    args = parser.parse_args()

    return args


# main
def main(client, args):
    # Parameters
    datetime_start = args.start
    file_count = args.count
    scale_factor = args.scale
    out_dir = args.outdir
    npartitions = 24

    # Make dir if not exists
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    # Sample files' path & name
    fn_geo = './sampleData/worldcities.csv'
    fn_name = './sampleData/baby-names.csv'
    fn_sname = './sampleData/surnames.csv'

    # Samplel files' data types
    meta_geo = {
        'city': 'object',
        'city_ascii': 'object',
        'lat': 'object',
        'lng': 'object',
        'city': 'object',
        'country': 'object',
        'iso2': 'object',
        'iso3': 'object',
        'capital': 'object',
        'population': 'float64',
        'id': 'int64'
    }

    meta_name = {
        'year': 'int64',
        'name': 'object',
        'percent': 'float64',
        'sex': 'object'
    }

    meta_sname = {
        'name': 'object',
        'rank': 'int64',
        'count': 'int64',
        'prop100k': 'float64',
        'cum_prop100k': 'float64',
        'pctwhite': 'object',
        'pctblack': 'object',
        'pctapi': 'object',
        'pctaian': 'object',
        'pct2prace': 'object',
        'pcthispanic': 'object'
    }

    meta_dtm = {
        'event_time': 'int64'
    }

    # Read file as Pandas DF & Select columns
    pdf_geo = pd.read_csv(fn_geo, dtype=meta_geo)[['city_ascii', 'lat', 'lng', 'country']]
    pdf_name = pd.read_csv(fn_name, dtype=meta_name)[['name', 'sex']]
    pdf_sname = pd.read_csv(fn_sname, dtype=meta_sname)[['name']]

    # Rename columns
    pdf_geo = pdf_geo.rename(columns={'city_ascii': 'city', 'lat': 'latitude', 'lng': 'longitude'})
    pdf_sname = pdf_sname.rename(columns={'name': 'sur_name'})

    # Convert Pandas DFs to Dask DFs
    df_geo = dd.from_pandas(pdf_geo, npartitions=1)
    df_name = dd.from_pandas(pdf_name, npartitions=1)
    df_sname = dd.from_pandas(pdf_sname, npartitions=1)

    # Distribute sample DFs
    # df_geo = await client.gather(client.scatter(df_geo, broadcast=True))
    # df_name = await client.gather(client.scatter(df_name, broadcast=True))
    # df_sname = await client.gather(client.scatter(df_sname, broadcast=True))
    df_geo = client.scatter(df_geo, broadcast=True)
    df_name = client.scatter(df_name, broadcast=True)
    df_sname = client.scatter(df_sname, broadcast=True)

    # Generate Datetime index & partition
    datetime_format = '%Y%m%d%H%M'
    list_dtm = pd.date_range(start=datetime_start, freq='5min', periods=file_count).strftime(datetime_format).to_list()
    pdf_dtm = pd.DataFrame({'event_time': list_dtm}, columns=['event_time'])
    df_dtm = dd.from_pandas(pdf_dtm, npartitions=npartitions).persist()

    # Sample & generate csv file per datetime
    out = df_dtm.map_partitions(run_helper, df_name, df_sname, df_geo, out_dir, scale_factor, meta=meta_dtm).map_partitions(len).compute()
    nfiles = out.sum()

    print("* {} files generated.".format(nfiles))


if __name__ == '__main__':
    # Parse arguments
    args = arg_parser()

    # Dask Client
    with Client(n_workers=args.worker, threads_per_worker=2, processes=True) as client, warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)
        main(client, args)
