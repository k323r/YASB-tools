#!/bin/python3

VERSION=0.1

import sys
sys.path.insert(0, "../yasb")

from LIDAR import *

from glob import glob
from math import sqrt, log

from multiprocessing import Pool

import argparse
from os import path
from zipfile import ZipFile

inputfiles = list()

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument('-if', '--input-files', nargs='+', help='input: single file or list of files')
    parser.add_argument("-v", "--verbose", help="turn on detailed output", action="store_true")
    parser.add_argument("-o", "--output-dir", help="name of output directory", type=str)
    parser.add_argument("-og", "--output-global", help="used to export the global data frame", type=str)
    parser.add_argument("-i", "--input-dir", help="name of input directory", type=str)
    parser.add_argument("-j", "--procs", help="number of processor to use", type=int, default=8)
    parser.add_argument("-p", "--lidar-pattern", help="glob pattern to select the files containing lidar data. If not provided, genLIDARPickle defaults to *.csv", type=str, default="*.csv")

    # parse arguments
    args = parser.parse_args()
    
    if args.verbose: print("* verbose: on")
    if args.verbose: print("* genLIDARPickle.py v{}".format(VERSION))

    """
    if not args.input_dir and not args.input_files:
        print("* setting input directory to cwd")
        args.input_dir=path.curdir
    """

    if not args.output_dir:
        raise Exception("*! please provide an output directory name")

    if not path.isdir(args.output_dir):
        raise Exception("*! not a directory: {}".format(args.output_dir))
    
    if not args.input_dir and not args.input_files:
        raise Exception("*! Please provide a valid input directory or a list of files to process")

    if args.input_files:
        for f in args.input_files:
            if path.isfile(f):
                inputfiles.append(f)
    else:
        inputfiles = glob(path.join(args.input_dir, args.lidar_pattern))
        inputfiles = [f for f in inputfiles if path.isfile(f)]

    ### main logic
    
    pool = Pool(args.procs)
    frames = list()
    days = dict()

    keys = generateKeys()


    for lidarFile in inputfiles:
        frames.append(
                pool.apply_async(
                    processLIDARFile, (lidarFile, keys, args.verbose)))

    for lidardict in [d.get() for d in frames]:
        days.update(lidardict)
   
    # ckeck for empty dataframes:
    for day in days:
        if days[day].empty:
            if args.verbose:
                print('* found empty data frame: {}, deleting'.format(day))
            del(days[day])

    frames = list() 

    for day in sorted(days):
        frames.append(
                pool.apply_async(
                    cleanLIDARData, (days[day], args.verbose)))

    frames = pd.concat([d.get() for d in frames])

    for i in range(0, 11):
        key = "dir_{}_corr".format(i)
        if args.verbose: print("*       correcting for vessel heading for each lidar level {}".format(i))
        ### list comprehension faster? -> yes
        frames[key] = [correctHeading(x, y) for x, y in zip(frames[key], frames["heading"])]

    # delete duplicate indices
    if args.verbose: print('* deleting duplicated indices')
    frames = frames.loc[~frames.index.duplicated(keep='first')]

    # resample to 1 s for datetime selection
    if args.verbose: print('* resampling to 1 s')
    frames = frames.asfreq('1s', fill_value=np.nan)

    # drop nans
    if args.verbose: print('* dropping NaNs')
    frames.dropna()

    if args.verbose: print("* saving pickles")
    for day, data in frames.groupby(pd.Grouper(freq='D')):
        if not len(data) > 600:         # 10 min
            if args.verbose: print('* skipping day: {}'.format(day))
            continue
        datestring = '{:04d}-{:02d}-{:02d}'.format(day.year, day.month, day.day)
        exportPickle = path.join(args.output_dir, "{}_LIDAR.pickle".format(datestring))
        if args.verbose: print("*   exporting {}".format(exportPickle))
        try:
            data.to_pickle(exportPickle)
        except:
            print("*! failed to export data as pickles")
    if args.output_global:
        if args.verbose: print('* saving global pickle')
        try:
            frames.to_pickle(args.output_global)
        except Exception as e:
            print('*! failed to export global frame {}'.format(e))

