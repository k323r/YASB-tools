#!/bin/python3

### TODO


VERSION=0.1

import argparse
from os.path import isfile
import pandas as pd

if __name__ == "__main__":


    parser = argparse.ArgumentParser()
    
    parser.add_argument("-v", "--verbose", help="turn on detailed output", action="store_true")
    parser.add_argument("-i", "--input", help="path to MSR log file", type=str)
    parser.add_argument("-o", "--output", help="name of output pickle file", type=str)
    parser.add_argument("-m", "--substract-mean", help="substract mean values from acceleration", action="store_true")
    parser.add_argument("-t", "--time-zone", help="time zone of time series", type=str, default='Europe/Berlin')


    # parse arguments
    args = parser.parse_args()
    
    if args.verbose: print("* verbose: on")
    if args.verbose: print("* TOMTool v{}".format(VERSION))

    if not isfile(args.input):
        raise Exception('please provide an input file')

    if not args.output:
        raise Exception("please provide an output file name for data export")
    
    if args.verbose: print('* input file: {}\noutput file: {}'.format(args.input, args.output))

    data = pd.read_csv(args.input,
                       skiprows=43,
                       delimiter=';',
                       header=0,
                       names=('acc_x', 'acc_y', 'acc_z', 'bat')
                      )

    if args.verbose: print('* removing bat column')
    data.drop(columns=['bat',], inplace=True)

    data.index = pd.to_datetime(data.index)

    try:
        data.index = data.index.tz_localize(args.time_zone)
    except Exception as e:
        print('* failed to localize data: {}'.format(e))

    # remove duplicate indices
    if args.verbose:
        print('* removing duplicate indices')
    data = data.loc[~data.index.duplicated(keep='first')]

    if data.isnull().any().any():
        if args.verbose: print('* removing nans')
        data.dropna(inplace=True)

    if args.substract_mean:
        if args.verbose: print('* remove means')
        for comp in ['acc_x', 'acc_y', 'acc_z']:
            data[comp] = data[comp] - data[comp].mean()

    if isfile(args.output):
        overwrite = input("*! file already exists, overwrite? [N/y]")
        if not overwrite.lower() == 'y':
            print ('* exit, bye')

    try:
        if args.verbose: print('* exporting pickle to: {}'.format(args.output))
        data.to_pickle(args.output)
    except Exception as e:
        print("*! failed to export pickle!")
        print("*! -> {}".format(e))

