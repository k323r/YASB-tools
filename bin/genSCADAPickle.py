#!/bin/python3

VERSION=0.1

import sys
sys.path.insert(0, "../yasb")

from bikbox import *
from LIDAR import *

from glob import glob
from math import sqrt, log

import argparse
from os import path
from zipfile import ZipFile

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("-v", "--verbose", help="turn on detailed output", action="store_true")
    parser.add_argument("-i", "--input", help="input directory containing TOMBox log files", type=str)
    parser.add_argument("-o", "--output", help="name of output pickle file", type=str)
    parser.add_argument("-j", "--procs", help="number of processors to use", type=int)
    parser.add_argument("-m", "--substract-mean", help="substract mean values from acceleration", action="store_true")


    # parse arguments
    args = parser.parse_args()
    
    if args.verbose: print("* verbose: on")
    if args.verbose: print("* TOMTool v{}".format(VERSION))

    if not args.input:
        if args.verbose: print("* setting input directory to cwd")
        args.input=path.curdir

    if not args.output:
        raise Exception("please provide an output pickle name")
    
    if not args.procs:
        args.procs=4

    if not path.isdir(args.input):
        raise Exception("Please provide a valid input directory")

    if args.verbose: print("* calling parallel processing function, using {} processors".format(args.procs))
    data = processDataSet_parallel(
            args.input,
            args.output,
            nProcs=args.procs,
            verbose=args.verbose,
            substractMean=args.substract_mean,
            )

