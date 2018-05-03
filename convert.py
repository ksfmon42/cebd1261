#!/usr/bin/env python3
import sys
import os
import numpy as np
import pandas as pd
import argparse

# Parse all positional arguments: input file names and optional named argument: subdirectory
parser=argparse.ArgumentParser()
parser.add_argument('input_files', metavar='filename', type=str, nargs='+', help='one or more input file names')
parser.add_argument('--input_dir', help='Input subdirectory, default to current')
args=parser.parse_args()
input_files = args.input_files
input_dir = args.input_dir

# Prepend subdirectory, if any, to file names
if input_dir:
    input_files = list(map(lambda f: input_dir + '/' + f, input_files))

outfile = open('final.csv', 'w')
outfile.write('City, Province, Date, Hour, Average Wait\n')

with open(input_files[0]) as infile:
    for line in infile:
        locationDateTime, avgWaitThatHour = line.split('\t')
        avgWaitThatHour = avgWaitThatHour.strip()
        location, dateTimeHour = locationDateTime.split(';')
        dateTimeHour=dateTimeHour.strip()
        city, province = location.split(':')
        day, hour = dateTimeHour.split('_')
#        print('%s,%s,%s,%s:00,%s' % (city, province, day, hour, avgWaitThatHour))
        outfile.write('%s,%s,%s,%s:00,%s\n' % (city, province, day, hour, avgWaitThatHour))

infile.close()
outfile.close()

