"""
Voice feature extraction for mPower using openSMILE
http://www.audeering.com/research/opensmile

Example:

   nohup python voice_feature_extraction.py process-rows \
        --limit 10000 --offset 50000 \
        --completed voice_gemaps.features.csv voice_countdown_gemaps.features.csv \
        --append > voice_feature_extraction.log 2>&1 &

Author: Christopher Bare
Adapted from mhealthx written by Arno Klein: https://github.com/Sage-Bionetworks/mhealthx
"""
import synapseclient
from synapseclient import Activity
from synapseclient import Entity, Project, Folder, File
from synapseclient import Evaluation, Submission, SubmissionStatus
from synapseclient import Schema, Column, Table, Row, RowSet
import synapseclient.utils as utils

import argparse
import csv
import json
import math
import os
import pandas as pd
import re
import subprocess
import sys

from collections import OrderedDict
from datetime import datetime

##----------------------------------------------------------
## defaults
##----------------------------------------------------------

## project: mPower - voice processing
MPOWER_VOICE_PROJECT = 'syn4931517'

## voice table from Bridge
## level1 - syn4590865
## public - syn5511444
VOICE_TABLE = 'syn4590865'
DATA_COLUMNS = ['audio_audio.m4a', 'audio_countdown.m4a']

OUT_FILE = 'voice_features.csv'

OPENSMILE_DIR = os.path.expanduser("~/install/openSMILE-2.2rc1")
OPENSMILE_CONF = os.path.join('config', 'gemaps', 'GeMAPSv01a.conf')

## which columns from the source table to we want to carry forward into
## the derived tables (feature table(s) and possibly a table of wav files)
KEEP_COLUMNS = ['recordId', 'healthCode', 'createdOn', 'appVersion', 'phoneInfo', 'calculatedMeds']
META_COLS = ['ROW_ID', 'ROW_VERSION'] + KEEP_COLUMNS

## remove intermediate files after processing
CLEANUP = False


def verify_dependencies(args):
    """
    Make sure we can find external dependencies, the executables
    ffmpeg and openSMILE
    """
    if not os.path.exists(args.opensmile_home):
        raise Exception("Can't find openSMILE home {0}".format(args.opensmile_home))
    if not os.path.exists(args.opensmile_conf):
        raise Exception("Can't find openSMILE config {0}".format(args.opensmile_conf))

    try:
        command = "ffmpeg -version"
        output = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError:
        raise Exception("Can't find ffmpeg executable")
    else:
        m = re.search('ffmpeg version (.*) Copyright', output, re.MULTILINE)
        if m:
            ffmpeg_version = m.group(1)
            print 'ffmpeg version ', ffmpeg_version

    try:
        command = "SMILExtract -h"
        output = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError:
        raise Exception("Can't find SMILExtract executable")
    else:
        m = re.search('openSMILE version (.*)', output, re.MULTILINE)
        if m:
            opensmile_version = m.group(1)
            print 'openSMILE version ', opensmile_version


def double_quote(strings):
    """
    Generate quotes strings from input strings.
    """
    for s in strings:
        yield '"' + s + '"'


def process_rows(args, syn):
    """
    Perform audio feature extraction for rows in a Synapse table
    containing file references. For each .m4a audio file:
    1. convert to .wav with ffmpeg
    2. extract features using openSMILE
    3. accumulate metadata and features in an output .csv file
    """
    ##----------------------------------------------------------
    ## Query source table with limit and offset
    ##----------------------------------------------------------
    query = "SELECT {cols} FROM {table_id}".format(
                cols=','.join(double_quote(KEEP_COLUMNS+args.data_columns)),
                table_id=args.voice_table_id)
    if args.limit:
        query += " LIMIT {0}".format(args.limit)
    if args.offset:
        query += " OFFSET {0}".format(args.offset)
    results = syn.tableQuery(query)

    ##----------------------------------------------------------
    ## load results as a DataFrame, but specify string dtype for FileHandleId
    ## columns so Pandas doesn't infer their type as int64 or float (with nans
    ## for missing values).
    ##----------------------------------------------------------
    df = pd.read_csv(results.filepath, dtype={col:'string' for col in args.data_columns})
    df.index = ["%s_%s"%(id, version) for id, version in zip(df["ROW_ID"], df["ROW_VERSION"])]
    del df["ROW_ID"]
    del df["ROW_VERSION"]

    ##----------------------------------------------------------
    ## don't redo rows for which all data columns have
    ## already been processed
    ##----------------------------------------------------------
    completed_dfs = {}
    if args.completed:
        completed_rows = pd.Series(True, index=df.index)
        for i,column in enumerate(args.data_columns):
            ## read .csv
            completed_dfs[column] = pd.read_csv(args.completed[i], dtype={col:'string' for col in args.data_columns})
            ## fix calculatedMeds column name
            completed_dfs[column] = completed_dfs[column].rename(columns={'medTimepoint':'calculatedMeds'})
            ## reorder metadata columns to match query results,
            ## assuming completed dfs have metadata columns similar to:
            ## recordId, createdOn, appVersion, medTimepoint, ROW_VERSION,
            ## healthCode, phoneInfo, ROW_ID, audio_countdown.m4a, audio_audio.m4a,
            ## and cols 10:72 are the 62 GeMAPS features computed by openSMILE
            ## starting with 'F0semitoneFrom27.5Hz_sma3nz_amean'
            column_index = df.columns.append(completed_dfs[column].columns[10:72])
            completed_dfs[column] = completed_dfs[column][column_index]
            ## track rows with all data columns completed
            completed_rows = completed_rows & df.recordId.isin(completed_dfs[column].recordId)
    else:
        completed_rows = pd.Series(False, index=df.index)
    df_to_download = df.ix[~completed_rows,:]

    ##----------------------------------------------------------
    ## Bulk download audio data in .m4a format
    ##----------------------------------------------------------
    file_map = syn.downloadTableColumns(Table(results.tableId, df_to_download), args.data_columns)

    ##----------------------------------------------------------
    ## unix time stamps -> nicely formated dates
    ##----------------------------------------------------------
    df.createdOn = df.createdOn.apply(utils.from_unix_epoch_time)

    ##----------------------------------------------------------
    ## process audio files
    ##----------------------------------------------------------
    for i in range(df.shape[0]):
        row = df.iloc[[i],:]
        print "processing:", i, row['recordId'].values[0]

        for column, out_file in zip(args.data_columns, args.out_files):

            ## check if we've already processed this record
            if completed_rows[i]:
                out_row = completed_dfs[column].ix[completed_dfs[column].recordId==df.recordId[i],:]
                print "already computed!"
            else:

                file_handle_id = df.ix[i,column]

                ## Pandas represents missing values as nan
                if isinstance(file_handle_id, float):
                    if math.isnan(file_handle_id):
                        continue

                try:
                    filepath = file_map[file_handle_id]
                except KeyError as ex2:
                    print 'No file path for file handle id "%s".' % file_handle_id
                    continue

                try:
                    ##----------------------------------------------------------
                    ## convert to wav
                    ##----------------------------------------------------------
                    basename, ext = os.path.splitext(os.path.basename(filepath))
                    wave_file = basename+".wav"
                    if os.path.exists(wave_file):
                        os.remove(wave_file)
                    command = "ffmpeg -i {infile} -ac 2 {outfile}".format(infile=filepath, outfile=wave_file)
                    output = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)

                    ##----------------------------------------------------------
                    ## extract features with openSMILE
                    ## example: SMILExtract -I output.wav -C ./openSMILE-2.1.0/config/gemaps/GeMAPSv01a.conf --csvoutput features.csv
                    ##----------------------------------------------------------
                    features_file = basename+".csv"
                    if os.path.exists(features_file):
                        os.remove(features_file)
                    command = "SMILExtract -I {input_file} -C {conf_file} --csvoutput {output_file}".format(
                                    input_file=wave_file,
                                    conf_file=args.opensmile_conf,
                                    output_file=features_file)
                    output = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)

                    ##----------------------------------------------------------
                    ## merge metadata and features
                    ##----------------------------------------------------------
                    features = pd.read_csv(features_file, sep=';', index_col=None)
                    ## get rid of useless column
                    features.drop('name', axis=1, inplace=True)
                    ## force the indexes to be equal so they will concat into 1 row. WTF, pandas?
                    features.index = row.index
                    out_row = pd.concat((row, features), axis=1)

                except Exception as ex1:
                    try:
                        sys.stderr.write("~~>Exception while processing record:{record}\n".format(
                            record=row['recordId']))
                    except Exception as ex2:
                        sys.stderr.write("~~~>Exception while processing record.\n")

                finally:
                    if args.cleanup:
                        ## opensmile want to output an .arff file whether you ask it to or not. Worse
                        ## yet, it appends, so it keeps growing. Let's clean it up.
                        opensmile_arff_file = "output.arff"
                        for v in ['wave_file', 'features_file', 'opensmile_arff_file']:
                            try:
                                if v in locals() and os.path.exists(locals()[v]):
                                    os.remove(locals()[v])
                            except Exception as ex:
                                sys.stderr.write('Error cleaning up temp files: ')
                                sys.stderr.write(str(ex))
                                sys.stderr.write('\n')

            ## append row to output .csv file
            append = (args.append or i>0)
            with open(out_file, 'a' if append else 'w') as f:
                    out_row.to_csv(f, header=(not append), index=False, quoting=csv.QUOTE_NONNUMERIC)

    print "processing rows complete!"


def main():
    parser = argparse.ArgumentParser(description='Extract audio features from mPower voice data files.')
    parser.add_argument('--version',  action='version',
            version='Synapse Client %s' % synapseclient.__version__)
    parser.add_argument('-u', '--username',  dest='synapseUser',
            help='Username used to connect to Synapse')
    parser.add_argument('-p', '--password', dest='synapsePassword',
            help='Password used to connect to Synapse')

    subparsers = parser.add_subparsers(title='commands')

    subparser = subparsers.add_parser('process-rows')
    subparser.add_argument('--voice-table-id', metavar='SYNAPSE_ID', type=str, default=VOICE_TABLE,
            help='The Synapse ID of the voice data source table. Defaults to {0}'.format(VOICE_TABLE))
    subparser.add_argument('-d', '--data-columns', metavar='COLUMNS', type=str, nargs='+', default=DATA_COLUMNS,
            help='Synapse table column names holding file handles to audio data.')
    subparser.add_argument('--out-files', '--outfiles', metavar='PATH', type=str, nargs='+', default=[],
            help='File paths to which to write output - one per column. Defaults to column name + ".features.csv".')
    subparser.add_argument('--completed', metavar='PATH', type=str, nargs='+', default=[],
            help='Paths to .csv files of completed results - one per column.')
    subparser.add_argument('-n', '--limit', metavar='LIMIT', type=int, default=None,
            help='Limit the number of records to process. Defaults to None (no limit).')
    subparser.add_argument('--offset', metavar='OFFSET', type=int, default=None,
            help='Offset into source table on which to start processing.')
    subparser.add_argument('--opensmile-home', metavar='PATH', type=str, default=OPENSMILE_DIR,
            help='Path to openSMILE installation. Defaults to {0}'.format(OPENSMILE_DIR))
    subparser.add_argument('-C', '--opensmile-conf', metavar='PATH', type=str, default=OPENSMILE_CONF,
            help='Config file for openSMILE. Defaults to {0}'.format(OPENSMILE_CONF))
    subparser.add_argument('--append', action='store_true', default=False)
    subparser.add_argument('--cleanup', dest='cleanup', action='store_true', default=True)
    subparser.add_argument('--no-cleanup', dest='cleanup', action='store_false')
    subparser.set_defaults(func=process_rows)

    args = parser.parse_args()

    ## caller can pass a path to openSMILE config that is
    ## relative to openSMILE's home directory
    if not os.path.isabs(args.opensmile_conf):
        args.opensmile_conf = os.path.join(args.opensmile_home, args.opensmile_conf)

    ## default output files to be named after the columns
    if len(args.out_files) > len(args.data_columns):
        raise Exception("More output files than data columns?")
    for i, column in enumerate(args.data_columns):
        if i >= len(args.out_files):
            args.out_files.append(column + ".features.csv")

    ## if we specify completed files, there should be one per data column
    if len(args.completed) != len(args.data_columns):
        raise Exception("There should be exactly one completed file for each data column.")

    verify_dependencies(args)

    syn = synapseclient.Synapse(skip_checks=True)
    syn.login(args.synapseUser, args.synapsePassword, silent=True)

    args.func(args, syn)


if __name__ == "__main__":
    main()

