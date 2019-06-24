#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 17 14:07:11 2019

@author: Tamboli
"""
from __future__ import print_function
#import numpy as np
#import h5py
#import os
#import sys
import mysql.connector
from mysql.connector import errorcode
config = {
    'user': 'root',
    'password': 'BrynMawr',
    'host': '127.0.0.1',
    'database': 'sys',
    #'raise_on_warnings': True,
}
cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()
#connect to database
def _connect():
    config = {
        'user': 'root',
        'password': 'BrynMawr',
        'host': '127.0.0.1',
        'database': 'sys',
        #'raise_on_warnings': True,
    }
    cnx = None
    try:
        cnx = mysql.connector.connect(**config)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
        if cnx:
            cnx.close()
    return cnx 

DB_NAME = 'orbitdata'
TABLES = {}
TABLES['rValue'] = (
    "CREATE TABLE `rValue` ("
    "  `primary id` int(14) NOT NULL, AUTO_INCREMENT"
    "  `particle id` int(10) NOT NULL AUTO_INCREMENT,"
    "  `shot id` int(10) NOT NULL, AUTO_INCREMENT"
    "  `valueR` decimal(14) NOT NULL,"
    "  PRIMARY KEY (`primary id`)"
    ") ENGINE=InnoDB")

TABLES['vValue'] = (
    "CREATE TABLE `vValue` ("
    "  `primary id` int(14) NOT NULL, AUTO_INCREMENT"
    "  `particle id` int(10) NOT NULL AUTO_INCREMENT,"
    "  `shot id` int(10) NOT NULL, AUTO_INCREMENT"
    "  `valueV` decimal(14) NOT NULL,"
    "  PRIMARY KEY (`primary id`)"
    ") ENGINE=InnoDB")

TABLES['bounds'] = (
    "CREATE TABLE `bounds` ("
    "  `primary id` int(14) NOT NULL, AUTO_INCREMENT"
    "  `particle id` int(10) NOT NULL AUTO_INCREMENT,"
    "  `shot id` int(10) NOT NULL, AUTO_INCREMENT"
    "  `inORout` int(1) NOT NULL,"
    "  PRIMARY KEY (`primary id`)"
    ") ENGINE=InnoDB")

TABLES['steps'] = (
    "CREATE TABLE `steps` ("
    "  `primary id` int(14) NOT NULL, AUTO_INCREMENT"
    "  `particle id` int(10) NOT NULL AUTO_INCREMENT,"
    "  `shot id` int(10) NOT NULL, AUTO_INCREMENT"
    "  `timeStep` decimal(14) NOT NULL,"
    "  PRIMARY KEY (`primary id`)"
    ") ENGINE=InnoDB")

def create_database(cursor):
    print('in create db')
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)
try:
    cursor.execute("USE {}".format(DB_NAME))
except mysql.connector.Error as err:
    print("Database {} does not exists.".format(DB_NAME))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor)
        print("Database {} created successfully.".format(DB_NAME))
        cnx.database = DB_NAME
    else:
        print(err)
        exit(1)    
for table_name in TABLES:
    table_description = TABLES[table_name]
    try:
        print("Creating table {}: ".format(table_name), end='')
        cursor.execute(table_description)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

cursor.close()
cnx.close()

cursor.close()
cnx.close()


_connect()
create_database(cursor)
 




# =============================================================================
# #open file, read each line, add to new one with id#s
# def read(input_datadir, input_filename, of):
#     rf = h5py.File(os.path.join(input_datadir, input_filename), 'r')
#     for key in rf:
#         if input_filename +"/" + key not in of:
#             odset = of.create_dataset(input_filename +"/" + key, rf[key].shape)
#             odset[...] = rf[key]
#     rf.close()
#     
# def movefile(filename, desitnation_filename):
#     os.rename(filename, desitnation_filename)
#     
# def Main():
#     datadir = 'H:/BMCProjects/data/sample_trajectory_files/'
#     max_file_count = 100
#     if len(sys.argv) > 1:
#         max_file_count = int(sys.argv[1]) #number of files in each result file
#     input_files = [file for file in os.listdir(datadir) if os.path.isfile(os.path.join(datadir, file)) and (file.endswith('.h5') or file.endswith('hdf5'))]
#     #total_input_files_count = len(input_files)     #total number of input files
# 
#     datadir_processed = 'H:/BMCProjects/data/processed/'
#     datadir_output = 'H:/BMCProjects/data/results/'
#     if not os.path.exists(datadir_output):
#         os.makedirs(datadir_output)
#     if not os.path.exists(datadir_processed):
#         os.makedirs(datadir_processed)
# 
#     current_bucket_counter = 0
#     result_file = 'results_' + str(current_bucket_counter) + '.hdf5'
#     current_bucket = h5py.File(os.path.join(datadir_output, result_file), "a")
#     current_bucket_file_counter = 0
# 
#     for file_name in input_files:
#         if current_bucket_file_counter >= max_file_count:
#             current_bucket.close()
#             current_bucket_counter += 1
#             result_file = 'results_' + str(current_bucket_counter) + '.hdf5'
#             current_bucket = h5py.File(os.path.join(datadir_output, result_file), "a")
#             current_bucket_file_counter = 0
#         append(datadir, file_name, current_bucket)
#         movefile(os.path.join(datadir, file_name), os.path.join(datadir_processed, file_name))
#         current_bucket_file_counter+=1
#     
# =============================================================================
    

