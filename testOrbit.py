#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 17 14:07:11 2019

@author: Tamboli
"""
from __future__ import print_function
import numpy as np
import h5py
import os
import sys
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
def connect():
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

# =============================================================================
# TABLES['rValue'] = (
#     "CREATE TABLE `rValue` ("
#     "  `primary id` int(14) NOT NULL, AUTO_INCREMENT"
#     "  `particle id` int(10) NOT NULL AUTO_INCREMENT,"
#     "  `shot id` int(10) NOT NULL, AUTO_INCREMENT"
#     "  `valueR` decimal(14) NOT NULL,"
#     "  PRIMARY KEY (`primary id`)"
#     ") ENGINE=InnoDB")
# 
# TABLES['vValue'] = (
#     "CREATE TABLE `vValue` ("
#     "  `primary id` int(14) NOT NULL, AUTO_INCREMENT"
#     "  `particle id` int(10) NOT NULL AUTO_INCREMENT,"
#     "  `shot id` int(10) NOT NULL, AUTO_INCREMENT"
#     "  `valueV` decimal(14) NOT NULL,"
#     "  PRIMARY KEY (`primary id`)"
#     ") ENGINE=InnoDB")
# 
# TABLES['bounds'] = (
#     "CREATE TABLE `bounds` ("
#     "  `primary id` int(14) NOT NULL, AUTO_INCREMENT"
#     "  `particle id` int(10) NOT NULL AUTO_INCREMENT,"
#     "  `shot id` int(10) NOT NULL, AUTO_INCREMENT"
#     "  `inORout` int(1) NOT NULL,"
#     "  PRIMARY KEY (`primary id`)"
#     ") ENGINE=InnoDB")
# 
# TABLES['steps'] = (
#     "CREATE TABLE `steps` ("
#     "  `primary id` int(14) NOT NULL, AUTO_INCREMENT"
#     "  `particle id` int(10) NOT NULL AUTO_INCREMENT,"
#     "  `shot id` int(10) NOT NULL, AUTO_INCREMENT"
#     "  `timeStep` decimal(14) NOT NULL,"
#     "  PRIMARY KEY (`primary id`)"
#     ") ENGINE=InnoDB")
# =============================================================================

def create_database(cursor):
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
# =============================================================================
# for table_name in TABLES:
#     table_description = TABLES[table_name]
#     try:
#         print("Creating table {}: ".format(table_name), end='')
#         cursor.execute(table_description)
#     except mysql.connector.Error as err:
#         if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
#             print("already exists.")
#         else:
#             print(err.msg)
#     else:
#         print("OK")
# =============================================================================



def create_tableV(cursor):
    try:
        cursor.execute(
                "CREATE TABLE velocity (id INT AUTO_INCREMENT PRIMARY KEY, shot INT (5) , timeStep INT (5) , vX DECIMAL(55), vY DECIMAL(55), vZ DECIMAL(55) )")
              # "CREATE TABLES {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating table: ".format(err))
        exit(1)
try:
    cursor.execute("USE {}".format(DB_NAME))
except mysql.connector.Error as err:
    print("Table {} does not exists.".format(DB_NAME))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor)
        print("Database {} created successfully.".format(DB_NAME))
        cnx.database = DB_NAME
    else:
        print(err)
        exit(1)    


def create_tableR(cursor):
    try:
        cursor.execute(
                "CREATE TABLE radius (id INT AUTO_INCREMENT PRIMARY KEY, shot INT (5) , timeStep INT (5) , rX DECIMAL(55), rY DECIMAL(55), rZ DECIMAL(55) )")
              # "CREATE TABLES {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating table: ".format(err))
        exit(1)
try:
    cursor.execute("USE {}".format(DB_NAME))
except mysql.connector.Error as err:
    print("Table {} does not exists.".format(DB_NAME))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_tableR(cursor)
        print("Database {} created successfully.".format(DB_NAME))
        cnx.database = DB_NAME
    else:
        print(err)
        exit(1) 
        
def create_tableI(cursor):
    try:
        cursor.execute(
                "CREATE TABLE iter (id INT AUTO_INCREMENT PRIMARY KEY, shot INT (5) , timeStep INT (5) , iter INT (10) )")
              # "CREATE TABLES {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating table: ".format(err))
        exit(1)
try:
    cursor.execute("USE {}".format(DB_NAME))
except mysql.connector.Error as err:
    print("Table {} does not exists.".format(DB_NAME))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_tableR(cursor)
        print("Database {} created successfully.".format(DB_NAME))
        cnx.database = DB_NAME
    else:
        print(err)
        exit(1)    

def create_tableB(cursor):
    try:
        cursor.execute(
                "CREATE TABLE bounds (id INT AUTO_INCREMENT PRIMARY KEY, shot INT (5) , timeStep INT (5) , bounds INT (10) )")
              # "CREATE TABLES {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating table: ".format(err))
        exit(1)
try:
    cursor.execute("USE {}".format(DB_NAME))
except mysql.connector.Error as err:
    print("Table {} does not exists.".format(DB_NAME))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_tableR(cursor)
        print("Database {} created successfully.".format(DB_NAME))
        cnx.database = DB_NAME
    else:
        print(err)
        exit(1) 





#open file, read each line, add to new one with id#s
def read(input_datadir, input_filename, of):
    rf = h5py.File(os.path.join(input_datadir, input_filename), 'r')
    for key in rf:
        if input_filename +"/" + key not in of:
            odset = of.create_dataset(input_filename +"/" + key, rf[key].shape)
            odset[...] = rf[key]
    rf.close()
    
def Main():
    config = {
    'user': 'root',
    'password': 'BrynMawr',
    'host': '127.0.0.1',
    'database': 'sys',
    #'raise_on_warnings': True,
    }
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    connect()
    create_database(cursor)
    create_tableV(cursor)
    create_tableR(cursor)
    create_tableI(cursor)
    create_tableB(cursor)
    
Main()
# =============================================================================
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
# =============================================================================
    
    def Main():
    datadir = '/Users/Tamboli/Documents/data/sample_trajectory_files/'
    max_file_count = 100
    if len(sys.argv) > 1:
        max_file_count = int(sys.argv[1]) #number of files in each result file
    input_files = [file for file in os.listdir(datadir) if os.path.isfile(os.path.join(datadir, file)) and (file.endswith('.h5') or file.endswith('hdf5'))]
    #total_input_files_count = len(input_files)     #total number of input files

    datadir_processed = '/Users/Tamboli/Documents/data/processed/'
    datadir_output = '/Users/Tamboli/Documents/data/results/'
    if not os.path.exists(datadir_output):
        os.makedirs(datadir_output)
    if not os.path.exists(datadir_processed):
        os.makedirs(datadir_processed)

    current_bucket_counter = 0
    result_file = 'results_' + str(current_bucket_counter) + '.hdf5'
    current_bucket = h5py.File(os.path.join(datadir_output, result_file), "a")
    current_bucket_file_counter = 0

    for file_name in input_files:
        if current_bucket_file_counter >= max_file_count:
            current_bucket.close()
            current_bucket_counter += 1
            result_file = 'results_' + str(current_bucket_counter) + '.hdf5'
            current_bucket = h5py.File(os.path.join(datadir_output, result_file), "a")
            current_bucket_file_counter = 0
        #print(file_name)
        #print(datadir)
        read(datadir, file_name)
        #insert(cursor)
        #movefile(os.path.join(datadir, file_name), os.path.join(datadir_processed, file_name))
        current_bucket_file_counter+=1
        
        print (__name__)

if __name__ == "__main__":
   Main()

