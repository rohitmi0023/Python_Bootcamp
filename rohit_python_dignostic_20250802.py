##########################################################################
# SECTION 1 CORE PYTHON

######################################
# %%
# Q1 - List vs Tuple vs Set vs Dict
# Write a function compare_collections() that:
# a. Takes a list of 10 items as input. b. Converts it into tuple, set, and dictionary. c. Returns size in memory (sys.getsizeof) and type of each collection.
# Print observations on mutability, ordering, and hashing.
# Time Taken- 1hr
import sys

def compare_collections(lists):
    # print(args) # why does it prints (['1', '2'],), I expected [('1', '2')]
    # lists = list(*args)

    tuples = tuple(lists)
    sets = set(lists)
    dicts = {}

    for x in range(len(lists)):
        dicts[x] = lists[x]

    print(f'Size in memory {sys.getsizeof(lists)}, Type of object: {type(lists)}, Items: {lists}') # 184 bytes
    print(f'Size in memory {sys.getsizeof(tuples)}, Type of object: {type(tuples)}, Items: {tuples}') # 120 bytes
    print(f'Size in memory {sys.getsizeof(sets)}, Type of object: {type(sets)}, Items: {sets}') # 728 bytes
    print(f'Size in memory {sys.getsizeof(dicts)}, Type of object: {type(dicts)}, Items: {dicts}') # 352 bytes

    # Mutability, ordering and hashing

    lists[0] = 23 # mutable
    sets.add(23) # mutable, cannot use list operator though
    # tuples[0] = 23 # immutable error -> TypeError: 'tuple' object does not support item assignment
    dicts[0] = 23 # mutable1


    print(lists) # ordered
    print(sets) # unorderd -> random, unique elements
    print(tuples) # ordered
    print(dicts) # ordered -> always unique keys


    # print(hash(lists)) # unhashable returns TypeError: unhashable type: 'list'
    # print(hash(sets)) # unhashable returns TypeError: unhashable type: 'set'
    print(hash(tuples)) # hashable returns hash number -278315504148916073
    # print(hash(dicts)) # unhashable returns TypeError: unhashable type: 'dict'

    # Question: why is set unhashable? They do contain only unique elements which is required for hashing, right? or is it something related to mutability rather than uniqueness?

lists = []
for i in range(10):
    el  = input(f"Enter List Element number {i+1}")
    lists.append(el)

compare_collections(lists)

########################################
# %%
# Q2: Function Behavior
# What will be the output? Why?
# Rewrite this function to avoid the issue.
# Time Taken- 7mins

def append_val(val, lst = []):
    lst.append(val)
    return lst

print(append_val(10)) # [10]
print(append_val(20)) # [20] 10 will not be appended because lst in in function scope so everytime it gets created when function is invoked and loses it memory when function completes

# Modified
lst = []
def append_val(val):
    global lst
    lst.append(val)
    return lst

print(append_val(10)) # prints [10]
print(append_val(20)) # prints [10, 20]

######################################################################################
# SECTION 2: OOP AND ERROR HANDLING (30MINS)

#######################################
# %%
# Q3: Build a Data Processor Class
# Create a class BatchFileProcessor that:
# Accepts a folder path, Lists all .csv files in that folder,
# Reads each file into a dictionary of DataFrames,
# Logs errors using the logging module (file not found, parse errors, etc.)
# Bonus: Add a retry mechanism (max 3 attempts) for any failed file read.
# Time Taken- 40mins
import os
import logging
import csv

class BatchFileProcessor:
    # csv_files: str = []
    def __init__(self, path):
        self.path = path
    def listing_files(self):
        files = os.listdir(self.path)
        csv_files = []
        dict_files = {}
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(file)
        print(csv_files)
        try:
            for file in csv_files:
                with open(file) as f:
                    csv_reader = csv.DictReader(f)
                    # was not able to dissect the meaning of- to read each file into a dictionary of dataframe
                    for index, line in csv_reader:
                        dict_files[index] = line
                    print(dict_files)
        except:
            logging.error('Parsing Failed')
        # source_dir = pathlib.Path(self.path)
        # files = source_dir.iterdir
        # for file in files:
        #     print(file)

path = os.getcwd()
obj1 = BatchFileProcessor(path)
obj1.listing_files()

#####################################
# %%
# Q4. Custom Exception
# Create a custom exception class DataValidationError. Use it in a function that: a. Validates that a DataFrame column has no nulls. b. Raises the error with a custom message if nulls are found.
# Time Taken- 30mins
import pandas as pd

class DataValidationError(Exception):
    def __init__(self, message="Nulls Data validation failed!"):
        super().__init__(message)
        self.message = message
        

path = 'ex2.csv'
column_name = 'name'
try:
    df = pd.read_csv(path)
    if df[column_name].isnull().sum():
        raise DataValidationError()
    else:
        print('Nulls Data validation passed!')
    
except DataValidationError as error:
    print(error.message)

#############################################################################
# %%
# SECTION 3: DECORATORS, GENERATORS, CONTEXT MANAGERS

# Q5. Retry Decorator
# Write a decorator @retry_on_fail that:
# Retries a function up to 3 times, Waits 1 second between retries (use time.sleep), Catches all exceptions and logs failure reason
# Time Taken- 20mins
import time
def retry_on_fail(func):
    def wrapper(*args, **kwargs):
        for i in range(3):     
            try:
                func(*args, **kwargs)
                break
            except Exception as err:
                time.sleep(1)
                if i == 2:
                    print(f'Error message: {err}')
                    print('Failed 3 times') 
                else:
                    print(f'Error message: {err}')
                    print('Retrying')
    return wrapper

@retry_on_fail
def divide(a, b):
    print(a/b)

divide(1,0)

#############################################################################
# %%
# Q6. Generator Pipeline
# Implement a 3 stage generator pipeline:
# 1. read_lines(file_path) - yields each line from a file
# 2. filter_keywords(lines, keyword) - yields only lines with a keyword
# 3. parse_to_dict(lines) - yields dicts assuming each line is comma-seperated: name, age, city
# Time Taken- 10mins

def read_lines(file_path):
    with open(file_path) as f:
        for line in f:
            yield f.readline()

def filter_keywords(lines, keyword):
    if keyword in lines:
        yield lines

dicts = {}

def parse_to_dict(lines):
    for index, word in enumerate(lines):
        dict[index] = word 

#############################################################################
# %%
# Q7. Context Manager
# create a context manager Timer() that:
# a. on enter, notes the current time, b. on exit, prints total elapsed time in seconds.
# Use it around a time-consuming loop to show output
# Time Taken- 8mins
import time

def Timer():
    curr_time = time.time()
    time.sleep(2)
    print(f'Time Spent in this function: {round(abs(time.time()-curr_time))}')

Timer()

#############################################################################
# SECTION 4: DATA MANIPULATION 

###########################
# %%
# Q8: Pandas + Transformation
# from a user activity csv file, write a script that:
# a. Reads the file into a dataframe b. converts 'timestamp' into datetime c. Filters only login events d. adds a column hour extracted from timestamp
# Time Taken- 10mins

import pandas as pd

path = 'user_activity.csv'
df = pd.read_csv(path)
df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df[df['event'] == 'login']
df['hour'] = df['timestamp'].dt.hour
print(df)

############################
# %%
# Q9. Numpy Performance Check
# create a list of 1 miilion numbers. Convert it into a numpy array. Calculate the square of each number using a. Python Loop b. List Comprehension c. Numpy Vectorization. Compare time takens for each.
# Time Taken 30mins

import numpy as np
import time

lists = []
for i in range(1_000_000):
    lists.append(i)

arr1 = np.array(lists)
arr2 = np.array(lists)
arr3 = np.array(lists)

start_time = time.time()
for ele in range(len(arr1)):
    arr1[ele] = arr1[ele]*arr1[ele]
end_time = time.time()
loop_total = end_time - start_time
print(arr1)
print(f'loop total time: {loop_total}') # 1.554 sec

start_time = time.time()
arr2 = np.array([ele*ele for ele in arr2])
end_time = time.time()
print(arr2)
comprehension_total = end_time - start_time
print(f'List Comprehension total time: {comprehension_total}') # 0.524 sec

start_time = time.time()
arr3 = arr3*arr3
end_time = time.time()
print(arr3)
vec_total = end_time - start_time
print(f'Vectorization total time: {vec_total}') # 0.025 sec



###########################################################################
# SECTION 5: REAL WORLD ETL LOGIC

##############################
#%%
# Q10 Parse JSON and load Mock
# write a function parse_and_load(file_path) that: a. reads a JSON file with nested user data b. Flattens it into a dataframe c. Mocks a DB insert using a function db_insert(data: List[dict])
import json
file_path='users.json'
def parse_and_load(file_path):
    with open(file_path, mode='r') as f:
        data = json.load(f)
        for line in data:
            print(line['name'])

parse_and_load(file_path)

###################################
#%% 
# Q11. Folder Watcher Simulation
# Simulate a mini-folder watcher that: a. scans a folderevery 10 seconds b. identifies new '.xml' files c. Parses them using ElementTree d. moves successfully processed files to an archive folder 

import time 
import os
import shutil

start_time = time.time()
xml_files = set()
def folder_scanner(folder_path):
    print('Looking for XML Files...')
    files = os.listdir(folder_path)
    for file in files:
        if file.endswith('.xml') and file not in xml_files:
            xml_files.add(file)
            print(f'New Xml File detected- {file}')
            print('To Do Parsing Logic')
            if not os.path.exists('./archive'):
                os.mkdir('./archive')
            print(f'Source Path- {os.getcwd()}\\xml_files\\{file}')
            shutil.move(f'{os.getcwd()}\\xml_files\\{file}', f'{os.getcwd()}\\archive')
            print(f'Moved {file} to archive folder')

path = './xml_files/'
# retring 3 times after every 10 seconds
for i in range(3):
    folder_scanner(path)
    time.sleep(10)

