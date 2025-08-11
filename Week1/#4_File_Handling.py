# File Handling
# %%
# directory listing
import os

entries = os.listdir('archive/')
print('ListDir')
for entry in entries:
    print(entry)

# scandir supports context management protocol, closes the iterator
with os.scandir('archive/') as entries:
    print('ScanDir')
    for entry in entries:
        print(entry.name)

# %%
from pathlib import Path
from datetime import datetime

entries = Path('.venv/')
# print('Pathlib')
# for entry in entries.iterdir():
#     if entry.is_file():
#         print(entry.name)

def convert_date(timestamp):
    # convert epoch to formatted date string
    x = datetime.fromtimestamp(timestamp)
    x = x.strftime('%d %b %Y')
    return x

# listing files
files = (entry for entry in entries.iterdir() if entry.is_file())
for file in files:
    print(f'file name: {file.name}')
    print(f'Last Modified Date: {convert_date(file.stat().st_mtime)}')


# listing directories
dirs = (entry for entry in entries.iterdir() if entry.is_dir() )
for dir in dirs:
    print(dir)


# %%
#  Creating a directory
p = Path('ex_dir/')
try:
    p.mkdir()
except FileExistsError as exec:
    print(exec)
p.mkdir(exist_ok=True)

p2 = Path('ex_dir/2025/08/08/')
p2.mkdir(parents=True)

# %%
# FileName pattern Matching
from glob import glob, iglob

x = glob('*.txt')
print(x)
for ele in x:
    print(ele)

y = iglob('**/*.xml', recursive=True)
print(y)
for ele in y:
    print(ele)


# %%
# Traversing files and Directories using Path excluding venv folder
path = Path('.')
for file in path.rglob('*.json'):
    if 'mypy' not in str(file) and 'venv' not in str(file):
        print(file)

# %%
# deleting files/directories
data_file = Path('ex_dir/')
try:
    # if data_file.is_dir():
    #     data_file.rmdir()
    # if data_file.is_file():
    #     data_file.unlink()
    # delete non empty directories
    for child in data_file.iterdir():
        if child.is_dir():
            child.rmdir()
except:
    print('Directory not empty or does not exist')


# %% 
# Copying, Moving, and Renaming Files and Folders
import shutil
src = 'ex_dir/2025/hello.txt'
dst = 'ex_dir/2025_copy/hello.txt'
src_dir = 'ex_dir/2025/'
dst_dir = 'ex_dir/2025_copy2/'
shutil.copy(src, dst)
shutil.copy2(src, dst) # preserves metadata
shutil.copytree(src_dir, dst_dir) # copies entire directory tree

# Path().rename('fn') -> for renaming files and directories

# %%
# exercise
from pathlib import Path
import pandas as pd

def read_all_csv_files(path, retries=2):
    dicts = {}
    index = 0
    for file in path.rglob('**/*.csv'):
        if 'venv' not in str(file):
            print(file)
            i = 0
            while i <= retries:
                try:
                   pd.read_csv(file.name)
                   dicts[index] = file.name
                   index += 1
                   print('Read Successfully')
                   print()
                   break
                except Exception as e:
                    print(f'Attempt number {i+1} failed due to {e}')
                    i += 1
    return dicts 

    
path = Path('.')
retries = 2
files = read_all_csv_files(path, retries)
print(files)
