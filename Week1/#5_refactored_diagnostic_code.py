# %%
# Q5 (retry)
# Write a decorator @retry_on_fail that:
# Retries a function up to 3 times, Waits 1 second between retries (use time.sleep), Catches all exceptions and logs failure reason
from random import random
import time


def retry_on_fail(retries):
    def deco(func):
        def wrapper(*args, **kwargs):
            for i in range(retries+1):
                print(f'Executing function {func.__name__}')
                x = func()
                if x == 'Success':
                    return x
                else:
                    time.sleep(3)
                    if i < 3:
                        print(f'Failed, retrying {i+1} times')
                    else:
                        return x
        return wrapper
    return deco

@retry_on_fail(3)
def random_function():
    x = random()
    print(x)
    try:
        if x < 0.5:
            raise Exception
        else:
            return 'Success'
    except:
        return 'Failed'

random_function()

# %%
# Q6 (generators)
from pathlib import Path

file_path = 'generator.log'
path = Path(file_path)

def read_lines(path):
    print('Starting to read')
    with path.open('r') as file:
        for line_num, line in enumerate(file, 1):
            print(f"read_lines: Reading line {line_num}: {line.strip()}")
            yield line.strip()  # Remove newline for cleaner processing

def filter_lines(lines):
    print('Starting to Filter')
    for line in lines:
        words = line.split()
        if 'ERROR' in words:
            print(f'Found Error in line: {line}')
            yield line
        else:
            print(f'No ERROR in line: {line}')

def parse_to_dict(filterd_lines):
    print('Starting to Parse')
    for line_num, line in enumerate(filterd_lines):
        words = line.split()
        parsed_dicts = {
            'line_number': line_num,
            'full_line': line,
            'word_count': len(words),
            'words': words,
            'contains_error': 'ERROR' in words
        }
        print(f'parsed dict: {parsed_dicts}')
        yield parsed_dicts

print('Creating Generator pipeline!!')
gen1 = read_lines(path)
gen2 = filter_lines(gen1)
gen3 = parse_to_dict(gen2)

print('Consuming the pipeline...')
try:
    for i in range(10):
        print(f'-----Iteration {i+1}----')
        res = next(gen3)
        print(f'----Final Result {i+1}: {res}------')
        print('-' * 50)
except StopIteration:
    print(f'Completed generator read')
except FileNotFoundError:
    print(f'File not exist: {path}')

# %%
# Q7 (context manager)
from contextlib import contextmanager
import time

@contextmanager
def timer():
    print('Context Manager started')
    start_time = time.time()
    yield
    total_time = time.time() - start_time
    print(f'Total Time Taken for the timer context manager: {round(total_time,2)}')

with timer():
    print('Main code block starting executing....')
    time.sleep(2.5)
    print('Main code block completed executing....')  


# %%
# Q3 (file reading)
from pathlib import Path
import pandas as pd

class BatchFileProcessor:
    def __init__(self, path):
        self.path = path

    def list_csv_files(self):
        print('-'*50)
        print(f'executing list_csv_files function!!')
        lists = [file.name for file in path.glob('**/*.csv') if 'venv' not in str(file)]
        print(f'returned lists: {lists}')
        print()
        return lists

    def read_csv_files(self, lists):
        print('-'*50)
        print(f'executing read_csv_files function!!')
        if len(lists) == 0:
            print('No csv files found!!')
        else:
            df_dict = {}
            for file in lists:
                try:
                    df = pd.read_csv(file)
                    i = 1
                    print(f'Successfully read csv file: {file}')
                    df_dict_temp = {
                        'File Name': file,
                        'Data Frame': df
                    }
                    df_dict[i] = df_dict_temp
                except Exception as e:
                    print(f'Failed to read csv file: {file} due to {e}')        
            return df_dict


path = Path('.')
obj = BatchFileProcessor(path)
lists = obj.list_csv_files()
obj.read_csv_files(lists)

