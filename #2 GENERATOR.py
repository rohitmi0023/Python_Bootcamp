# DAY 1 - GENERATOR PIPELINES: STREAMLINE OR DIE
# CONCEPTS
# %%
lists = [1,2,3,4]

def squared(lists):
    l2 = []
    for ele in lists:
        l2.append(ele*ele)
    return l2

lists2 = squared(lists)
print(lists2)

# %%
def gen_sq(lists):
    for ele in lists:
        yield ele**2

lists_gen = gen_sq(lists)
print(lists_gen)
print(next(lists_gen))
# generators don't hold the result in memory, instead it yields one result at a time

# %%

print([ele**2 for ele in lists])
print((ele**2 for ele in lists))
print(list((ele**2 for ele in lists)))

# %%
import sys

# List comprehension
list_comp = [x for x in range(10000)]
print(f"List size: {sys.getsizeof(list_comp)} bytes")

# Generator expression
gen_exp = (x for x in range(10000))
print(f"Generator size: {sys.getsizeof(gen_exp)} bytes")

# %%

def read_large_file(file_path):
    """Generator that reads a file line by line"""
    with open(file_path, 'r') as file:
        for line in file:
            yield line.strip()

def filter_long_lines(lines, min_length=50):
    """Generator that filters lines by length"""
    for line in lines:
        if len(line) >= min_length:
            yield line

# Chaining generators - no intermediate lists created
long_lines = filter_long_lines(read_large_file('large_file.txt'))
print(next(long_lines))
print(next(long_lines))

# %%
def read_large_file(file_path):
    print("Reading line 1")
    yield "line 1"
    print("Reading line 2") 
    yield "line 2"
    print("Reading line 3")
    yield "line 3"

def filter_long_lines(lines, min_length=2):
    print("Filter function called")
    for line in lines:  # This works because 'lines' is iterable
        print(f"Processing: {line}")
        if len(line) >= min_length:
            print(f"Yielding: {line}")
            yield line

def manual_read_large_file(file_path):
    print("Reading line 1")
    yield "line 1"
    print("Reading line 2") 
    yield "line 2"
    print("Reading line 3")
    yield "line 3"

def bts_filter_long_lines(lines, min_length=2):
    # python hidden implementation
    print("Manual Filter function called")
    iterator = iter(lines)
    while True:
        try:
            line = next(iterator)
            print(f"Processing: {line}")
            if len(line) >= min_length:
                print(f"Yielding: {line}")
                yield line
        except StopIteration:
            break

# Demo
file_gen = read_large_file("dummy.txt")
manual_file_gen = manual_read_large_file("dummy.txt")

filtered_gen = filter_long_lines(file_gen, min_length=6)

manual_filtered_gen = bts_filter_long_lines(manual_file_gen, min_length=6)

print("Starting iteration...")
for result in filtered_gen:
    print(f"Got result: {result}")

print("Starting manual iteration...")
for result in manual_filtered_gen:
    print(f"Got result: {result}")




# %%
# 3 stage pipeline 
# read_lines(file_path) → filter_errors(lines) → parse_log(lines)
import os

file_path = 'large_generator.log'

absolute_path = os.path.join(file_path)

def read_lines(absolute_path):
    try:
        with open(absolute_path, mode='r') as file:
            for line in file:
                yield line
    except FileNotFoundError:
        print(f'File Not Found: {absolute_path}')
        return

def filter_errors(lines):
    yield from (line for line in lines if line.find('ERROR')+1)

def parse_log(lines):
    for index, line in enumerate(lines):
        yield {
            'index': index,
            'timestamp': line.split('-')[0],
            'level': line.split('-')[1],
            'message': line.split('-')[2]
        }

res = parse_log(filter_errors(read_lines(absolute_path)))

for i in range(10):
    print(next(res))


# print(next(filter_errors(read_lines(absolute_path))))
# print(next(filter_errors(read_lines(absolute_path))))


# %% Reflection Questions:
# Q. How does generator chaining improve memory use?
# A. Generator has lazy evaluation meaning they don't execute the program untill invoked with next(), so they create any value objects even though it will execute functions, and it gives back values one by one on-demand.

# Q. What breaks if yield is replaced with return?
# A. When we replace yield with return, it will return just the first element of the iterable.

# Q. Where can you plug such a pipeline in real ETL jobs?
# A. In strea, processing of large files 

