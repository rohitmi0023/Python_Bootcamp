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
# 3 stage pipeline 
# read_lines(file_path) → filter_errors(lines) → parse_log(lines)
import os

file_path = 'large_generator.log'

absolute_path = os.getcwd() + '\\' + file_path

def read_lines(absolute_path):
    with open(absolute_path, mode='r') as file:
        for line in file:
            yield line

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

