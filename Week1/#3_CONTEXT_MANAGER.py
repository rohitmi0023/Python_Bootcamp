# DAY 3 – CONTEXT MANAGERS THAT TIME AND PROTECT

# Concepts:
# 'with' statement allows you to take advantage of existing context managers to automatically handle the teardown, setup, etc whenever you are dealing with external resources

# Context Management Protocol allows you to create your own customized operations on context managers

# 'with' statement creates a runtime context that allows you to run a group of statements under the control of context manager
# with open('ex.txt') as target_var:
#     print('Hello')
# expression(open()) must return an object that implements the context management protocol. This protocol consists of two special methods: a. __enter()__ b. __exit()__
# %%
# with open('users.json') as f:
#     print(f.__enter__())
#     f.__exit__()
#     print('lets see')

# x = with open('users.json') as f

# print(x)
# %%
import pathlib

x = pathlib.Path('userss.json')
print(x)

try:
    with x.open() as f:
        print(f)
except OSError as err:
    print(f'Failed on {x} due to {err}')

# %%
class HelloContextManager:
    def __enter__(self):
        return 'Enter Method'
    def __exit__(self, exc_type, exc_value, exc_tb):
        print(f'Leaving the context {exc_type} {exc_value} {exc_tb}')


with HelloContextManager() as hello:
    print(hello)
    print('Ready to leave')

# %%
class HelloContextManager:
    def __enter__(self):
        return 'Enter Method'
    def __exit__(self, exc_type, exc_value, exc_tb):
        print(f'exc type {exc_type}')
        print(f'exc value {exc_value}')
        print(f'exc tb {exc_tb}')
        print(f'Leaving the context')


with HelloContextManager() as hello:
    print(hello)
    # print(f'some error {hello[30]}')
    print('Ready to leave')

# %%
class HelloContextManager:
    def __enter__(self):
        print('Entering the context')
        return 'Hello Context'
    
    def __exit__(self, exc_type, exc_value, exc_tb):
        print('Exiting context')
        # return True
        if isinstance(exc_value, IndexError):
            print(f'Exception occured of type: {str(exc_type).split(' ')[1].rstrip('>')}')
            print(f'Exception Message: {exc_value}')
            return True
        
with HelloContextManager() as hello:
    print(hello)
    print(hello[100])
    print(f'After exception raise')

print('next logics')

# %%

from pathlib import Path

file_name = 'README.md'

class readableFile():
    def __init__(self, file_name):
        self.file_name = file_name
    
    def __enter__(self):
        self.file_obj = Path(file_name).open('r')
        return self.file_obj
    
    def __enter__(self):
        self.file_obj = open(file_name, 'r')
        return self.file_obj


    def __exit__(self, exc_type, exc_value, exc_tb):
        if self.file_obj:
            self.file_obj.close()

with readableFile(file_name) as file:
    print(file.read())

# %%
import sys

class RedirectedStdout:
    def __init__(self, new_output):
        self.new_output = new_output

    def __enter__(self):
        self.saved_output = sys.stdout
        sys.stdout = self.new_output
        print(sys.stdout)
        print(self.saved_output)
    
    def __exit__(self, *args, **kwargs):
        sys.stdout = self.saved_output

with open('hello.txt', 'w') as f:
    with RedirectedStdout(f):
        print('Hello')
    print('Back to standard output..')

# %%
from contextlib import contextmanager

@contextmanager
def hello_context_manager():
    print('Entering context!')
    yield 'Hello World'
    print('Leaving Context!')

with hello_context_manager() as hello:
    print(hello)

#%%

from contextlib import contextmanager

@contextmanager
def writable_file(file_path):
    file = open(file_path, mode='w')
    try:
        yield file
    finally:
        file.close()

with writable_file('hello.txt') as f:
    f.write('Hello')

# %%
# Implement two versions of Timer: 
# a. class based: logs execution time of code block
# b. @contextmanager-based: Wraps any ETL step
import time
from contextlib import contextmanager

class Timer:
    def __init__(self):
        self.time = time
        self.start = 0
        self.end = 0

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.end = time.time()
        print(f'Class execution Total time taken: {self.end - self.start}')

with Timer():
    time.sleep(2)

@contextmanager    
def timer_func():
    start = time.time()
    yield 
    end = time.time()
    print(f'Contextmanager function Total time taken: {end-start}')

with timer_func():
    time.sleep(3)


# %%
# Reflecion Questions

# Q1. What's the advantage of contextlib.contextmanager over class?
# A. Easier to implement as we dont have to remember any of in-built methods

# Q2. How do context managers enhance reliability in file/db operations?
# A. during file/db operations, by using context manager we ensure that some code will get executed no matter what happens in the acutal such as closing out the file or db connections.

# Q3. Where would you use context managers in a pipeline?
# Handling files, performing set of code everytime before actual code and at the end of code. Handling some excpetions gracefully by returning True at the end method.

