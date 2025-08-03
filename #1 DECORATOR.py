# DAY 1 - DECORATORS THAT DELIVER
# CONCEPTS
# %%
# First Class Functions 
# Can treat functions as any other object
def square(x):
    return x**2

f = square
f2 = square(5)
f3 = f(5) # treating the variable f as a function

def map(func, arr): # function as an argument
    res = []
    for i in range(len(arr)):
        res.append(func(i))
    return res

lists = [1, 2, 3]
print(map(square, lists))


def logger(msg): # returning funct from a function
    def func():
        return msg
    return func

log_hi = logger('Hello')
print(log_hi())

def header_tag(tag):
    def text_fn(msg):
        print(f'<{tag}>{msg}</{tag}>')
    return text_fn

header = header_tag('h1')
header('Hello')

header = header_tag('h3')
header('GM!')

# %%
# learning args and kwargs

def func(arg1):
    print(arg1)

lists = [1,2,3]
print(lists)
print(type(lists))
print(*lists)
# print(type(*lists))
print(*(type(item) for item in lists))
func(lists)


def tries(*args):
    print('Tried', *args, 'times')
    print('Tried', *args, 'times', sep=', ')
    unpacked_args = ', '.join(str(arg) for arg in args)
    print(f'Tried {unpacked_args} times')

tries(2,3)

# def outer(*args):
#     tries(*args)

# outer(2)


# %%
# Closures 
# an inner function that remembers and has access to variables in the local scope in which it was created even after outer function has been executed

def outer():
    msg = 'Hello from outer function'
    def inner():
        print(msg)
    return inner

# print(outer) # <function outer at 0x000001F08731A200>
var = outer()
# print(var) # <function outer.<locals>.inner at 0x000001F087318CC0>

def logger(func):
    def wrapper(*args):
        print(f'Calling function {func.__name__} with postional arguments {args}')
        return func(*args)
    return wrapper

def add(a,b):
    return a+b

add_logger = logger(add)
# print(add_logger) # <function logger.<locals>.wrapper at 0x000001F0874AB4C0>
print(add_logger(1,2)) # 3

# %%
# Decorators
# a function that takes another function as an argument, adds some functionality and returns another function
# why -> allows us to add functionality to our existing function by adding our functionality inside of wrapper


def decorator_function(original_function):
    def wrapper():
        print('From Wrapper Function')
        original_function()
    return wrapper

class decorator_class(object):
    def __init__(self, original_function):
        self.original_function = original_function

    def __call__(self, *args, **kwargs):
        print('From Call method')
        self.original_function()


@decorator_function # display = decorator_function(display) # under the hood
def display():
    print('Display function ran!')

# @decorator_class
@decorator_function
def display():
    print('Display Function ran')



from functools import wraps

def timer(func):
    import time

    @wraps(func)
    def wrapper():
        st = time.time()
        func()
        print(f'timer decorator running, function name: {func.__name__}')
        time.sleep(1)
        et = time.time()
        print(f'duration is: {et-st}')

    return wrapper

def logger(func):
    @wraps(func)
    def wrapper():
        func()
        print(f'Logging, function name: {func.__name__}')

    return wrapper

@logger
@timer
def display():
    print('ran display function')


# display = logger(timer(display))
display()
# %%
# Parameterized

def para_deco(arg1, arg2):
    def deco(func):
        def wrapper(*args, **kwargs):
            print(f'Hiii {arg1} {arg2}')
            return func(*args, **kwargs)
        return wrapper
    return deco

@para_deco('Rohit', 'Mishra')
def say_hi(name, sirname):
    print(f'Hello {name} {sirname}')

say_hi('K', 'M') # param deco is called first then actual deco starts


###### END -  CONCEPTS

#%%
# Q1. @retry(max_retries=3, delay=2) a. retries the wrapped function on exception b. logs retry count c. returns the original function's result

import random
from functools import wraps

def retry(max_retries, delay):
    def decorator(func):
        @wraps(func)
        def wrapper(*args,**kwargs):
            for _ in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except:
                    import time
                    print(f'Retying number {_+1}')
                    time.sleep(delay)
        return wrapper
    return decorator


@retry(3, 2)
def db_upload(value):
    compared_value = round(random.random(),2)
    if value < compared_value:
        print(f'Uploaded Successfully as {value} is < {compared_value}!!!')
    else:
        print(f'Failed To Upload because {value} is >= {compared_value}!!')
        raise Exception

# db_upload = decorator(db_upload)
db_upload(round(random.random(),2))

# %%
# Reflection Questions
# Q1. What happens if the decorator doesn't return the wrapped function?
# Ans. Then the wrapper function won't get executed which has the main function invoking code so the main function will also not get executed.

# Q2. Why are *args and **kwargs necessary in decorators?
# Ans. Decorators are flexible meaning they can run on top of any function with differenct number of arguments. So, to get all different number and types of arguments, we use *args and **kwargs.

# Q3. What are the risks of hardcoding retries inside a decorator?
# Ans. It reduces the flexibility of decorator, while when passed as an parameter, it can work differently for different main functions.