# Section 2: OOP and Error Handling (30mins)

# Q4. Custom Exception
# Time Taken- 30mins
# %%
import pandas as pd

class DataValidationError(Exception):
    def __init__(self, message="Data validation error occurred"):
        super().__init__(message)
        

path = 'ex2.csv'
try:
    df = pd.read_csv(path)
    if df.isna == True:
        raise DataValidationError(df)
    
except DataValidationError as error:
    print("Something went wrong with csv file reading!")
