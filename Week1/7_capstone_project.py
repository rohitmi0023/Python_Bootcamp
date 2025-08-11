# %%
"""
XML to DataFrame Batch Ingestion Pipeline

Continuously monitors 'incoming/' directory for XML files, converts them to DataFrames using pandas,
exports as CSV files, and archives processed XMLs to 'archive/' directory. Features exponential 
backoff retry mechanism for error recovery and comprehensive logging with both file and console output.
Processes files in 10-second batch cycles with graceful error handling and automatic directory creation.

"""


"""
Dependencies: pandas, pathlib, logging, shutil
Usage: python xml_pipeline.py
Output: CSV files in incoming/, archived XMLs in archive/, logs in xml_pipeline.log
"""


from pathlib import Path
import time
from contextlib import contextmanager
import pandas as pd
from random import random
import shutil
from functools import wraps
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('7_capstone_project.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

path = Path('incoming/')

xml_files_lists: list[str] = []

batch_wait_time = 10

# timer context manager that logs time duration for a batch
@contextmanager
def timer():
    start_time = time.time()
    logger.info(f'Starting Context Manager Execution {timer.__name__}')
    yield
    total_time = time.time() - start_time
    logger.info(f'Completed Context Manager {timer.__name__}, Total Time Taken: {total_time}')
    logger.info(f'Batch processing completed. Waiting for {batch_wait_time} seconds for next batch processing!!')
    time.sleep(batch_wait_time) # 10 seconds delay for next batch processing

# retry decorator with exponential backoff till 5 retries
def retry_deco(func):
    @wraps(func)
    def wrapper(*args, **kwargs):          
        logger.debug(f'Executing {func.__name__} with retry decorator')        
        retry_base = 2
        retry_exp = 0
        while True:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(f'Error occurred in {func.__name__}: {e}')
                retry_time = retry_base ** retry_exp
                logger.warning(f'Retrying {func.__name__} in {retry_time} seconds... (attempt {retry_exp + 1})')
                time.sleep(retry_time)
                retry_exp += 1
                if retry_exp > 5:
                    logger.critical(f'Max retries exceeded for {func.__name__}')
                    raise e
    return wrapper

# scans a folder for xml files and returns xml filenames in list
def folder_scanner(xml_files_lists):
    logger.debug(f'Executing function {folder_scanner.__name__}')
    for file in path.glob('**/*.xml'):
        if file.name not in xml_files_lists:
            logger.debug(f'New XML file detected: {file.name}')
            xml_files_lists.append(file.name)
    logger.info(f'Lists of XML files: {xml_files_lists}')
    return xml_files_lists

# converts XML to dataframe
@retry_deco
def xml_to_dataframe(xml_path: Path) -> pd.DataFrame:
    logger.debug(f'Executing function {xml_to_dataframe.__name__}')
    with xml_path.open('r') as file:
        x = random()
        if x > 0.4:
            try:
                df = pd.read_xml(file)
                logger.debug(f'Loaded Dataframe head of: {df.head()}')
                logger.info(f'Loaded DataFrame from {xml_path}')
                return df
            except Exception as e:
                logger.error(f'Error parsing XML file {xml_path} : {e}')
        else:
            error_msg = f'Simulated parsing failure for {xml_path.name}, value {x}'
            logger.warning(error_msg)
            raise ValueError(error_msg)

# converts dataframe to csv
def df_to_csv(df: pd.DataFrame, file_path: Path) -> None:
    logger.debug(f'Executing function {df_to_csv.__name__} for file {file_path.name}')    
    try:
        csv_file = Path('incoming') / f'{file_path.stem}.csv'
        logger.debug(f'writing df to csv file: {csv_file}')
        df.to_csv(csv_file, index=False)
        logger.info(f'Successfully wrote DataFrame to {csv_file} with {df.shape[0]} rows and {df.shape[1]} columns')
    except Exception as e:
        logger.error(f'Error writing DataFrame for {file_path.name}: {e}')

# moves processed XML files to archive
def move_processed_files(xml_files_lists: list[str], src: Path, dst: Path) -> None:
    logger.debug(f'Executing function {move_processed_files.__name__}')
    try:
        for xml_file in xml_files_lists:
            shutil.move(src, dst)
            logger.info(f'Moved {xml_file} to archive folder')
    except Exception as e:
        logger.error(f'Error moving files to archive: {e}')
    logger.info(f'Completed processing of XML files: {xml_files_lists}')


try:
    while True:
        with timer():
            # scan a folder for xml files and returns xml filenames
            xml_lists = folder_scanner(xml_files_lists)
            processed_files = []

            if not xml_lists:
                logger.info('No new XML files found, waiting for next batch...')
                time.sleep(batch_wait_time)
                continue
            else:
                logger.info(f'Processing batch of {len(xml_lists)} XML files')

            # iterating through each xml file
            for xml_file in xml_lists:
                try:
                    file_path = path / xml_file
                    logger.info(f'Processing file: {xml_file}')
                    # parse XML to dataframe
                    df = xml_to_dataframe(file_path)

                    # write to csv file
                    df_to_csv(df, file_path)
                
                    processed_files.append(xml_file)
                    logger.info(f'Successfully processed file: {xml_file}')

                except Exception as e:
                    logger.error(f'Error processing file {xml_file}: {e}')
                    continue

            # move processed files to archive
            if processed_files:
                try:
                    for xml_file in processed_files:
                        src_path = path / xml_file
                        dst_path = Path('archive')
                        dst_path.mkdir(exist_ok=True)
                        dst_path = Path('archive') / xml_file                        
                        move_processed_files(processed_files, src_path, dst_path)
                        logger.info(f'Archived processed file {xml_file} to archive')
                    logger.info(f'Successfully archived {len(processed_files)} processed files')
                except Exception as e:
                    logger.error(f'Error moving processed files: {e}')

            xml_files_lists.clear()
            processed_files.clear()


except KeyboardInterrupt:
    logger.info('Batch processing interrupted by user.'.upper())
except Exception as e:
    logger.critical(f'An unexpected error occured in the main loop: {e}')
    raise
finally:
    logger.info('Capstone processing completed.')
