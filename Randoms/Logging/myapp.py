# %%
import logging
import mylib

logger = logging.getLogger(__name__)
logging.basicConfig(filename='Randoms/Logging/app.log', level=logging.DEBUG)
# logging.basicConfig(filename='Randoms/Logging/app.log', level=logging.DEBUG, format='%(name)s %(asctime)s - %(levelname)s - %(message)s')

def main_func():
    logger.info('Start')
    mylib.do()
    logger.info('End')

if __name__ == '__main__':
    main_func()
# %%
