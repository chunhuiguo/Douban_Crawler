'''The Main Module

Summary
-------
This module defines the main() function as the program start point.
'''

import sys
import time

import config
import util
import scheduler


def main():
    '''The program start point, containing the following procedures:
    -- startup configuration for program environment
    -- configure and start APSchedulers
    -- schedule daily routine jobs
    -- schedule data pre-process jobs
    -- schedule movie info crawl jobs
    -- schedule comment crawl jobs
     
    
    Parameters
    ----------
    None
    
    Returns
    -------
    None
    '''

    # Program startup configuration
    util.startup_config()

    # Configure scheduler
    try:
        config.bg_scheduler = scheduler.start_scheduler()
    except Exception as e:
        msg = f'Start a background scheduler failed. The program is TERMINATED! -- Original Exception -- {e}'
        current_frame = sys._getframe()
        logger_name = f'{__name__}.{current_frame.f_code.co_name} at line {current_frame.f_lineno}'
        util.log(msg, config.LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_CRITICAL)
        util.log(msg, config.ERROR_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_CRITICAL)
        util.log(msg, config.SCHEDULER_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_CRITICAL)
        util.log(msg, config.SCHEDULER_ERROR_LOG_FILE, logger_name=logger_name, log_level=config.LOG_LEVEL_CRITICAL)
        exit()
    
    # Jobs scheduled to run each day in the following order:
    # (1) daily routine jobs: daily maintainance jobs including update log file, update movie list, update comment crawl jobs schedule
    # (2) data pre-process jobs: pre-process comment data from the previous day
    # (3) movie info crawl jobs: crawl movie info
    # (4) comment crawl jobs: crawl comments

    # schedule daily routine jobs
    scheduler.schedule_daily_routine_jobs(config.bg_scheduler)

    # schedule data pre-process jobs
    scheduler.schedule_data_preprocess_jobs(config.bg_scheduler) 

    # schedule movie info crawl jobs 
    scheduler.schedule_movie_info_crawl_jobs(config.bg_scheduler)  

    # schedule comment crawl jobs 
    scheduler.schedule_comment_crawl_jobs(config.bg_scheduler)
       
    
    # Keep the main thread alive    
    while True:
        time.sleep(100)
        # ??? TO IMPLEMENT:
        # Accept user input 'movie_id' and re-crawl its comments (i.e. crawl_all set to be True)
    
    # ??? NEED THIS
    config.bg_scheduler.shutdown()

    # ??? TO IMPLEMENT
    # -- email notification: CRITICAL log & some ERROR log


    
 
    
if __name__ == '__main__':
    main()

