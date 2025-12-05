'''The Utility Module

Summary
-------
This module defines some utility functions.
'''

import os
from datetime import datetime

import config
import daily_job_dispatcher
import data_preprocess_dispatcher


def pause(msg=''):
    '''Print the user message and pause the execution until the user press anykey

    Parameters
    ----------
    msg : str, optional
        The message to be printed (default is '')

    Returns
    -------
    None

    Notes
    -----
    For debugging and testing purpose only, the programmer can pause the program
    execution at any point.
    '''

    print(msg)
    input('Press ANYKEY to continue...')


def log_level_to_str(log_level):
    '''Get the name of the log_level
    
    Parameters
    ----------
    log_level : int
        The log level (constants defined in config.py)
    
    Returns
    -------
    Str
        The name of the given log_level
    '''

    log_level_name = {
        config.LOG_LEVEL_DEBUG: 'DEBUG',
        config.LOG_LEVEL_INFO: 'INFO' ,
        config.LOG_LEVEL_WARNING: 'WARNING' ,
        config.LOG_LEVEL_ERROR: 'ERROR' ,
        config.LOG_LEVEL_CRITICAL: 'CRITICAL'
    }

    return log_level_name.get(log_level, log_level)


def log(msg, log_file, logger_name='root', log_level=config.LOG_LEVEL_INFO):
    '''Log a message with level log_level under the log with name logger_name into the log_file.
    The log message format is 'timestamp; log_level; logger_name; msg'.
    
    Parameters
    ----------
    msg : str
        The message to be logged
    log_file : str
        The file to log the message
        The log_file could be any of the following:
        -- config.LOG_FILE: log ALL information
        -- config.ERROR_LOG_FILE: ONLY log ERROR/CRITICAL information
        -- config.SCHEDULER_LOG_FILE: log APScheduler information via APScheduler's logger
    logger_name : str, optional
        The name of the logger (default is 'root')
    log_level : int, optional
        The level of the log message (default is 'INFO')
        There are 5 log level:
        -- config.LOG_LEVEL_DEBUG: Detailed information, typically only of interest to a developer trying to diagnose a problem.
        -- config.LOG_LEVEL_INFO: Confirmation that things are working as expected.
        -- config.LOG_LEVEL_WARNING: An indication that something unexpected happened, or that a problem might occur in the near future (e.g. 'disk space low'). The software is still working as expected.
        -- config.LOG_LEVEL_ERROR: Due to a more serious problem, the software has not been able to perform some function.
        -- config.LOG_LEVEL_CRITICAL: A serious error, indicating that the program itself may be unable to continue running.
    
    Returns
    -------
    None
    '''

    timestamp = datetime.now(config.TIME_ZONE)
    log_level_str = log_level_to_str(log_level)
    text = f'{timestamp}; {log_level_str}; {logger_name}; {msg}\n\n'
    
    with open(log_file, mode='a', encoding='utf-8') as file:
        file.write(text)


def update_log_and_daily_file():
    '''Update the files to store log data and other daily data
    
    The log files include:
    -- the (regular) log file: log ALL information
    -- the error log file: ONLY log ALL ERROR/CRITICAL information
    -- the scheduler log & error files: log information related to the scheduler
    -- the data pre-processor log & error files: log information related to the data pre-processor
    -- the movie info crawler log & error files: log information related to the movie info crawler
    -- the comment crawler log & error files: log information related to the comment crawler
    -- the movie list manager log & error files: log information related to the movie list manager
    
    The daily files include:
    -- the daily CSV file to store comment crawl job cron schedule information
    -- the daily CSV file to store scheduled jobs information

    Parameters
    ----------
    None
    
    Returns
    -------
    None
    '''

    log_file_prefix = datetime.now(config.TIME_ZONE).strftime('%Y-%m-%d')  + '_'
    log_file_suffix = '.log'

    config.LOG_FILE = os.path.join(config.LOG_DIRECTORY, log_file_prefix + 'log' + log_file_suffix)
    config.ERROR_LOG_FILE = os.path.join(config.LOG_DIRECTORY, log_file_prefix + 'error' + log_file_suffix)
    config.SCHEDULER_LOG_FILE = os.path.join(config.LOG_DIRECTORY, log_file_prefix + 'log_scheduler' + log_file_suffix)
    config.SCHEDULER_ERROR_LOG_FILE = os.path.join(config.LOG_DIRECTORY, log_file_prefix + 'error_scheduler' + log_file_suffix)
    config.DATA_PREPROCESSOR_LOG_FILE = os.path.join(config.LOG_DIRECTORY, log_file_prefix + 'log_data_preprocessor' + log_file_suffix)
    config.DATA_PREPROCESSOR_ERROR_LOG_FILE = os.path.join(config.LOG_DIRECTORY, log_file_prefix + 'error_data_preprocessor' + log_file_suffix)
    config.MOVIE_INFO_CRAWLER_LOG_FILE = os.path.join(config.LOG_DIRECTORY, log_file_prefix + 'log_movie_info_crawler' + log_file_suffix)
    config.MOVIE_INFO_CRAWLER_ERROR_LOG_FILE = os.path.join(config.LOG_DIRECTORY, log_file_prefix + 'error_movie_info_crawler' + log_file_suffix)
    config.COMMENT_CRAWLER_LOG_FILE = os.path.join(config.LOG_DIRECTORY, log_file_prefix + 'log_comment_crawler' + log_file_suffix)
    config.COMMENT_CRAWLER_ERROR_LOG_FILE = os.path.join(config.LOG_DIRECTORY, log_file_prefix + 'error_comment_crawler' + log_file_suffix)
    config.MOVIE_LIST_MANAGER_LOG_FILE = os.path.join(config.LOG_DIRECTORY, log_file_prefix + 'log_movie_list_manager' + log_file_suffix)
    config.MOVIE_LIST_MANAGER_ERROR_LOG_FILE = os.path.join(config.LOG_DIRECTORY, log_file_prefix + 'error_movie_list_manager' + log_file_suffix)

    config.COMMENT_CRAWL_JOBS_CRON_SCHEDULE_FILE = os.path.join(config.SCHEDULING_DIRECTORY, log_file_prefix + 'comment_crawl_job_cron_schedule.csv')
    config.SCHEDULED_JOBS_FILE = os.path.join(config.SCHEDULING_DIRECTORY, log_file_prefix + 'scheduled_jobs.csv')

    msg = 'The {log_type}log file is created successfully!'
    log(msg.format(log_type=''), config.LOG_FILE)
    log(msg.format(log_type='error '), config.ERROR_LOG_FILE)
    log(msg.format(log_type='scheduler '), config.SCHEDULER_LOG_FILE)
    log(msg.format(log_type='scheduler error '), config.SCHEDULER_ERROR_LOG_FILE)
    log(msg.format(log_type='data pre-processor '), config.DATA_PREPROCESSOR_LOG_FILE)
    log(msg.format(log_type='data pre-processor error '), config.DATA_PREPROCESSOR_ERROR_LOG_FILE)
    log(msg.format(log_type='movie info crawler '), config.MOVIE_INFO_CRAWLER_LOG_FILE)
    log(msg.format(log_type='movie info crawler error '), config.MOVIE_INFO_CRAWLER_ERROR_LOG_FILE)
    log(msg.format(log_type='comment crawler '), config.COMMENT_CRAWLER_LOG_FILE)
    log(msg.format(log_type='comment crawler error '), config.COMMENT_CRAWLER_ERROR_LOG_FILE)
    log(msg.format(log_type='movie list manager '), config.MOVIE_LIST_MANAGER_LOG_FILE)
    log(msg.format(log_type='movie list manager error '), config.MOVIE_LIST_MANAGER_ERROR_LOG_FILE)


def startup_config():
    '''Congifure the program environment when the program starts

    Parameters
    ----------
    None
    
    Returns
    -------
    None
    '''

    # ??? TO IMPLEMENT:
    # use python code to install required Python modules (if not installed yet)

    # Create the data directory (if not exist)
    os.makedirs(config.DATA_DIRECTORY, exist_ok=True)
    # Create the directory to store crawled comment data (if not exist)
    os.makedirs(config.COMMENT_CRAWLED_DIRECTORY, exist_ok=True)
    os.makedirs(config.COMMENT_DAILY_DIRECTORY, exist_ok=True)
    os.makedirs(config.COMMENT_MERGED_DIRECTORY, exist_ok=True)
    # Create the directory to store crawled movie info data (if not exist)
    os.makedirs(config.MOVIE_INFO_DIRECTORY, exist_ok=True)
    # Create the directory to store log files (if not exist)
    os.makedirs(config.LOG_DIRECTORY, exist_ok=True)
    # Create the directory to store movie list files (if not exist)
    os.makedirs(config.MOVIE_LIST_DIRECTORY, exist_ok=True)
    # Create the directory to store job scheduling information files (if not exist)
    os.makedirs(config.SCHEDULING_DIRECTORY, exist_ok=True)

    # Run daily routine jobs once
    daily_job_dispatcher.dispatch_daily_routine_jobs(None, True)

    # Run data pre-process jobs once to pre-process all existing comment data
    data_preprocess_dispatcher.dispatch_data_preprocess_jobs(True)


def my_exit():
    #??? TO IMPLEMENT
    # shutdown all schedulers
    pass